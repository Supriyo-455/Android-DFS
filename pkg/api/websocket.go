package api

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/android-dfs/storage-node/pkg/gossip"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Client represents an active WebSocket connection from a management dashboard.
type Client struct {
	conn net.Conn
	send chan []byte
}

// Hub coordinates real-time event dissemination to connected dashboard clients.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

// NewHub initializes an event dispatch hub.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the WebSocket event dispatcher loop.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("[CLUSTER-HUB] UI client connected. Total observers: %d", len(h.clients))

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				_ = client.conn.Close()
			}
			h.mu.Unlock()
			log.Printf("[CLUSTER-HUB] UI client disconnected. Remaining: %d", len(h.clients))

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// BroadcastEvent encodes and delivers a cluster event to all connected dashboards.
func (h *Hub) BroadcastEvent(event gossip.ClusterEvent) {
	data, err := json.Marshal(event)
	if err == nil {
		h.broadcast <- data
	}
}

// HandleWebSocket upgrades standard HTTP GET requests into an RFC 6455 WebSocket session.
func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "websocket" {
		http.Error(w, "Expected websocket upgrade", http.StatusBadRequest)
		return
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	// Compute accept hash: Base64(SHA-1(Key + GUID))
	hasher := sha1.New()
	hasher.Write([]byte(key + wsGUID))
	acceptKey := base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	// Hijack TCP connection
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Hijacking unsupported", http.StatusInternalServerError)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Transmit HTTP 101 Switching Protocols response
	resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Accept: %s\r\n\r\n", acceptKey)

	if _, err := bufrw.WriteString(resp); err != nil {
		_ = conn.Close()
		return
	}
	_ = bufrw.Flush()

	client := &Client{
		conn: conn,
		send: make(chan []byte, 128),
	}

	h.register <- client

	// Start reader & writer routines
	go client.writePump(h)
	go client.readPump(h, bufrw.Reader)
}

func (c *Client) readPump(h *Hub, r *bufio.Reader) {
	defer func() {
		h.unregister <- c
	}()

	for {
		// Read WebSocket frame header
		b1, err := r.ReadByte()
		if err != nil {
			return
		}
		opcode := b1 & 0x0F
		if opcode == 0x8 {
			// Close frame received
			return
		}

		b2, err := r.ReadByte()
		if err != nil {
			return
		}

		isMasked := (b2 & 0x80) != 0
		length := int64(b2 & 0x7F)

		if length == 126 {
			var extLen uint16
			if err := binary.Read(r, binary.BigEndian, &extLen); err != nil {
				return
			}
			length = int64(extLen)
		} else if length == 127 {
			var extLen uint64
			if err := binary.Read(r, binary.BigEndian, &extLen); err != nil {
				return
			}
			length = int64(extLen)
		}

		var mask [4]byte
		if isMasked {
			if _, err := io.ReadFull(r, mask[:]); err != nil {
				return
			}
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		if isMasked {
			for i := 0; i < len(payload); i++ {
				payload[i] ^= mask[i%4]
			}
		}

		// Client messages (e.g. ping/commands)
		if opcode == 0x9 {
			// Ping -> Pong
			c.send <- []byte(`{"type":"pong"}`)
		}
	}
}

func (c *Client) writePump(h *Hub) {
	ticker := time.NewTicker(25 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = writeFrame(c.conn, 0x8, []byte{}) // Close frame
				return
			}
			if err := writeFrame(c.conn, 0x1, msg); err != nil { // Text frame
				return
			}
		case <-ticker.C:
			// Ping heartbeat
			if err := writeFrame(c.conn, 0x9, []byte{}); err != nil {
				return
			}
		}
	}
}

func writeFrame(w io.Writer, opcode byte, payload []byte) error {
	var header []byte
	finAndOp := byte(0x80) | (opcode & 0x0F) // FIN = 1
	n := len(payload)

	if n < 126 {
		header = []byte{finAndOp, byte(n)}
	} else if n <= 65535 {
		header = make([]byte, 4)
		header[0] = finAndOp
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(n))
	} else {
		header = make([]byte, 10)
		header[0] = finAndOp
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(n))
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// HandleSSE provides an alternative Server-Sent Events stream for HTTP clients.
func (h *Hub) HandleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	client := &Client{
		send: make(chan []byte, 64),
	}
	h.register <- client

	defer func() {
		h.unregister <- client
	}()

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg, ok := <-client.send:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
