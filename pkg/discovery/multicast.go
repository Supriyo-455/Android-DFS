package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// PeerAnnouncement is broadcast over UDP multicast for zero-configuration LAN discovery.
type PeerAnnouncement struct {
	NodeID      string    `json:"node_id"`
	NodeName    string    `json:"node_name"`
	Role        string    `json:"role,omitempty"` // "master" for desktop controller, "slave" for phone storage nodes
	HTTPAddr    string    `json:"http_addr"`
	GossipPort  int       `json:"gossip_port"`
	MerkleRoot  string    `json:"merkle_root"`
	TotalChunks int64     `json:"total_chunks"`
	StoredBytes int64     `json:"stored_bytes"`
	IsAndroid   bool      `json:"is_android"`
	Timestamp   time.Time `json:"timestamp"`
	Leaving     bool      `json:"leaving,omitempty"`
}

// MulticastDiscovery coordinates LAN subnet peer broadcasts and listener loops.
type MulticastDiscovery struct {
	groupAddr    string
	localNode    PeerAnnouncement
	listenerConn *net.UDPConn
	senderConn   *net.UDPConn
	broadcastDst *net.UDPAddr
	onPeerFound  func(PeerAnnouncement)
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.RWMutex
}

// NewMulticastDiscovery initializes the multicast discovery engine.
func NewMulticastDiscovery(
	groupAddr string,
	localNode PeerAnnouncement,
	onPeerFound func(PeerAnnouncement),
) (*MulticastDiscovery, error) {
	ctx, cancel := context.WithCancel(context.Background())

	md := &MulticastDiscovery{
		groupAddr:   groupAddr,
		localNode:   localNode,
		onPeerFound: onPeerFound,
		ctx:         ctx,
		cancel:      cancel,
	}

	return md, nil
}

// Start joins the multicast group and begins listening and periodic broadcasting.
func (m *MulticastDiscovery) Start() error {
	addr, err := net.ResolveUDPAddr("udp", m.groupAddr)
	if err != nil {
		return fmt.Errorf("invalid multicast address %s: %w", m.groupAddr, err)
	}
	m.broadcastDst = addr

	// Configure ListenConfig with SO_REUSEADDR and SO_REUSEPORT
	lc := net.ListenConfig{
		Control: SocketControl,
	}

	packetConn, err := lc.ListenPacket(m.ctx, "udp4", m.groupAddr)
	if err != nil {
		// Fallback to standard net.ListenMulticastUDP if ListenPacket with control is restricted
		log.Printf("[DISCOVERY] ListenPacket fallback: %v. Retrying standard Multicast listener...", err)
		mcConn, mcErr := net.ListenMulticastUDP("udp4", nil, addr)
		if mcErr != nil {
			return fmt.Errorf("failed to join multicast UDP group %s: %w", m.groupAddr, mcErr)
		}
		m.listenerConn = mcConn
	} else {
		if udpConn, ok := packetConn.(*net.UDPConn); ok {
			m.listenerConn = udpConn
		} else {
			return fmt.Errorf("failed to cast packet connection to UDPConn")
		}
	}

	// Create sender UDP socket
	sConn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to establish multicast dialer socket: %w", err)
	}
	m.senderConn = sConn

	log.Printf("[DISCOVERY] Joined UDP multicast group %s. SO_REUSEADDR/SO_REUSEPORT active.", m.groupAddr)

	m.wg.Add(2)
	go m.listenLoop()
	go m.broadcastLoop()

	return nil
}

// UpdateAnnouncement updates the state payload (e.g. new Merkle root or chunk count).
func (m *MulticastDiscovery) UpdateAnnouncement(updater func(*PeerAnnouncement)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	updater(&m.localNode)
}

// BroadcastNow sends an immediate multicast datagram to the subnet.
func (m *MulticastDiscovery) BroadcastNow() error {
	m.mu.RLock()
	payload := m.localNode
	payload.Timestamp = time.Now()
	m.mu.RUnlock()

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = m.senderConn.Write(data)
	return err
}

func (m *MulticastDiscovery) broadcastLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Initial immediate broadcast
	_ = m.BroadcastNow()

	for {
		select {
		case <-m.ctx.Done():
			// Send leaving announcement
			m.mu.Lock()
			leaving := m.localNode
			leaving.Leaving = true
			leaving.Timestamp = time.Now()
			m.mu.Unlock()
			if data, err := json.Marshal(leaving); err == nil && m.senderConn != nil {
				_, _ = m.senderConn.Write(data)
			}
			return
		case <-ticker.C:
			_ = m.BroadcastNow()
		}
	}
}

func (m *MulticastDiscovery) listenLoop() {
	defer m.wg.Done()
	buf := make([]byte, 4096)

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			_ = m.listenerConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, _, err := m.listenerConn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			var announcement PeerAnnouncement
			if err := json.Unmarshal(buf[:n], &announcement); err != nil {
				continue
			}

			// Ignore own broadcasts
			m.mu.RLock()
			selfID := m.localNode.NodeID
			m.mu.RUnlock()

			if announcement.NodeID == selfID {
				continue
			}

			if m.onPeerFound != nil {
				m.onPeerFound(announcement)
			}
		}
	}
}

// Stop gracefully shuts down discovery routines and closes sockets.
func (m *MulticastDiscovery) Stop() {
	m.cancel()
	if m.listenerConn != nil {
		_ = m.listenerConn.Close()
	}
	if m.senderConn != nil {
		_ = m.senderConn.Close()
	}
	m.wg.Wait()
	log.Printf("[DISCOVERY] Multicast discovery stopped gracefully.")
}
