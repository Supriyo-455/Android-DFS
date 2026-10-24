package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/android-dfs/storage-node/pkg/config"
	"github.com/android-dfs/storage-node/pkg/entropy"
	"github.com/android-dfs/storage-node/pkg/gossip"
	"github.com/android-dfs/storage-node/pkg/resilience"
	"github.com/android-dfs/storage-node/pkg/storage"
)

// Server provides the HTTP/REST and real-time streaming management engine.
type Server struct {
	cfg          *config.Config
	cas          *storage.ContentAddressableStore
	fastcdc      *storage.FastCDC
	f2fs         *storage.F2FSOptimizer
	routingTable *gossip.RoutingTable
	gossipEngine *gossip.GossipEngine
	hub          *Hub
	androidInfo  resilience.AndroidInfo
	startTime    time.Time
}

// NewServer builds an API server instance.
func NewServer(
	cfg *config.Config,
	cas *storage.ContentAddressableStore,
	fastcdc *storage.FastCDC,
	f2fs *storage.F2FSOptimizer,
	rt *gossip.RoutingTable,
	ge *gossip.GossipEngine,
	hub *Hub,
	androidInfo resilience.AndroidInfo,
) *Server {
	return &Server{
		cfg:          cfg,
		cas:          cas,
		fastcdc:      fastcdc,
		f2fs:         f2fs,
		routingTable: rt,
		gossipEngine: ge,
		hub:          hub,
		androidInfo:  androidInfo,
		startTime:    time.Now(),
	}
}

// RegisterRoutes registers all API routes onto the given ServeMux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/nodes", s.handleNodes)
	mux.HandleFunc("/api/nodes/sync", s.handleForceSync)
	mux.HandleFunc("/api/gossip", s.handleGossip)
	mux.HandleFunc("/api/upload", s.handleUpload)
	mux.HandleFunc("/api/chunks/list", s.handleChunksList)
	mux.HandleFunc("/api/chunks/", s.handleChunkGet)
	mux.HandleFunc("/api/action/shutdown", s.handleShutdown)
	mux.HandleFunc("/ws", s.hub.HandleWebSocket)
	mux.HandleFunc("/api/events", s.hub.HandleSSE)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hashes, _ := s.cas.ListHashes()
	tree := entropy.NewMerkleTree(hashes)
	casStats := s.cas.Stats()
	f2fsStats := s.f2fs.GetStats()
	peers := s.routingTable.GetPeers()

	response := map[string]interface{}{
		"node_id":          s.cfg.NodeID,
		"node_name":        s.cfg.NodeName,
		"cluster_name":     s.cfg.ClusterName,
		"http_addr":        s.cfg.HTTPAddr,
		"uptime_seconds":   int64(time.Since(s.startTime).Seconds()),
		"merkle_root":      tree.RootHash(),
		"cas_stats":        casStats,
		"f2fs_stats":       f2fsStats,
		"android_info":     s.androidInfo,
		"active_peers":     len(peers),
		"timestamp":        time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	peers := s.routingTable.GetPeers()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(peers)
}

func (s *Server) handleForceSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		TargetID string `json:"target_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.gossipEngine.ForceSync(payload.TargetID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": fmt.Sprintf("Anti-entropy sync triggered with peer %s", payload.TargetID),
	})
}

func (s *Server) handleGossip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg gossip.GossipMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid gossip message", http.StatusBadRequest)
		return
	}

	// Register or update peer
	s.routingTable.Upsert(gossip.PeerNode{
		ID:          msg.SenderID,
		Name:        msg.SenderID,
		HTTPAddr:    r.RemoteAddr,
		MerkleRoot:  msg.MerkleRoot,
		TotalChunks: msg.TotalChunks,
		StoredBytes: msg.StoredBytes,
		Status:      gossip.StatusActive,
		LastSeen:    time.Now(),
	})

	// Reply with our state
	hashes, _ := s.cas.ListHashes()
	localTree := entropy.NewMerkleTree(hashes)
	stats := s.cas.Stats()

	reply := gossip.GossipMessage{
		SenderID:    s.cfg.NodeID,
		MerkleRoot:  localTree.RootHash(),
		TotalChunks: stats.TotalChunks,
		StoredBytes: stats.StoredBytes,
		Timestamp:   time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read body (supports both raw streams and multipart uploads)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read upload stream", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if len(bodyBytes) == 0 {
		http.Error(w, "Empty payload", http.StatusBadRequest)
		return
	}

	// Perform FastCDC Content-Defined Chunking
	chunks := s.fastcdc.ChunkBuffer(bodyBytes)

	var newChunksCount int
	var dedupChunksCount int
	chunkSummaries := make([]map[string]interface{}, 0, len(chunks))

	for _, chunk := range chunks {
		// Store in CAS
		isNew, err := s.cas.Put(chunk.Hash, chunk.Data)
		if err != nil {
			log.Printf("[CAS] Error storing chunk %s: %v", chunk.Hash, err)
			continue
		}

		// Log F2FS flash alignment write
		s.f2fs.RecordWrite(len(chunk.Data))

		if isNew {
			newChunksCount++
		} else {
			dedupChunksCount++
		}

		chunkSummaries = append(chunkSummaries, map[string]interface{}{
			"offset": chunk.Offset,
			"length": chunk.Length,
			"hash":   chunk.Hash,
			"is_new": isNew,
		})
	}

	hashes, _ := s.cas.ListHashes()
	newTree := entropy.NewMerkleTree(hashes)

	// Broadcast chunk stored event to cluster dashboard
	s.hub.BroadcastEvent(gossip.ClusterEvent{
		Type:       "chunk_stored",
		SourceNode: s.cfg.NodeID,
		Data: map[string]interface{}{
			"chunks_count": len(chunks),
			"new_chunks":   newChunksCount,
			"dedup_chunks": dedupChunksCount,
			"merkle_root":  newTree.RootHash(),
			"bytes":        len(bodyBytes),
		},
		Timestamp: time.Now(),
	})

	response := map[string]interface{}{
		"status":           "success",
		"file_size":        len(bodyBytes),
		"chunks_generated": len(chunks),
		"unique_chunks":    newChunksCount,
		"dedup_chunks":     dedupChunksCount,
		"new_merkle_root":  newTree.RootHash(),
		"chunks":           chunkSummaries,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) handleChunksList(w http.ResponseWriter, r *http.Request) {
	hashes, err := s.cas.ListHashes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(hashes)
}

func (s *Server) handleChunkGet(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/api/chunks/")
	if len(parts) < 2 || parts[1] == "" {
		http.Error(w, "Missing chunk hash", http.StatusBadRequest)
		return
	}
	hash := parts[1]

	data, err := s.cas.Get(hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Hash", hash)
	_, _ = w.Write(data)
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("[API] Graceful shutdown triggered via management interface.")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "shutting_down",
		"message": "Node daemon is releasing wakelocks and exiting gracefully.",
	})

	go func() {
		time.Sleep(500 * time.Millisecond)
		os.Exit(0)
	}()
}
