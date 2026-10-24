package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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
	manifests    *storage.ManifestStore
	fastcdc      *storage.FastCDC
	f2fs         *storage.F2FSOptimizer
	routingTable *gossip.RoutingTable
	gossipEngine *gossip.GossipEngine
	hub          *Hub
	androidInfo  resilience.AndroidInfo
	startTime    time.Time
	httpClient   *http.Client
}

// NewServer builds an API server instance.
func NewServer(
	cfg *config.Config,
	cas *storage.ContentAddressableStore,
	manifests *storage.ManifestStore,
	fastcdc *storage.FastCDC,
	f2fs *storage.F2FSOptimizer,
	rt *gossip.RoutingTable,
	ge *gossip.GossipEngine,
	hub *Hub,
	androidInfo resilience.AndroidInfo,
) *Server {
	srv := &Server{
		cfg:          cfg,
		cas:          cas,
		manifests:    manifests,
		fastcdc:      fastcdc,
		f2fs:         f2fs,
		routingTable: rt,
		gossipEngine: ge,
		hub:          hub,
		androidInfo:  androidInfo,
		startTime:    time.Now(),
		httpClient:   &http.Client{Timeout: 20 * time.Second},
	}
	srv.startPeriodicManifestSync()
	return srv
}

// RegisterRoutes registers all API routes onto the given ServeMux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/nodes", s.handleNodes)
	mux.HandleFunc("/api/nodes/sync", s.handleForceSync)
	mux.HandleFunc("/api/peers/add", s.handlePeerAdd)
	mux.HandleFunc("/api/gossip", s.handleGossip)
	mux.HandleFunc("/api/upload", s.handleUpload)
	mux.HandleFunc("/api/files", s.handleFiles)
	mux.HandleFunc("/api/files/delete", s.handleFileDelete)
	mux.HandleFunc("/api/files/download", s.handleFileDownload)
	mux.HandleFunc("/api/files/download/", s.handleFileDownload)
	mux.HandleFunc("/api/chunks/list", s.handleChunksList)
	mux.HandleFunc("/api/chunks/put", s.handleChunkPut)
	mux.HandleFunc("/api/chunks/", s.handleChunkGet)
	mux.HandleFunc("/api/manifests/sync", s.handleManifestSync)
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
	sysMetrics := resilience.GetSystemMetrics(s.cfg.DataDir)
	files := s.manifests.ListFiles()

	response := map[string]interface{}{
		"node_id":            s.cfg.NodeID,
		"node_name":          s.cfg.NodeName,
		"cluster_name":       s.cfg.ClusterName,
		"http_addr":          s.cfg.HTTPAddr,
		"uptime_seconds":     int64(time.Since(s.startTime).Seconds()),
		"merkle_root":        tree.RootHash(),
		"cas_stats":          casStats,
		"f2fs_stats":         f2fsStats,
		"android_info":       s.androidInfo,
		"system_metrics":     sysMetrics,
		"active_peers":       len(peers),
		"stored_files_count": len(files),
		"timestamp":          time.Now().Unix(),
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

func (s *Server) handlePeerAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Addr string `json:"addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.gossipEngine.ConnectPeer(payload.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"message": fmt.Sprintf("Connected to peer at %s", payload.Addr),
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

	peerAddr := msg.SenderHTTPAddr
	if peerAddr == "" {
		host := r.RemoteAddr
		if colon := strings.LastIndex(host, ":"); colon != -1 {
			host = host[:colon]
		}
		peerAddr = host + ":8080"
	}

	peerName := msg.SenderName
	if peerName == "" {
		peerName = msg.SenderID
	}

	// Register or update peer
	s.routingTable.Upsert(gossip.PeerNode{
		ID:          msg.SenderID,
		Name:        peerName,
		HTTPAddr:    peerAddr,
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

	// Determine filename
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		filename = r.Header.Get("X-Filename")
	}
	if filename == "" {
		filename = fmt.Sprintf("file_%s.bin", time.Now().Format("20060102_150405"))
	}

	// Perform FastCDC Content-Defined Chunking
	chunks := s.fastcdc.ChunkBuffer(bodyBytes)

	// Identify active cluster peers for load balancing
	var activePeers []gossip.PeerNode
	for _, p := range s.routingTable.GetPeers() {
		if p.HTTPAddr != "" && p.Status == gossip.StatusActive {
			activePeers = append(activePeers, p)
		}
	}

	numNodes := 1 + len(activePeers)
	var newChunksCount int
	var dedupChunksCount int
	chunkSummaries := make([]map[string]interface{}, 0, len(chunks))
	chunkHashes := make([]string, 0, len(chunks))
	chunkHosts := make(map[string]string)

	for i, chunk := range chunks {
		chunkHashes = append(chunkHashes, chunk.Hash)
		targetIdx := i % numNodes

		var isNew bool
		var targetNodeName string

		if targetIdx == 0 {
			// Store on local node
			var err error
			isNew, err = s.cas.Put(chunk.Hash, chunk.Data)
			if err != nil {
				log.Printf("[CAS] Error storing local chunk %s: %v", chunk.Hash, err)
				continue
			}
			s.f2fs.RecordWrite(len(chunk.Data))
			chunkHosts[chunk.Hash] = "local"
			targetNodeName = s.cfg.NodeName
		} else {
			// Balance load: Store on remote peer phone
			peer := activePeers[targetIdx-1]
			var err error
			isNew, err = s.storeChunkOnPeer(peer.HTTPAddr, chunk.Hash, chunk.Data)
			if err != nil {
				// Fallback to storing locally if remote peer network transfer encounters an error
				log.Printf("[DISTRIBUTED] Peer %s write failed for chunk %s (%v), storing locally", peer.Name, chunk.Hash, err)
				isNew, err = s.cas.Put(chunk.Hash, chunk.Data)
				if err != nil {
					log.Printf("[CAS] Fallback local store failed for chunk %s: %v", chunk.Hash, err)
					continue
				}
				s.f2fs.RecordWrite(len(chunk.Data))
				chunkHosts[chunk.Hash] = "local"
				targetNodeName = s.cfg.NodeName
			} else {
				chunkHosts[chunk.Hash] = peer.HTTPAddr
				targetNodeName = peer.Name
			}
		}

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
			"node":   targetNodeName,
		})
	}

	// Register file manifest for full cluster retrieval and reassembly
	fileRec := storage.FileRecord{
		ID:          storage.ComputeSHA256(bodyBytes),
		Filename:    filename,
		Size:        int64(len(bodyBytes)),
		ChunkCount:  len(chunks),
		ChunkHashes: chunkHashes,
		ChunkHosts:  chunkHosts,
		UploadTime:  time.Now(),
		ContentType: http.DetectContentType(bodyBytes),
	}
	_ = s.manifests.RegisterFile(fileRec)

	// Propagate file manifest to all cluster peers
	go s.broadcastManifest(fileRec)

	hashes, _ := s.cas.ListHashes()
	newTree := entropy.NewMerkleTree(hashes)

	// Broadcast chunk stored event to cluster dashboard
	s.hub.BroadcastEvent(gossip.ClusterEvent{
		Type:       "chunk_stored",
		SourceNode: s.cfg.NodeID,
		Data: map[string]interface{}{
			"filename":     filename,
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
		"file_id":          fileRec.ID,
		"filename":         filename,
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

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		files := s.manifests.ListFiles()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(files)
		return
	}
	if r.Method == http.MethodDelete {
		s.handleFileDelete(w, r)
		return
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleFileDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID string `json:"id"`
	}

	if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.ID == "" {
		req.ID = r.URL.Query().Get("id")
	}
	if req.ID == "" {
		req.ID = r.URL.Query().Get("name")
	}

	if req.ID == "" {
		http.Error(w, `{"error":"Missing file ID"}`, http.StatusBadRequest)
		return
	}

	rec, exists := s.manifests.GetFile(req.ID)
	if !exists {
		http.Error(w, `{"error":"File not found"}`, http.StatusNotFound)
		return
	}

	if err := s.manifests.DeleteFile(req.ID, s.cas); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	// Propagate deletion to cluster peers if requested
	if r.URL.Query().Get("propagate") != "false" {
		go func(fileId string) {
			delPayload, _ := json.Marshal(map[string]string{"id": fileId})
			for _, peer := range s.routingTable.GetPeers() {
				if peer.HTTPAddr != "" && peer.Status == gossip.StatusActive {
					delURL := fmt.Sprintf("http://%s/api/files/delete?id=%s&propagate=false", peer.HTTPAddr, fileId)
					req, err := http.NewRequestWithContext(context.Background(), "POST", delURL, bytes.NewBuffer(delPayload))
					if err == nil {
						req.Header.Set("Content-Type", "application/json")
						resp, err := s.httpClient.Do(req)
						if err == nil {
							resp.Body.Close()
						}
					}
				}
			}
		}(req.ID)
	}

	hashes, _ := s.cas.ListHashes()
	newTree := entropy.NewMerkleTree(hashes)

	// Broadcast file deletion event to cluster dashboard
	s.hub.BroadcastEvent(gossip.ClusterEvent{
		Type:       "file_deleted",
		SourceNode: s.cfg.NodeID,
		Data: map[string]interface{}{
			"file_id":     rec.ID,
			"filename":    rec.Filename,
			"merkle_root": newTree.RootHash(),
		},
		Timestamp: time.Now(),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":          "success",
		"file_id":         rec.ID,
		"filename":        rec.Filename,
		"new_merkle_root": newTree.RootHash(),
	})
}

func (s *Server) handleFileDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		id = r.URL.Query().Get("name")
	}
	if id == "" {
		parts := strings.Split(r.URL.Path, "/api/files/download/")
		if len(parts) >= 2 && parts[1] != "" {
			id = parts[1]
		}
	}

	if id == "" {
		http.Error(w, "Missing file id or name parameter", http.StatusBadRequest)
		return
	}

	rec, exists := s.manifests.GetFile(id)
	if !exists {
		http.Error(w, "File not found in cluster manifest store", http.StatusNotFound)
		return
	}

	cleanFilename := filepath.Base(rec.Filename)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, cleanFilename))
	if rec.ContentType != "" {
		w.Header().Set("Content-Type", rec.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Header().Set("Content-Length", strconv.FormatInt(rec.Size, 10))

	// Reassemble file using cluster-aware chunk fetcher
	chunkFetcher := func(chunkHash string) ([]byte, error) {
		// 1. Check local CAS first
		if data, err := s.cas.Get(chunkHash); err == nil {
			return data, nil
		}

		// 2. If chunk location is recorded in manifest, fetch from that peer
		if host, ok := rec.ChunkHosts[chunkHash]; ok && host != "" && host != "local" {
			data, err := s.fetchChunkFromHost(host, chunkHash)
			if err == nil {
				return data, nil
			}
		}

		// 3. Fallback: query all active peers
		for _, peer := range s.routingTable.GetPeers() {
			if peer.HTTPAddr != "" && peer.Status == gossip.StatusActive {
				data, err := s.fetchChunkFromHost(peer.HTTPAddr, chunkHash)
				if err == nil {
					return data, nil
				}
			}
		}

		return nil, fmt.Errorf("chunk %s unavailable on local storage or connected peers", chunkHash)
	}

	if err := s.manifests.ReassembleStream(id, chunkFetcher, w); err != nil {
		log.Printf("[DOWNLOAD] Error reassembling distributed file %s: %v", id, err)
	}
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

func (s *Server) handleChunkPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hash := r.URL.Query().Get("hash")
	if hash == "" {
		hash = r.Header.Get("X-Chunk-Hash")
	}

	data, err := io.ReadAll(r.Body)
	if err != nil || len(data) == 0 {
		http.Error(w, "Empty or invalid chunk payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if hash == "" {
		hash = storage.ComputeSHA256(data)
	}

	isNew, err := s.cas.Put(hash, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("CAS store failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.f2fs.RecordWrite(len(data))

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"hash":   hash,
		"is_new": isNew,
		"size":   len(data),
	})
}

func (s *Server) handleManifestSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var rec storage.FileRecord
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_ = s.manifests.RegisterFile(rec)

	s.hub.BroadcastEvent(gossip.ClusterEvent{
		Type:       "manifest_synced",
		SourceNode: s.cfg.NodeID,
		Data: map[string]interface{}{
			"file_id":  rec.ID,
			"filename": rec.Filename,
		},
		Timestamp: time.Now(),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "synced"})
}

func (s *Server) storeChunkOnPeer(peerAddr, hash string, data []byte) (bool, error) {
	putURL := fmt.Sprintf("http://%s/api/chunks/put?hash=%s", peerAddr, hash)
	req, err := http.NewRequestWithContext(context.Background(), "POST", putURL, bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Chunk-Hash", hash)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("remote peer returned HTTP %d", resp.StatusCode)
	}

	var res struct {
		IsNew bool `json:"is_new"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&res)
	return res.IsNew, nil
}

func (s *Server) broadcastManifest(rec storage.FileRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	for _, peer := range s.routingTable.GetPeers() {
		if peer.HTTPAddr != "" && peer.Status == gossip.StatusActive {
			syncURL := fmt.Sprintf("http://%s/api/manifests/sync", peer.HTTPAddr)
			req, err := http.NewRequestWithContext(context.Background(), "POST", syncURL, bytes.NewBuffer(data))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				resp, err := s.httpClient.Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}
	}
}

func (s *Server) fetchChunkFromHost(host, hash string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/api/chunks/%s", host, hash)
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote peer %s returned HTTP %d for chunk %s", host, resp.StatusCode, hash)
	}

	return io.ReadAll(resp.Body)
}

func (s *Server) syncManifestsFromPeer(peerAddr string) {
	url := fmt.Sprintf("http://%s/api/files", peerAddr)
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var remoteFiles []storage.FileRecord
	if err := json.NewDecoder(resp.Body).Decode(&remoteFiles); err != nil {
		return
	}

	for _, rf := range remoteFiles {
		if _, exists := s.manifests.GetFile(rf.ID); !exists {
			_ = s.manifests.RegisterFile(rf)
			s.hub.BroadcastEvent(gossip.ClusterEvent{
				Type:       "manifest_synced",
				SourceNode: s.cfg.NodeID,
				Data: map[string]interface{}{
					"file_id":  rf.ID,
					"filename": rf.Filename,
				},
				Timestamp: time.Now(),
			})
		}
	}
}

func (s *Server) startPeriodicManifestSync() {
	ticker := time.NewTicker(12 * time.Second)
	go func() {
		for range ticker.C {
			peers := s.routingTable.GetPeers()
			for _, p := range peers {
				if p.HTTPAddr != "" && p.Status == gossip.StatusActive {
					s.syncManifestsFromPeer(p.HTTPAddr)
				}
			}
		}
	}()
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
