package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/android-dfs/storage-node/pkg/resilience"
	"github.com/android-dfs/storage-node/pkg/storage"
)

// PhoneNode tracks the live state and telemetry of an Android storage phone.
type PhoneNode struct {
	ID          string                   `json:"id"`
	Name        string                   `json:"name"`
	Role        string                   `json:"role"`
	Addr        string                   `json:"addr"` // e.g. 192.168.0.110:8080
	Online      bool                     `json:"online"`
	LastSeen    time.Time                `json:"last_seen"`
	Metrics     resilience.SystemMetrics `json:"metrics"`
	TotalChunks int64                    `json:"total_chunks"`
	StoredBytes int64                    `json:"stored_bytes"`
	MerkleRoot  string                   `json:"merkle_root"`
	AndroidInfo resilience.AndroidInfo   `json:"android_info"`
	FilesCount  int                      `json:"files_count"`
	LatencyMs   int64                    `json:"latency_ms"`
}

// CombinedMetrics aggregates storage, compute, and battery stats across the phone fleet.
type CombinedMetrics struct {
	TotalPhones       int     `json:"total_phones"`
	OnlinePhones      int     `json:"online_phones"`
	StorageTotalBytes uint64  `json:"storage_total_bytes"`
	StorageFreeBytes  uint64  `json:"storage_free_bytes"`
	StorageUsedBytes  uint64  `json:"storage_used_bytes"`
	StorageUsedPct    float64 `json:"storage_used_pct"`
	RAMTotalBytes     uint64  `json:"ram_total_bytes"`
	RAMUsedBytes      uint64  `json:"ram_used_bytes"`
	RAMUsagePct       float64 `json:"ram_usage_pct"`
	AvgCPUPct         float64 `json:"avg_cpu_pct"`
	AvgBatteryPct     int     `json:"avg_battery_pct"`
	TotalChunks       int64   `json:"total_chunks"`
	StoredBytes       int64   `json:"stored_bytes"`
	TotalFiles        int     `json:"total_files"`
}

// DesktopServer coordinates desktop cloud storage actions across the phone fleet.
type DesktopServer struct {
	phones     map[string]*PhoneNode
	phonesMu   sync.RWMutex
	manifests  *storage.ManifestStore
	fastcdc    *storage.FastCDC
	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc
	uploadRR   uint64
}

// NewDesktopServer initializes the desktop cloud controller.
func NewDesktopServer(dataDir string) (*DesktopServer, error) {
	manifests, err := storage.NewManifestStore(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create desktop manifest store: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ds := &DesktopServer{
		phones:     make(map[string]*PhoneNode),
		manifests:  manifests,
		fastcdc:    storage.NewFastCDC(64*1024, 256*1024, 1024*1024, 2),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		ctx:        ctx,
		cancel:     cancel,
	}

	return ds, nil
}

// RegisterRoutes sets up REST endpoints for the desktop cloud portal.
func (ds *DesktopServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/status", ds.handleStatus)
	mux.HandleFunc("/api/cluster/status", ds.handleClusterStatus)
	mux.HandleFunc("/api/cluster/phones/add", ds.handleAddPhone)
	mux.HandleFunc("/api/cluster/phones/remove", ds.handleRemovePhone)
	mux.HandleFunc("/api/upload", ds.handleUpload)
	mux.HandleFunc("/api/files", ds.handleFiles)
	mux.HandleFunc("/api/files/download/", ds.handleFileDownload)
	mux.HandleFunc("/api/files/download", ds.handleFileDownload)
	mux.HandleFunc("/api/files/delete", ds.handleFileDelete)

	// Gossip compatibility stub
	mux.HandleFunc("/api/gossip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"role":    "master",
			"node_id": "desktop-controller",
			"status":  "ok",
		})
	})
}

func (ds *DesktopServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":   "desktop-controller",
		"node_name": "Desktop Master Controller",
		"role":      "master",
		"http_addr": r.Host,
	})
}

// AddPhone registers an Android phone IP address into the cluster.
func (ds *DesktopServer) AddPhone(addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	if !strings.Contains(addr, ":") {
		addr += ":8080"
	}

	ds.phonesMu.Lock()
	if _, exists := ds.phones[addr]; !exists {
		ds.phones[addr] = &PhoneNode{
			Addr:     addr,
			Name:     "Phone (" + addr + ")",
			Online:   false,
			LastSeen: time.Now(),
		}
		log.Printf("[FLEET] Added phone node %s to desktop cluster", addr)
	}
	ds.phonesMu.Unlock()

	go ds.pollSinglePhone(addr)
}

// StartBackgroundPoller periodically sweeps all phones to refresh live telemetry and manifests.
func (ds *DesktopServer) StartBackgroundPoller() {
	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case <-ds.ctx.Done():
				return
			case <-ticker.C:
				ds.pollAllPhones()
			}
		}
	}()
}

func (ds *DesktopServer) pollAllPhones() {
	ds.phonesMu.RLock()
	addrs := make([]string, 0, len(ds.phones))
	for addr := range ds.phones {
		addrs = append(addrs, addr)
	}
	ds.phonesMu.RUnlock()

	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			ds.pollSinglePhone(a)
		}(addr)
	}
	wg.Wait()
}

func (ds *DesktopServer) pollSinglePhone(addr string) {
	statusURL := fmt.Sprintf("http://%s/api/status", addr)
	start := time.Now()
	req, err := http.NewRequestWithContext(ds.ctx, "GET", statusURL, nil)
	if err != nil {
		ds.markPhoneOffline(addr)
		return
	}

	resp, err := ds.httpClient.Do(req)
	if err != nil {
		ds.markPhoneOffline(addr)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ds.markPhoneOffline(addr)
		return
	}

	latency := time.Since(start).Milliseconds()

	var payload struct {
		NodeID           string                   `json:"node_id"`
		NodeName         string                   `json:"node_name"`
		Role             string                   `json:"role"`
		MerkleRoot       string                   `json:"merkle_root"`
		AndroidInfo      resilience.AndroidInfo   `json:"android_info"`
		SystemMetrics    resilience.SystemMetrics `json:"system_metrics"`
		CASStats         storage.CASStats         `json:"cas_stats"`
		StoredFilesCount int                      `json:"stored_files_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return
	}

	ds.phonesMu.Lock()
	p, exists := ds.phones[addr]
	if exists {
		displayName := payload.NodeName
		if displayName == "localhost" || displayName == "" {
			if payload.AndroidInfo.IsAndroid {
				if payload.AndroidInfo.AndroidVersion > 0 {
					displayName = fmt.Sprintf("Android %d (%s)", payload.AndroidInfo.AndroidVersion, addr)
				} else {
					displayName = fmt.Sprintf("Android Phone (%s)", addr)
				}
			} else {
				displayName = fmt.Sprintf("Phone (%s)", addr)
			}
		}

		p.ID = payload.NodeID
		p.Name = displayName
		p.Role = "slave"
		p.Online = true
		p.LastSeen = time.Now()
		p.Metrics = payload.SystemMetrics
		p.TotalChunks = payload.CASStats.TotalChunks
		p.StoredBytes = payload.CASStats.StoredBytes
		p.MerkleRoot = payload.MerkleRoot
		p.AndroidInfo = payload.AndroidInfo
		p.FilesCount = payload.StoredFilesCount
		p.LatencyMs = latency
	}
	ds.phonesMu.Unlock()

	// Sync file catalog from this phone into desktop manifest store
	ds.syncFilesFromPhone(addr)
}

func (ds *DesktopServer) markPhoneOffline(addr string) {
	ds.phonesMu.Lock()
	defer ds.phonesMu.Unlock()
	if p, exists := ds.phones[addr]; exists {
		p.Online = false
	}
}

func (ds *DesktopServer) syncFilesFromPhone(addr string) {
	filesURL := fmt.Sprintf("http://%s/api/files", addr)
	req, err := http.NewRequestWithContext(ds.ctx, "GET", filesURL, nil)
	if err != nil {
		return
	}

	resp, err := ds.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var remoteFiles []storage.FileRecord
	if err := json.NewDecoder(resp.Body).Decode(&remoteFiles); err != nil {
		return
	}

	for _, rf := range remoteFiles {
		if _, exists := ds.manifests.GetFile(rf.ID); !exists {
			_ = ds.manifests.RegisterFile(rf)
		}
	}
}

func (ds *DesktopServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	ds.phonesMu.RLock()
	defer ds.phonesMu.RUnlock()

	phoneList := make([]PhoneNode, 0, len(ds.phones))
	var comb CombinedMetrics
	comb.TotalPhones = len(ds.phones)

	var batterySum int
	var batteryCount int
	var cpuSum float64

	for _, p := range ds.phones {
		phoneList = append(phoneList, *p)
		if p.Online {
			comb.OnlinePhones++
			comb.StorageTotalBytes += p.Metrics.StorageTotalBytes
			comb.StorageFreeBytes += p.Metrics.StorageFreeBytes
			comb.StorageUsedBytes += p.Metrics.StorageUsedBytes
			comb.RAMTotalBytes += p.Metrics.RAMTotalBytes
			comb.RAMUsedBytes += p.Metrics.RAMUsedBytes
			comb.TotalChunks += p.TotalChunks
			comb.StoredBytes += p.StoredBytes

			cpuSum += p.Metrics.CPUUsagePct
			if p.Metrics.BatteryPct > 0 {
				batterySum += p.Metrics.BatteryPct
				batteryCount++
			}
		}
	}

	if comb.StorageTotalBytes > 0 {
		comb.StorageUsedPct = (float64(comb.StorageUsedBytes) / float64(comb.StorageTotalBytes)) * 100.0
	}
	if comb.RAMTotalBytes > 0 {
		comb.RAMUsagePct = (float64(comb.RAMUsedBytes) / float64(comb.RAMTotalBytes)) * 100.0
	}
	if comb.OnlinePhones > 0 {
		comb.AvgCPUPct = cpuSum / float64(comb.OnlinePhones)
	}
	if batteryCount > 0 {
		comb.AvgBatteryPct = batterySum / batteryCount
	}

	files := ds.manifests.ListFiles()
	comb.TotalFiles = len(files)

	response := map[string]interface{}{
		"combined":  comb,
		"phones":    phoneList,
		"timestamp": time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (ds *DesktopServer) handleAddPhone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addr string `json:"addr"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Addr == "" {
		req.Addr = r.URL.Query().Get("addr")
	}

	if req.Addr == "" {
		http.Error(w, "Missing phone address", http.StatusBadRequest)
		return
	}

	ds.AddPhone(req.Addr)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "addr": req.Addr})
}

func (ds *DesktopServer) handleRemovePhone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Addr string `json:"addr"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Addr == "" {
		req.Addr = r.URL.Query().Get("addr")
	}

	ds.phonesMu.Lock()
	delete(ds.phones, req.Addr)
	ds.phonesMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "removed", "addr": req.Addr})
}

// pickBalancedTargetPhone chooses the online phone with the minimum stored chunks and bytes.
// If multiple phones are tied, it round-robins across them so no phone remains idle.
func (ds *DesktopServer) pickBalancedTargetPhone(onlinePhones []*PhoneNode) *PhoneNode {
	if len(onlinePhones) == 0 {
		return nil
	}
	if len(onlinePhones) == 1 {
		return onlinePhones[0]
	}

	ds.phonesMu.RLock()
	defer ds.phonesMu.RUnlock()

	var best *PhoneNode
	minScore := int64(1<<62 - 1)

	for _, p := range onlinePhones {
		// Calculate workload score (prioritizing chunk count first, then total stored bytes)
		score := p.TotalChunks*1000000 + p.StoredBytes
		if score < minScore {
			minScore = score
			best = p
		}
	}

	if best == nil {
		idx := atomic.AddUint64(&ds.uploadRR, 1) % uint64(len(onlinePhones))
		return onlinePhones[idx]
	}

	return best
}

func (ds *DesktopServer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil || len(bodyBytes) == 0 {
		http.Error(w, "Invalid or empty payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	filename := r.URL.Query().Get("filename")
	if filename == "" {
		filename = r.Header.Get("X-Filename")
	}
	if filename == "" {
		filename = fmt.Sprintf("upload_%s.bin", time.Now().Format("20060102_150405"))
	}

	// Find all online phones
	ds.phonesMu.RLock()
	var onlinePhones []*PhoneNode
	for _, p := range ds.phones {
		if p.Online {
			onlinePhones = append(onlinePhones, p)
		}
	}
	ds.phonesMu.RUnlock()

	if len(onlinePhones) == 0 {
		http.Error(w, "No active phone nodes connected to desktop cluster. Connect at least one phone first!", http.StatusServiceUnavailable)
		return
	}

	// Perform FastCDC Content-Defined Chunking
	chunks := ds.fastcdc.ChunkBuffer(bodyBytes)
	chunkHashes := make([]string, 0, len(chunks))
	chunkHosts := make(map[string]string)
	chunkSummaries := make([]map[string]interface{}, 0, len(chunks))

	var uniqueCount int
	var dedupCount int

	// Dynamic least-loaded striping across all online phone slaves
	for _, chunk := range chunks {
		chunkHashes = append(chunkHashes, chunk.Hash)
		targetPhone := ds.pickBalancedTargetPhone(onlinePhones)
		if targetPhone == nil {
			targetPhone = onlinePhones[0]
		}

		isNew, err := ds.putChunkToPhone(targetPhone.Addr, chunk.Hash, chunk.Data)
		if err != nil {
			// Try fallback to any other online phone
			stored := false
			for _, alt := range onlinePhones {
				if alt.Addr != targetPhone.Addr {
					if n, e := ds.putChunkToPhone(alt.Addr, chunk.Hash, chunk.Data); e == nil {
						isNew = n
						targetPhone = alt
						stored = true
						break
					}
				}
			}
			if !stored {
				http.Error(w, fmt.Sprintf("Failed to store chunk %s across phones: %v", chunk.Hash, err), http.StatusInternalServerError)
				return
			}
		}

		// Update in-memory load stats immediately so subsequent chunks/uploads balance accurately
		ds.phonesMu.Lock()
		if p, ok := ds.phones[targetPhone.Addr]; ok {
			p.TotalChunks++
			p.StoredBytes += int64(len(chunk.Data))
		}
		ds.phonesMu.Unlock()

		chunkHosts[chunk.Hash] = targetPhone.Addr

		if isNew {
			uniqueCount++
		} else {
			dedupCount++
		}

		chunkSummaries = append(chunkSummaries, map[string]interface{}{
			"offset": chunk.Offset,
			"length": chunk.Length,
			"hash":   chunk.Hash,
			"phone":  targetPhone.Name,
			"is_new": isNew,
		})
	}

	// Register file manifest in desktop store
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
	_ = ds.manifests.RegisterFile(fileRec)

	// Propagate manifest to all online phones
	go ds.broadcastManifestToPhones(fileRec, onlinePhones)

	response := map[string]interface{}{
		"status":           "success",
		"file_id":          fileRec.ID,
		"filename":         filename,
		"file_size":        len(bodyBytes),
		"chunks_generated": len(chunks),
		"unique_chunks":    uniqueCount,
		"dedup_chunks":     dedupCount,
		"chunks":           chunkSummaries,
		"target_phones":    len(onlinePhones),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (ds *DesktopServer) putChunkToPhone(addr, hash string, data []byte) (bool, error) {
	putURL := fmt.Sprintf("http://%s/api/chunks/put?hash=%s", addr, hash)
	req, err := http.NewRequestWithContext(ds.ctx, "POST", putURL, bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Chunk-Hash", hash)

	resp, err := ds.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("phone %s returned HTTP %d", addr, resp.StatusCode)
	}

	var res struct {
		IsNew bool `json:"is_new"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&res)
	return res.IsNew, nil
}

func (ds *DesktopServer) broadcastManifestToPhones(rec storage.FileRecord, phones []*PhoneNode) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	for _, p := range phones {
		syncURL := fmt.Sprintf("http://%s/api/manifests/sync", p.Addr)
		req, err := http.NewRequestWithContext(context.Background(), "POST", syncURL, bytes.NewBuffer(data))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			resp, err := ds.httpClient.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}
	}
}

func (ds *DesktopServer) handleFiles(w http.ResponseWriter, r *http.Request) {
	files := ds.manifests.ListFiles()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(files)
}

func (ds *DesktopServer) handleFileDownload(w http.ResponseWriter, r *http.Request) {
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

	rec, exists := ds.manifests.GetFile(id)
	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
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

	// Reassemble chunks by streaming from designated host phones over LAN
	for _, chunkHash := range rec.ChunkHashes {
		data, err := ds.fetchChunkFromFleet(rec, chunkHash)
		if err != nil {
			log.Printf("[DOWNLOAD] Error fetching chunk %s: %v", chunkHash, err)
			return
		}
		if _, err := w.Write(data); err != nil {
			return
		}
	}
}

func (ds *DesktopServer) fetchChunkFromFleet(rec storage.FileRecord, hash string) ([]byte, error) {
	// Try the designated host first
	if host, ok := rec.ChunkHosts[hash]; ok && host != "" {
		data, err := ds.fetchChunkFromHost(host, hash)
		if err == nil {
			return data, nil
		}
	}

	// Fallback to any online phone
	ds.phonesMu.RLock()
	var addrs []string
	for addr, p := range ds.phones {
		if p.Online {
			addrs = append(addrs, addr)
		}
	}
	ds.phonesMu.RUnlock()

	for _, addr := range addrs {
		data, err := ds.fetchChunkFromHost(addr, hash)
		if err == nil {
			return data, nil
		}
	}

	return nil, fmt.Errorf("chunk %s missing across all phones in fleet", hash)
}

func (ds *DesktopServer) fetchChunkFromHost(host, hash string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/api/chunks/%s", host, hash)
	req, err := http.NewRequestWithContext(ds.ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := ds.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned HTTP %d for chunk %s", host, resp.StatusCode, hash)
	}

	return io.ReadAll(resp.Body)
}

func (ds *DesktopServer) handleFileDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.ID == "" {
		req.ID = r.URL.Query().Get("id")
	}

	if req.ID == "" {
		http.Error(w, "Missing file ID", http.StatusBadRequest)
		return
	}

	rec, exists := ds.manifests.GetFile(req.ID)
	if !exists {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Delete from local desktop manifest store
	_ = ds.manifests.DeleteFile(req.ID, nil)

	// Command all phones to delete their respective chunks and manifests
	ds.phonesMu.RLock()
	for addr, p := range ds.phones {
		if p.Online {
			go func(a string) {
				delURL := fmt.Sprintf("http://%s/api/files/delete?id=%s", a, req.ID)
				dReq, err := http.NewRequestWithContext(context.Background(), "POST", delURL, nil)
				if err == nil {
					resp, err := ds.httpClient.Do(dReq)
					if err == nil {
						resp.Body.Close()
					}
				}
			}(addr)
		}
	}
	ds.phonesMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"file_id":  rec.ID,
		"filename": rec.Filename,
	})
}
