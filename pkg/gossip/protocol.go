package gossip

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/android-dfs/storage-node/pkg/entropy"
	"github.com/android-dfs/storage-node/pkg/storage"
)

// GossipMessage is exchanged during epidemic routing rounds.
type GossipMessage struct {
	SenderID       string   `json:"sender_id"`
	SenderName     string   `json:"sender_name"`
	SenderHTTPAddr string   `json:"sender_http_addr"`
	MerkleRoot     string   `json:"merkle_root"`
	TotalChunks    int64    `json:"total_chunks"`
	StoredBytes    int64    `json:"stored_bytes"`
	PeerList       []string `json:"peer_list"`
	Timestamp      int64    `json:"timestamp"`
}

// GossipEngine orchestrates peer state dissemination and anti-entropy synchronization.
type GossipEngine struct {
	nodeID         string
	senderName     string
	senderHTTPAddr string
	routingTable   *RoutingTable
	casStore       *storage.ContentAddressableStore
	interval       time.Duration
	fanout         int
	httpClient     *http.Client
	eventChan      chan ClusterEvent
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.RWMutex
}

// NewGossipEngine creates a new Gossip Protocol instance.
func NewGossipEngine(
	nodeID string,
	rt *RoutingTable,
	cas *storage.ContentAddressableStore,
	interval time.Duration,
	events chan ClusterEvent,
) *GossipEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &GossipEngine{
		nodeID:       nodeID,
		routingTable: rt,
		casStore:     cas,
		interval:     interval,
		fanout:       3, // Default epidemic fanout
		httpClient: &http.Client{
			Timeout: 4 * time.Second,
		},
		eventChan: events,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// SetLocalDetails configures the local name and address advertised in gossip exchanges.
func (g *GossipEngine) SetLocalDetails(name, addr string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.senderName = name
	g.senderHTTPAddr = addr
}

// ConnectPeer dials a peer directly by IP:Port and registers it into the cluster mesh.
func (g *GossipEngine) ConnectPeer(addr string) error {
	addr = strings.TrimSpace(addr)
	if !strings.Contains(addr, ":") {
		addr += ":8080"
	}

	endpoint := fmt.Sprintf("http://%s/api/status", addr)
	req, err := http.NewRequestWithContext(g.ctx, "GET", endpoint, nil)
	if err != nil {
		return err
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to contact peer at %s: %w", addr, err)
	}
	defer resp.Body.Close()

	var status struct {
		NodeID     string            `json:"node_id"`
		NodeName   string            `json:"node_name"`
		MerkleRoot string            `json:"merkle_root"`
		CASStats   storage.CASStats  `json:"cas_stats"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return fmt.Errorf("invalid response from peer %s: %w", addr, err)
	}

	if status.NodeID == g.nodeID {
		return fmt.Errorf("cannot connect to self")
	}

	peer := PeerNode{
		ID:          status.NodeID,
		Name:        status.NodeName,
		HTTPAddr:    addr,
		MerkleRoot:  status.MerkleRoot,
		TotalChunks: status.CASStats.TotalChunks,
		StoredBytes: status.CASStats.StoredBytes,
		Status:      StatusActive,
		LastSeen:    time.Now(),
	}
	g.routingTable.Upsert(peer)
	log.Printf("[GOSSIP] Connected directly to peer %s (%s)", peer.Name, peer.HTTPAddr)
	return nil
}

// Start initiates periodic gossip rounds and background stale peer sweeps.
func (g *GossipEngine) Start() {
	g.wg.Add(2)
	go g.gossipLoop()
	go g.pruneLoop()
	log.Printf("[GOSSIP] Epidemic routing engine initialized (Interval: %v, Fanout: %d).", g.interval, g.fanout)
}

func (g *GossipEngine) gossipLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.executeRound()
		}
	}
}

func (g *GossipEngine) pruneLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.routingTable.PruneStalePeers()
		}
	}
}

// executeRound selects a random subset of peers and exchanges state.
func (g *GossipEngine) executeRound() {
	peers := g.routingTable.GetPeers()
	if len(peers) == 0 {
		return
	}

	// Shuffle and pick up to 'fanout' peers
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	count := g.fanout
	if len(peers) < count {
		count = len(peers)
	}

	// Build local tree snapshot
	hashes, _ := g.casStore.ListHashes()
	localTree := entropy.NewMerkleTree(hashes)
	stats := g.casStore.Stats()

	msg := GossipMessage{
		SenderID:       g.nodeID,
		SenderName:     g.senderName,
		SenderHTTPAddr: g.senderHTTPAddr,
		MerkleRoot:     localTree.RootHash(),
		TotalChunks:    stats.TotalChunks,
		StoredBytes:    stats.StoredBytes,
		Timestamp:      time.Now().Unix(),
	}

	for i := 0; i < count; i++ {
		target := peers[i]
		go g.sendGossipToPeer(target, msg, localTree)
	}
}

func (g *GossipEngine) sendGossipToPeer(peer PeerNode, msg GossipMessage, localTree *entropy.MerkleTree) {
	if peer.HTTPAddr == "" {
		return
	}

	endpoint := fmt.Sprintf("http://%s/api/gossip", peer.HTTPAddr)
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(g.ctx, "POST", endpoint, bytes.NewBuffer(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	var remoteResp GossipMessage
	if err := json.NewDecoder(resp.Body).Decode(&remoteResp); err != nil {
		return
	}

	latency := time.Since(start).Milliseconds()

	// Update peer status in routing table
	peer.MerkleRoot = remoteResp.MerkleRoot
	peer.TotalChunks = remoteResp.TotalChunks
	peer.StoredBytes = remoteResp.StoredBytes
	peer.LatencyMs = latency
	g.routingTable.Upsert(peer)

	// Merkle Anti-Entropy Check:
	// If root hashes differ, trigger targeted synchronization!
	if remoteResp.MerkleRoot != "" && remoteResp.MerkleRoot != localTree.RootHash() {
		g.triggerAntiEntropySync(peer)
	}
}

// triggerAntiEntropySync performs cursor-based Merkle tree difference resolution.
func (g *GossipEngine) triggerAntiEntropySync(peer PeerNode) {
	g.routingTable.MarkStatus(peer.ID, StatusSyncing)

	// Emit sync_start event for cluster dashboard telemetry
	g.emit(ClusterEvent{
		Type:       "sync_start",
		SourceNode: g.nodeID,
		TargetNode: peer.ID,
		Timestamp:  time.Now(),
	})

	go func() {
		defer func() {
			time.Sleep(1200 * time.Millisecond) // Visual hold for smooth physics animation
			g.routingTable.MarkStatus(peer.ID, StatusActive)
			g.emit(ClusterEvent{
				Type:       "sync_complete",
				SourceNode: g.nodeID,
				TargetNode: peer.ID,
				Timestamp:  time.Now(),
			})
		}()

		// Query remote peer's chunk list
		reqURL := fmt.Sprintf("http://%s/api/chunks/list", peer.HTTPAddr)
		req, err := http.NewRequestWithContext(g.ctx, "GET", reqURL, nil)
		if err != nil {
			return
		}

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		var remoteHashes []string
		if err := json.NewDecoder(resp.Body).Decode(&remoteHashes); err != nil {
			return
		}

		// Reconcile trees
		localHashes, _ := g.casStore.ListHashes()
		localTree := entropy.NewMerkleTree(localHashes)
		remoteTree := entropy.NewMerkleTree(remoteHashes)

		result := entropy.Reconcile(localTree, remoteTree)
		log.Printf("[ANTI-ENTROPY] Reconciled with %s: %s", peer.Name, result.AntiEntropySummary())

		// Pull missing chunks from remote peer
		for _, hashToPull := range result.ChunksToPull {
			chunkURL := fmt.Sprintf("http://%s/api/chunks/%s", peer.HTTPAddr, hashToPull)
			cReq, err := http.NewRequestWithContext(g.ctx, "GET", chunkURL, nil)
			if err != nil {
				continue
			}

			cResp, err := g.httpClient.Do(cReq)
			if err != nil {
				continue
			}

			chunkBuf := new(bytes.Buffer)
			_, _ = chunkBuf.ReadFrom(cResp.Body)
			cResp.Body.Close()

			_, _ = g.casStore.Put(hashToPull, chunkBuf.Bytes())
		}
	}()
}

// ForceSync triggers a forced anti-entropy reconciliation with a designated node.
func (g *GossipEngine) ForceSync(targetID string) error {
	peer, exists := g.routingTable.GetPeer(targetID)
	if !exists {
		return fmt.Errorf("peer %s not found in routing table", targetID)
	}
	g.triggerAntiEntropySync(*peer)
	return nil
}

func (g *GossipEngine) emit(evt ClusterEvent) {
	if g.eventChan != nil {
		select {
		case g.eventChan <- evt:
		default:
		}
	}
}

// Stop cleanly terminates the gossip worker routines.
func (g *GossipEngine) Stop() {
	g.cancel()
	g.wg.Wait()
	log.Printf("[GOSSIP] Epidemic routing engine stopped.")
}
