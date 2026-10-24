package gossip

import (
	"sync"
	"time"
)

// RoutingTable maintains the active catalog of known peers in the cluster.
type RoutingTable struct {
	mu           sync.RWMutex
	localID      string
	peers        map[string]*PeerNode
	deadTimeout  time.Duration
	eventChannel chan ClusterEvent
}

// NewRoutingTable initializes an empty cluster routing table.
func NewRoutingTable(localID string, deadTimeout time.Duration, events chan ClusterEvent) *RoutingTable {
	if deadTimeout <= 0 {
		deadTimeout = 15 * time.Second
	}
	return &RoutingTable{
		localID:      localID,
		peers:        make(map[string]*PeerNode),
		deadTimeout:  deadTimeout,
		eventChannel: events,
	}
}

// Upsert updates or registers a peer from discovery announcements or gossip exchanges.
func (r *RoutingTable) Upsert(node PeerNode) {
	if node.ID == r.localID || node.ID == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existing, exists := r.peers[node.ID]
	node.LastSeen = time.Now()

	if !exists {
		node.Status = StatusActive
		r.peers[node.ID] = &node
		r.emit(ClusterEvent{
			Type:       "peer_joined",
			SourceNode: node.ID,
			Data:       node,
			Timestamp:  time.Now(),
		})
		return
	}

	// Update existing record
	existing.Name = node.Name
	existing.HTTPAddr = node.HTTPAddr
	existing.GossipPort = node.GossipPort
	existing.MerkleRoot = node.MerkleRoot
	existing.TotalChunks = node.TotalChunks
	existing.StoredBytes = node.StoredBytes
	existing.IsAndroid = node.IsAndroid
	existing.LastSeen = time.Now()
	if existing.Status == StatusDead || existing.Status == StatusSuspect {
		existing.Status = StatusActive
	}
}

// RemovePeer drops a node from the routing table (e.g. graceful departure).
func (r *RoutingTable) RemovePeer(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.peers[nodeID]; exists {
		delete(r.peers, nodeID)
		r.emit(ClusterEvent{
			Type:       "peer_left",
			SourceNode: nodeID,
			Timestamp:  time.Now(),
		})
	}
}

// MarkStatus updates the status flag of a peer (e.g. StatusSyncing).
func (r *RoutingTable) MarkStatus(nodeID string, status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if peer, exists := r.peers[nodeID]; exists {
		peer.Status = status
		if status == StatusSyncing {
			peer.AntiEntropyCnt++
		}
	}
}

// GetPeers returns a snapshot copy of all known peers.
func (r *RoutingTable) GetPeers() []PeerNode {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]PeerNode, 0, len(r.peers))
	for _, p := range r.peers {
		result = append(result, *p)
	}
	return result
}

// GetPeer retrieves an individual peer by ID.
func (r *RoutingTable) GetPeer(nodeID string) (*PeerNode, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	peer, exists := r.peers[nodeID]
	if !exists {
		return nil, false
	}
	copy := *peer
	return &copy, true
}

// PruneStalePeers sweeps dead mobile edge nodes to keep routing overhead light.
func (r *RoutingTable) PruneStalePeers() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for id, peer := range r.peers {
		elapsed := now.Sub(peer.LastSeen)
		if elapsed > r.deadTimeout*2 {
			delete(r.peers, id)
			r.emit(ClusterEvent{
				Type:       "peer_dead",
				SourceNode: id,
				Timestamp:  now,
			})
		} else if elapsed > r.deadTimeout {
			peer.Status = StatusSuspect
		}
	}
}

func (r *RoutingTable) emit(evt ClusterEvent) {
	if r.eventChannel != nil {
		select {
		case r.eventChannel <- evt:
		default:
			// Non-blocking drop if channel full
		}
	}
}
