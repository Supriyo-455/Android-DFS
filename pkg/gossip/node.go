package gossip

import (
	"time"
)

// NodeStatus defines the operational health of a peer.
type NodeStatus string

const (
	StatusActive  NodeStatus = "active"
	StatusSyncing NodeStatus = "syncing"
	StatusSuspect NodeStatus = "suspect"
	StatusDead    NodeStatus = "dead"
)

// PeerNode represents a known participant in the distributed cluster.
type PeerNode struct {
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	HTTPAddr       string     `json:"http_addr"`
	GossipPort     int        `json:"gossip_port"`
	Status         NodeStatus `json:"status"`
	MerkleRoot     string     `json:"merkle_root"`
	TotalChunks    int64      `json:"total_chunks"`
	StoredBytes    int64      `json:"stored_bytes"`
	IsAndroid      bool       `json:"is_android"`
	LastSeen       time.Time  `json:"last_seen"`
	LatencyMs      int64      `json:"latency_ms"`
	AntiEntropyCnt int64      `json:"anti_entropy_cnt"`
}

// ClusterEvent represents a state transition emitted to the web management dashboard via WebSockets.
type ClusterEvent struct {
	Type        string      `json:"type"` // "peer_joined", "peer_left", "sync_start", "sync_complete", "chunk_stored"
	SourceNode  string      `json:"source_node"`
	TargetNode  string      `json:"target_node,omitempty"`
	Data        interface{} `json:"data,omitempty"`
	Timestamp   time.Time   `json:"timestamp"`
}
