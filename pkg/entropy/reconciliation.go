package entropy

import (
	"fmt"
	"sort"
	"time"
)

// ReconciliationResult details the outcome of an anti-entropy exchange between two nodes.
type ReconciliationResult struct {
	Synchronized      bool      `json:"synchronized"`
	LocalRootHash     string    `json:"local_root_hash"`
	RemoteRootHash    string    `json:"remote_root_hash"`
	ChunksToPull      []string  `json:"chunks_to_pull"`      // Chunks present on remote, absent locally
	ChunksToPush      []string  `json:"chunks_to_push"`      // Chunks present locally, absent on remote
	SimilarityPct     float64   `json:"similarity_pct"`
	DurationMs        int64     `json:"duration_ms"`
	Timestamp         time.Time `json:"timestamp"`
}

// Reconcile compares a local Merkle tree with a remote Merkle tree.
// If root hashes match, zero network payload is transferred.
// Otherwise, it isolates exact missing chunk hashes using range difference calculations.
func Reconcile(localTree, remoteTree *MerkleTree) *ReconciliationResult {
	start := time.Now()

	res := &ReconciliationResult{
		LocalRootHash:  localTree.RootHash(),
		RemoteRootHash: remoteTree.RootHash(),
		Timestamp:      time.Now(),
	}

	// Step 1: Root Hash Comparison (Zero-Cost Exit)
	if localTree.RootHash() == remoteTree.RootHash() {
		res.Synchronized = true
		res.SimilarityPct = 100.0
		res.DurationMs = time.Since(start).Milliseconds()
		return res
	}

	// Step 2: Range Difference Isolation
	localSet := make(map[string]struct{}, len(localTree.LeafKeys))
	for _, k := range localTree.LeafKeys {
		localSet[k] = struct{}{}
	}

	remoteSet := make(map[string]struct{}, len(remoteTree.LeafKeys))
	for _, k := range remoteTree.LeafKeys {
		remoteSet[k] = struct{}{}
	}

	var toPull []string
	var toPush []string

	// Chunks on remote not found locally -> pull
	for k := range remoteSet {
		if _, exists := localSet[k]; !exists {
			toPull = append(toPull, k)
		}
	}

	// Chunks locally not found on remote -> push
	for k := range localSet {
		if _, exists := remoteSet[k]; !exists {
			toPush = append(toPush, k)
		}
	}

	sort.Strings(toPull)
	sort.Strings(toPush)

	res.ChunksToPull = toPull
	res.ChunksToPush = toPush
	res.Synchronized = len(toPull) == 0 && len(toPush) == 0

	totalUnique := len(localSet) + len(toPull)
	if totalUnique > 0 {
		commonCount := len(localSet) - len(toPush)
		res.SimilarityPct = (float64(commonCount) / float64(totalUnique)) * 100.0
	} else {
		res.SimilarityPct = 100.0
	}

	res.DurationMs = time.Since(start).Milliseconds()
	return res
}

// AntiEntropySummary produces a human-readable diagnostic message.
func (r *ReconciliationResult) AntiEntropySummary() string {
	if r.Synchronized {
		return fmt.Sprintf("[ANTI-ENTROPY] Perfect sync! Root: %s (Duration: %dms)", r.LocalRootHash, r.DurationMs)
	}
	return fmt.Sprintf(
		"[ANTI-ENTROPY] Desynchronization detected! Similarity: %.2f%% | Pull: %d chunks | Push: %d chunks | Resolving in %dms",
		r.SimilarityPct, len(r.ChunksToPull), len(r.ChunksToPush), r.DurationMs,
	)
}
