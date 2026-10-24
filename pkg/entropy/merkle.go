package entropy

import (
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
)

// MerkleNode represents a single node (leaf or internal branch) in the Merkle Tree.
type MerkleNode struct {
	Hash     string        `json:"hash"`
	Level    int           `json:"level"`
	IsLeaf   bool          `json:"is_leaf"`
	KeyRange [2]string     `json:"key_range"` // Min and Max key in this subtree
	Left     *MerkleNode   `json:"-"`
	Right    *MerkleNode   `json:"-"`
	Children []*MerkleNode `json:"children,omitempty"`
}

// MerkleTree represents a hierarchical hash tree for anti-entropy synchronization.
type MerkleTree struct {
	mu       sync.RWMutex
	Root     *MerkleNode `json:"root"`
	LeafKeys []string    `json:"leaf_keys"`
	Count    int         `json:"count"`
}

// NewMerkleTree builds a balanced Merkle Tree from a slice of chunk keys (hashes).
func NewMerkleTree(keys []string) *MerkleTree {
	// Deduplicate and sort keys lexicographically
	uniqueMap := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k != "" {
			uniqueMap[k] = struct{}{}
		}
	}

	sortedKeys := make([]string, 0, len(uniqueMap))
	for k := range uniqueMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	tree := &MerkleTree{
		LeafKeys: sortedKeys,
		Count:    len(sortedKeys),
	}

	if len(sortedKeys) == 0 {
		tree.Root = &MerkleNode{
			Hash:   "0000000000000000",
			Level:  0,
			IsLeaf: true,
		}
		return tree
	}

	// Create leaf nodes
	var currentLevel []*MerkleNode
	for _, key := range sortedKeys {
		// Murmur3 hash of the key
		mHash := Murmur3_64([]byte(key), 0x9747b28c)
		hashStr := fmt.Sprintf("%016x", mHash)

		node := &MerkleNode{
			Hash:     hashStr,
			Level:    0,
			IsLeaf:   true,
			KeyRange: [2]string{key, key},
		}
		currentLevel = append(currentLevel, node)
	}

	// Build tree bottom-up
	level := 1
	for len(currentLevel) > 1 {
		var nextLevel []*MerkleNode
		for i := 0; i < len(currentLevel); i += 2 {
			if i+1 < len(currentLevel) {
				left := currentLevel[i]
				right := currentLevel[i+1]
				combined := []byte(left.Hash + right.Hash)
				mHash := Murmur3_64(combined, 0x9747b28c)
				parent := &MerkleNode{
					Hash:     fmt.Sprintf("%016x", mHash),
					Level:    level,
					IsLeaf:   false,
					KeyRange: [2]string{left.KeyRange[0], right.KeyRange[1]},
					Left:     left,
					Right:    right,
					Children: []*MerkleNode{left, right},
				}
				nextLevel = append(nextLevel, parent)
			} else {
				// Odd node carried over
				nextLevel = append(nextLevel, currentLevel[i])
			}
		}
		currentLevel = nextLevel
		level++
	}

	tree.Root = currentLevel[0]
	return tree
}

// RootHash returns the top-level tree fingerprint.
func (t *MerkleTree) RootHash() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.Root == nil {
		return "0000000000000000"
	}
	return t.Root.Hash
}

// Cursor provides level-order and in-order traversal of Merkle nodes.
type Cursor struct {
	tree    *MerkleTree
	nodes   []*MerkleNode
	currIdx int
}

// NewCursor creates a cursor starting at the root.
func (t *MerkleTree) NewCursor() *Cursor {
	var nodes []*MerkleNode
	if t.Root != nil {
		// Breadth-first collection
		queue := []*MerkleNode{t.Root}
		for len(queue) > 0 {
			curr := queue[0]
			queue = queue[1:]
			nodes = append(nodes, curr)
			if curr.Left != nil {
				queue = append(queue, curr.Left)
			}
			if curr.Right != nil {
				queue = append(queue, curr.Right)
			}
		}
	}
	return &Cursor{
		tree:    t,
		nodes:   nodes,
		currIdx: 0,
	}
}

// First positions the cursor at the first node.
func (c *Cursor) First() *MerkleNode {
	if len(c.nodes) == 0 {
		return nil
	}
	c.currIdx = 0
	return c.nodes[0]
}

// Next advances the cursor and returns the next node.
func (c *Cursor) Next() *MerkleNode {
	c.currIdx++
	if c.currIdx >= len(c.nodes) {
		return nil
	}
	return c.nodes[c.currIdx]
}

// Current returns node at current position.
func (c *Cursor) Current() *MerkleNode {
	if c.currIdx >= len(c.nodes) {
		return nil
	}
	return c.nodes[c.currIdx]
}

// ChecksumBytes returns the root hash in binary representation.
func (t *MerkleTree) ChecksumBytes() []byte {
	root := t.RootHash()
	b, err := hex.DecodeString(root)
	if err != nil {
		return []byte(root)
	}
	return b
}
