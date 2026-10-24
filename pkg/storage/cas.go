package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	ErrChunkNotFound   = errors.New("chunk not found in content-addressable storage")
	ErrCorruptChunk    = errors.New("chunk data corrupt: sha256 mismatch")
	ErrInvalidChunkKey = errors.New("invalid chunk key format")
)

// CASStats maintains cluster and node-level deduplication statistics.
type CASStats struct {
	TotalChunks    int64   `json:"total_chunks"`
	UniqueChunks   int64   `json:"unique_chunks"`
	IngestedBytes  int64   `json:"ingested_bytes"`
	StoredBytes    int64   `json:"stored_bytes"`
	SavedBytes     int64   `json:"saved_bytes"`
	DedupRatio     float64 `json:"dedup_ratio"`
}

// ContentAddressableStore manages immutable chunks partitioned by SHA-256 hash.
type ContentAddressableStore struct {
	baseDir       string
	chunksDir     string
	mu            sync.RWMutex
	statsMu       sync.RWMutex
	totalChunks   atomic.Int64
	uniqueChunks  atomic.Int64
	ingestedBytes atomic.Int64
	storedBytes   atomic.Int64
}

// NewCAS initializes or loads a Content-Addressable Storage repository.
func NewCAS(baseDir string) (*ContentAddressableStore, error) {
	chunksDir := filepath.Join(baseDir, "chunks")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to initialize CAS chunks directory: %w", err)
	}

	cas := &ContentAddressableStore{
		baseDir:   baseDir,
		chunksDir: chunksDir,
	}

	// Scan existing chunks to populate accurate cold-start metrics
	cas.scanExisting()
	return cas, nil
}

// ShardedPath computes a 2-level nested path for the chunk hash (e.g. chunks/ab/cd/abcdef123...)
func (c *ContentAddressableStore) ShardedPath(hash string) (string, error) {
	if len(hash) < 4 {
		return "", ErrInvalidChunkKey
	}
	dir := filepath.Join(c.chunksDir, hash[0:2], hash[2:4])
	return filepath.Join(dir, hash), nil
}

// Exists checks whether a chunk is already persisted in CAS without reading its payload.
func (c *ContentAddressableStore) Exists(hash string) bool {
	path, err := c.ShardedPath(hash)
	if err != nil {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// Put persists a chunk into the CAS store. If the chunk already exists, it updates deduplication stats.
func (c *ContentAddressableStore) Put(hash string, data []byte) (bool, error) {
	c.ingestedBytes.Add(int64(len(data)))
	c.totalChunks.Add(1)

	path, err := c.ShardedPath(hash)
	if err != nil {
		return false, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Deduplication check: if file already exists, we skip disk write!
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return false, nil // Deduplicated hit (already present)
	}

	// Verify cryptographic hash before storing
	computed := sha256.Sum256(data)
	computedHex := hex.EncodeToString(computed[:])
	if computedHex != hash {
		return false, fmt.Errorf("%w: expected %s, got %s", ErrCorruptChunk, hash, computedHex)
	}

	// Ensure parent directory exists
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0755); err != nil {
		return false, err
	}

	// Write atomically using temporary file
	tmpPath := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return false, err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return false, err
	}

	c.uniqueChunks.Add(1)
	c.storedBytes.Add(int64(len(data)))
	return true, nil // New unique chunk written
}

// Get retrieves chunk data by SHA-256 hash.
func (c *ContentAddressableStore) Get(hash string) ([]byte, error) {
	path, err := c.ShardedPath(hash)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrChunkNotFound
		}
		return nil, err
	}

	return data, nil
}

// OpenChunk returns an open read-stream for a chunk.
func (c *ContentAddressableStore) OpenChunk(hash string) (io.ReadCloser, error) {
	path, err := c.ShardedPath(hash)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// ListHashes returns all chunk SHA-256 hashes present in the CAS.
func (c *ContentAddressableStore) ListHashes() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var hashes []string
	err := filepath.Walk(c.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		// Valid SHA-256 hash files have 64 hex characters
		base := filepath.Base(path)
		if len(base) == 64 {
			hashes = append(hashes, base)
		}
		return nil
	})
	return hashes, err
}

// Stats returns instantaneous deduplication and storage metrics.
func (c *ContentAddressableStore) Stats() CASStats {
	total := c.totalChunks.Load()
	unique := c.uniqueChunks.Load()
	ingested := c.ingestedBytes.Load()
	stored := c.storedBytes.Load()
	saved := ingested - stored
	if saved < 0 {
		saved = 0
	}

	var ratio float64 = 1.0
	if stored > 0 {
		ratio = float64(ingested) / float64(stored)
	}

	return CASStats{
		TotalChunks:   total,
		UniqueChunks:  unique,
		IngestedBytes: ingested,
		StoredBytes:   stored,
		SavedBytes:    saved,
		DedupRatio:    ratio,
	}
}

func (c *ContentAddressableStore) scanExisting() {
	var count int64
	var size int64

	_ = filepath.Walk(c.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(filepath.Base(path)) == 64 {
			count++
			size += info.Size()
		}
		return nil
	})

	c.uniqueChunks.Store(count)
	c.totalChunks.Store(count)
	c.storedBytes.Store(size)
	c.ingestedBytes.Store(size)
}
