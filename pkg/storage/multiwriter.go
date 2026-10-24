package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"sync"
)

// StreamReplicator facilitates zero-buffer streaming from incoming network/upload readers
// directly to local storage and remote peer connections concurrently.
type StreamReplicator struct {
	bufferSize int
}

// NewStreamReplicator creates an allocator with an optimized buffer size aligned to 4KB NAND pages.
func NewStreamReplicator() *StreamReplicator {
	return &StreamReplicator{
		bufferSize: 64 * 1024, // 64KB chunk buffer (multiple of 4KB NAND page)
	}
}

// ReplicationResult encapsulates metadata gathered during zero-buffer replication.
type ReplicationResult struct {
	SHA256        string
	BytesWritten  int64
	ReplicasCount int
}

// ReplicateZeroBuffer streams data from an input reader directly to local disk and multiple network peer connections
// simultaneously using io.MultiWriter and an in-flight crypto/sha256 hasher.
// This guarantees that multi-megabyte payloads never reside completely in Android memory.
func (s *StreamReplicator) ReplicateZeroBuffer(
	src io.Reader,
	localDestPath string,
	peerConns []net.Conn,
) (*ReplicationResult, error) {
	// Prepare local disk destination file
	tmpPath := fmt.Sprintf("%s.stream.tmp", localDestPath)
	localFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local destination for zero-buffer stream: %w", err)
	}
	defer func() {
		_ = localFile.Close()
	}()

	hasher := sha256.New()

	// Assemble list of all writers: local file, hasher, and all active peer net.Conn sockets
	writers := make([]io.Writer, 0, 2+len(peerConns))
	writers = append(writers, localFile, hasher)

	for _, conn := range peerConns {
		writers = append(writers, conn)
	}

	// io.MultiWriter duplicates every single chunk of bytes to each writer concurrently
	multiWriter := io.MultiWriter(writers...)

	// Copy from src to multiWriter using reusable buffer
	buf := make([]byte, s.bufferSize)
	n, copyErr := io.CopyBuffer(multiWriter, src, buf)
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("zero-buffer stream replication error: %w", copyErr)
	}

	// Close local file before rename
	_ = localFile.Close()

	computedHash := hex.EncodeToString(hasher.Sum(nil))

	// Move temporary file to final destination
	if err := os.Rename(tmpPath, localDestPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("failed to commit replicated stream file: %w", err)
	}

	return &ReplicationResult{
		SHA256:        computedHash,
		BytesWritten:  n,
		ReplicasCount: len(peerConns),
	}, nil
}

// HashingWriter computes SHA-256 transparently as data is written through it.
type HashingWriter struct {
	w      io.Writer
	hasher hash.Hash
	mu     sync.Mutex
}

// NewHashingWriter creates a wrapper that hashes data on the fly.
func NewHashingWriter(w io.Writer) *HashingWriter {
	return &HashingWriter{
		w:      w,
		hasher: sha256.New(),
	}
}

func (hw *HashingWriter) Write(p []byte) (int, error) {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	hw.hasher.Write(p)
	return hw.w.Write(p)
}

func (hw *HashingWriter) HexHash() string {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hex.EncodeToString(hw.hasher.Sum(nil))
}
