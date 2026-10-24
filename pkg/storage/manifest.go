package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrFileNotFound = errors.New("file manifest not found")
)

// FileRecord represents an intact file assembled from FastCDC Content-Addressable chunks.
type FileRecord struct {
	ID          string    `json:"id"`
	Filename    string    `json:"filename"`
	Size        int64     `json:"size"`
	ChunkCount  int       `json:"chunk_count"`
	ChunkHashes []string  `json:"chunk_hashes"`
	UploadTime  time.Time `json:"upload_time"`
	ContentType string    `json:"content_type"`
}

// ManifestStore manages metadata for files uploaded into the distributed CAS store.
type ManifestStore struct {
	baseDir string
	metaDir string
	mu      sync.RWMutex
	files   map[string]FileRecord
}

// NewManifestStore creates or restores the file manifest repository.
func NewManifestStore(baseDir string) (*ManifestStore, error) {
	metaDir := filepath.Join(baseDir, "meta", "manifests")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create manifests dir: %w", err)
	}

	ms := &ManifestStore{
		baseDir: baseDir,
		metaDir: metaDir,
		files:   make(map[string]FileRecord),
	}

	ms.loadExisting()
	return ms, nil
}

func (m *ManifestStore) loadExisting() {
	_ = filepath.Walk(m.metaDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(path) == ".json" {
			data, err := os.ReadFile(path)
			if err == nil {
				var rec FileRecord
				if json.Unmarshal(data, &rec) == nil {
					m.files[rec.ID] = rec
					if rec.Filename != "" {
						m.files[rec.Filename] = rec
					}
				}
			}
		}
		return nil
	})
}

// RegisterFile stores a new file manifest mapping the file to its FastCDC chunk sequence.
func (m *ManifestStore) RegisterFile(rec FileRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rec.UploadTime.IsZero() {
		rec.UploadTime = time.Now()
	}
	rec.ChunkCount = len(rec.ChunkHashes)

	m.files[rec.ID] = rec
	if rec.Filename != "" {
		m.files[rec.Filename] = rec
	}

	// Persist to disk
	path := filepath.Join(m.metaDir, rec.ID+".json")
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// GetFile retrieves a file record by ID or filename.
func (m *ManifestStore) GetFile(identifier string) (FileRecord, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, exists := m.files[identifier]
	return rec, exists
}

// ListFiles returns all uploaded files sorted by newest first.
func (m *ManifestStore) ListFiles() []FileRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	unique := make(map[string]FileRecord)
	for _, rec := range m.files {
		unique[rec.ID] = rec
	}

	list := make([]FileRecord, 0, len(unique))
	for _, rec := range unique {
		list = append(list, rec)
	}
	return list
}

// ReassembleFile streams the original file by reading its constituent CAS chunks in sequential order.
func (m *ManifestStore) ReassembleFile(identifier string, cas *ContentAddressableStore, w io.Writer) error {
	rec, exists := m.GetFile(identifier)
	if !exists {
		return ErrFileNotFound
	}

	for _, hash := range rec.ChunkHashes {
		chunkData, err := cas.Get(hash)
		if err != nil {
			return fmt.Errorf("missing chunk %s during file reassembly: %w", hash, err)
		}

		if _, err := w.Write(chunkData); err != nil {
			return fmt.Errorf("error streaming reassembled file chunk: %w", err)
		}
	}

	return nil
}

// DeleteFile removes a file manifest and prunes associated CAS chunks that are no longer used by any other file.
func (m *ManifestStore) DeleteFile(identifier string, cas *ContentAddressableStore) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	rec, exists := m.files[identifier]
	if !exists {
		return ErrFileNotFound
	}

	// Delete from manifest map
	delete(m.files, rec.ID)
	if rec.Filename != "" && m.files[rec.Filename].ID == rec.ID {
		delete(m.files, rec.Filename)
	}

	// Delete manifest metadata file from disk
	manifestPath := filepath.Join(m.metaDir, rec.ID+".json")
	_ = os.Remove(manifestPath)

	// Collect remaining chunk hashes still in use by other files
	usedChunks := make(map[string]bool)
	for _, f := range m.files {
		for _, h := range f.ChunkHashes {
			usedChunks[h] = true
		}
	}

	// Delete unreferenced chunks from CAS store
	if cas != nil {
		for _, h := range rec.ChunkHashes {
			if !usedChunks[h] {
				_ = cas.Delete(h)
			}
		}
		cas.Rescan()
	}

	return nil
}
