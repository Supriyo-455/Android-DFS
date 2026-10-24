package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

//go:embed all:dist
var DistFS embed.FS

// SPAFileSystem wraps an fs.FS to provide client-side routing fallback.
// When a file (such as /nodes/status or /topology) is not found,
// it gracefully serves index.html with HTTP 200 OK so the frontend SPA router can resolve the view.
type SPAFileSystem struct {
	root http.FileSystem
}

// NewSPAHandler returns an http.Handler serving the embedded SPA with client-side fallback.
func NewSPAHandler() (http.Handler, error) {
	sub, err := fs.Sub(DistFS, "dist")
	if err != nil {
		return nil, err
	}

	spaFS := &SPAFileSystem{
		root: http.FS(sub),
	}

	fileServer := http.FileServer(spaFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Prevent caching of index.html for instant updates during development
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		fileServer.ServeHTTP(w, r)
	}), nil
}

// Open intercepts missing files and falls back to index.html.
func (s *SPAFileSystem) Open(name string) (http.File, error) {
	f, err := s.root.Open(name)
	if err == nil {
		// Verify if it's a directory without index.html
		stat, sErr := f.Stat()
		if sErr == nil && stat.IsDir() {
			indexPath := filepath.Join(name, "index.html")
			if _, iErr := s.root.Open(indexPath); iErr != nil {
				// Directory has no index file, fallback to root index.html
				_ = f.Close()
				return s.root.Open("index.html")
			}
		}
		return f, nil
	}

	// If file does not exist, and is not an API call or asset with extension, fallback to index.html
	if os.IsNotExist(err) || strings.Contains(err.Error(), "file does not exist") {
		return s.root.Open("index.html")
	}

	return nil, err
}
