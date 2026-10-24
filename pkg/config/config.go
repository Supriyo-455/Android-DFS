package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Config represents all operational parameters for the storage node daemon.
type Config struct {
	// Node identification
	NodeID       string `json:"node_id"`
	NodeName     string `json:"node_name"`
	ClusterName  string `json:"cluster_name"`

	// Networking
	HTTPAddr      string        `json:"http_addr"`       // e.g. "0.0.0.0:8080"
	MulticastAddr string        `json:"multicast_addr"`  // e.g. "239.0.0.1:40000"
	GossipPort    int           `json:"gossip_port"`     // TCP port for peer-to-peer sync
	GossipInterval time.Duration `json:"gossip_interval"`// Period between gossip rounds

	// Storage & Hardware Aware Optimizations
	DataDir         string `json:"data_dir"`           // Base directory for CAS chunks
	F2FSOptimized   bool   `json:"f2fs_optimized"`     // Enable F2FS segment & alignment optimizations
	SegmentBlockSize int   `json:"segment_block_size"` // Default 4096 bytes (4KB NAND page)
	SegmentSize     int    `json:"segment_size"`       // Default 2097152 bytes (2MB F2FS segment)

	// Content-Defined Chunking (FastCDC) Tuning
	CDCMinSize int `json:"cdc_min_size"` // Min chunk size (typically AvgSize / 4 = 8KB)
	CDCAvgSize int `json:"cdc_avg_size"` // Target chunk size (typically 32KB or 64KB)
	CDCMaxSize int `json:"cdc_max_size"` // Max chunk size (typically AvgSize * 4 = 128KB)
	CDCNormalization int `json:"cdc_normalization"` // FastCDC normalization level (1, 2 or 3; default 2)

	// Android & Process Resilience
	AndroidMode     bool   `json:"android_mode"`      // Auto-detected or forced Termux/Android mode
	WakeLockEnabled bool   `json:"wakelock_enabled"`  // Auto-acquire PARTIAL_WAKE_LOCK in Termux
	MaxPhantomOverride bool `json:"max_phantom_override"` // Flag for child process mitigations
}

// DefaultConfig returns an optimized configuration based on OS and architecture.
func DefaultConfig() *Config {
	isAndroid := runtime.GOOS == "android" || os.Getenv("TERMUX_VERSION") != "" || fileExists("/data/data/com.termux")

	// Determine sensible data dir based on environment
	defaultDataDir := "./storage_data"
	if isAndroid {
		// Use Termux internal storage path with F2FS partition if available
		defaultDataDir = "/data/data/com.termux/files/home/.distributed-storage/data"
	}

	randomID := generateRandomID(8)
	hostname, err := os.Hostname()
	if err != nil || hostname == "" || hostname == "localhost" {
		deviceModel := ""
		if isAndroid {
			if out, err := exec.Command("getprop", "ro.product.model").Output(); err == nil {
				deviceModel = strings.TrimSpace(string(out))
			}
			if deviceModel == "" {
				if out, err := exec.Command("getprop", "ro.product.marketname").Output(); err == nil {
					deviceModel = strings.TrimSpace(string(out))
				}
			}
		}
		if deviceModel != "" {
			hostname = fmt.Sprintf("%s (%s)", deviceModel, randomID[:4])
		} else if isAndroid {
			hostname = "Android-Phone-" + randomID[:4]
		} else {
			hostname = "node-" + randomID[:6]
		}
	}

	return &Config{
		NodeID:          "node-" + randomID,
		NodeName:        hostname,
		ClusterName:     "mobile-mesh-storage",
		HTTPAddr:        ":8080",
		MulticastAddr:   "239.0.0.1:40000",
		GossipPort:      9090,
		GossipInterval:  2 * time.Second,
		DataDir:         defaultDataDir,
		F2FSOptimized:   true,
		SegmentBlockSize: 4096,     // 4KB NAND page
		SegmentSize:     2 * 1024 * 1024, // 2MB F2FS Segment
		CDCMinSize:      8 * 1024,  // 8 KB
		CDCAvgSize:      32 * 1024, // 32 KB
		CDCMaxSize:      128 * 1024, // 128 KB
		CDCNormalization: 2,         // Level 2 normalization for balanced distribution
		AndroidMode:     isAndroid,
		WakeLockEnabled: isAndroid,
		MaxPhantomOverride: isAndroid,
	}
}

// EnsureDirs creates necessary directory trees for Content-Addressable Storage.
func (c *Config) EnsureDirs() error {
	chunksDir := filepath.Join(c.DataDir, "chunks")
	metaDir := filepath.Join(c.DataDir, "meta")
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return err
	}
	return os.MkdirAll(metaDir, 0755)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func generateRandomID(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}
