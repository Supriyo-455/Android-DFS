//go:build !linux && !android && !windows

package resilience

import (
	"runtime"
)

// GetSystemMetrics fallback for other UNIX platforms.
func GetSystemMetrics(storagePath string) SystemMetrics {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return SystemMetrics{
		CPUUsagePct:       10.0,
		RAMTotalBytes:     1024 * 1024 * 1024 * 8,
		RAMUsedBytes:      m.Alloc,
		RAMFreeBytes:      (1024 * 1024 * 1024 * 8) - m.Alloc,
		RAMUsagePct:       float64(m.Alloc) / float64(1024*1024*1024*8) * 100.0,
		StorageTotalBytes: 1024 * 1024 * 1024 * 128,
		StorageFreeBytes:  1024 * 1024 * 1024 * 64,
		StorageUsedBytes:  1024 * 1024 * 1024 * 64,
		StorageUsedPct:    50.0,
	}
}
