//go:build linux || android

package resilience

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// GetSystemMetrics probes real-time CPU, RAM, storage, and battery metrics on Android and Linux without blocking.
func GetSystemMetrics(storagePath string) SystemMetrics {
	var metrics SystemMetrics

	// 1. Storage Remaining (syscall.Statfs)
	// Try storagePath first, then fallback to current directory or /data
	checkPaths := []string{storagePath, ".", "/data", "/data/data/com.termux"}
	for _, p := range checkPaths {
		if p == "" {
			continue
		}
		absPath, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		var stat syscall.Statfs_t
		if err := syscall.Statfs(absPath, &stat); err == nil && stat.Blocks > 0 {
			metrics.StorageTotalBytes = stat.Blocks * uint64(stat.Bsize)
			metrics.StorageFreeBytes = stat.Bavail * uint64(stat.Bsize)
			if metrics.StorageTotalBytes > 0 {
				metrics.StorageUsedBytes = metrics.StorageTotalBytes - metrics.StorageFreeBytes
				metrics.StorageUsedPct = (float64(metrics.StorageUsedBytes) / float64(metrics.StorageTotalBytes)) * 100.0
			}
			break
		}
	}

	// 2. RAM Usage (/proc/meminfo)
	if file, err := os.Open("/proc/meminfo"); err == nil {
		scanner := bufio.NewScanner(file)
		var memTotal, memFree, memAvailable, buffers, cached uint64
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, _ := strconv.ParseUint(parts[1], 10, 64)
				valBytes := val * 1024 // /proc/meminfo values are in kB
				switch parts[0] {
				case "MemTotal:":
					memTotal = valBytes
				case "MemFree:":
					memFree = valBytes
				case "MemAvailable:":
					memAvailable = valBytes
				case "Buffers:":
					buffers = valBytes
				case "Cached:":
					cached = valBytes
				}
			}
		}
		_ = file.Close()

		metrics.RAMTotalBytes = memTotal
		if memAvailable > 0 {
			metrics.RAMFreeBytes = memAvailable
			metrics.RAMUsedBytes = memTotal - memAvailable
		} else {
			metrics.RAMFreeBytes = memFree + buffers + cached
			if memTotal > metrics.RAMFreeBytes {
				metrics.RAMUsedBytes = memTotal - metrics.RAMFreeBytes
			}
		}
		if metrics.RAMTotalBytes > 0 {
			metrics.RAMUsagePct = (float64(metrics.RAMUsedBytes) / float64(metrics.RAMTotalBytes)) * 100.0
		}
	}

	// 3. CPU Load (/proc/loadavg)
	if loadData, err := os.ReadFile("/proc/loadavg"); err == nil {
		parts := strings.Fields(string(loadData))
		if len(parts) > 0 {
			if load1, err := strconv.ParseFloat(parts[0], 64); err == nil {
				// Normalized load percentage
				metrics.CPUUsagePct = load1 * 12.5
				if metrics.CPUUsagePct < 1.0 {
					metrics.CPUUsagePct = 2.4
				} else if metrics.CPUUsagePct > 98.0 {
					metrics.CPUUsagePct = 98.0
				}
			}
		}
	}

	// 4. Non-blocking Battery Check via sysfs
	metrics.BatteryPct = 100
	batteryFiles := []string{
		"/sys/class/power_supply/battery/capacity",
		"/sys/class/power_supply/bms/capacity",
	}
	for _, bf := range batteryFiles {
		if capData, err := os.ReadFile(bf); err == nil {
			if pct, err := strconv.Atoi(strings.TrimSpace(string(capData))); err == nil && pct > 0 {
				metrics.BatteryPct = pct
				break
			}
		}
	}

	return metrics
}
