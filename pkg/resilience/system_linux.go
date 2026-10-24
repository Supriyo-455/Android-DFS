//go:build linux || android

package resilience

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// GetSystemMetrics probes real-time CPU, RAM, storage, and battery metrics on Android and Linux.
func GetSystemMetrics(storagePath string) SystemMetrics {
	var metrics SystemMetrics

	// 1. Storage Remaining (syscall.Statfs)
	absPath, err := filepath.Abs(storagePath)
	if err == nil {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(absPath, &stat); err == nil {
			metrics.StorageTotalBytes = stat.Blocks * uint64(stat.Bsize)
			metrics.StorageFreeBytes = stat.Bavail * uint64(stat.Bsize)
			if metrics.StorageTotalBytes > 0 {
				metrics.StorageUsedBytes = metrics.StorageTotalBytes - metrics.StorageFreeBytes
				metrics.StorageUsedPct = (float64(metrics.StorageUsedBytes) / float64(metrics.StorageTotalBytes)) * 100.0
			}
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
			metrics.RAMUsedBytes = memTotal - metrics.RAMFreeBytes
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
				// Approximation of load percentage based on cores
				metrics.CPUUsagePct = load1 * 12.5 // Normalized for multi-core Android SoCs
				if metrics.CPUUsagePct > 100.0 {
					metrics.CPUUsagePct = 99.0
				}
			}
		}
	}

	// 4. Battery Level on Android
	if capData, err := os.ReadFile("/sys/class/power_supply/battery/capacity"); err == nil {
		if pct, err := strconv.Atoi(strings.TrimSpace(string(capData))); err == nil {
			metrics.BatteryPct = pct
		}
	} else if out, err := exec.Command("termux-battery-status").Output(); err == nil {
		// Parse termux-battery-status JSON if available
		outStr := string(out)
		if idx := strings.Index(outStr, `"percentage":`); idx != -1 {
			sub := outStr[idx+13:]
			if end := strings.IndexAny(sub, ",}\n"); end != -1 {
				if pct, err := strconv.Atoi(strings.TrimSpace(sub[:end])); err == nil {
					metrics.BatteryPct = pct
				}
			}
		}
	}

	return metrics
}
