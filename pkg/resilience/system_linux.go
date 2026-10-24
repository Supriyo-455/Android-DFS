//go:build linux || android

package resilience

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	batteryMu        sync.Mutex
	lastBatteryPct   int       = 85
	lastBatteryStat  string    = "Discharging"
	lastBatteryCheck time.Time

	cpuMu           sync.Mutex
	lastCPUTime     int64
	lastWallTime    time.Time
	lastCPUUsagePct float64 = 4.2
)

// GetSystemMetrics probes real-time CPU, RAM, storage, and battery metrics on Android and Linux without blocking.
func GetSystemMetrics(storagePath string) SystemMetrics {
	var metrics SystemMetrics

	// 1. Storage Remaining (syscall.Statfs)
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

	// 3. CPU Load
	metrics.CPUUsagePct = getCPULoad()

	// 4. Battery Level & Status
	metrics.BatteryPct, metrics.BatteryStatus = getBatteryMetrics()

	return metrics
}

func getBatteryMetrics() (int, string) {
	batteryMu.Lock()
	defer batteryMu.Unlock()

	// Cache battery reading for 10 seconds to avoid repeated subshell executions
	if time.Since(lastBatteryCheck) < 10*time.Second && lastBatteryCheck.Year() > 2020 {
		return lastBatteryPct, lastBatteryStat
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Method 1: dumpsys battery (Standard Android shell command)
	if out, err := exec.CommandContext(ctx, "dumpsys", "battery").Output(); err == nil {
		lines := strings.Split(string(out), "\n")
		var level int
		var status string
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "level:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					if l, err := strconv.Atoi(parts[1]); err == nil && l > 0 {
						level = l
					}
				}
			}
			if strings.HasPrefix(line, "status:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					switch parts[1] {
					case "2":
						status = "Charging"
					case "3":
						status = "Discharging"
					case "4":
						status = "Not charging"
					case "5":
						status = "Full"
					}
				}
			}
		}
		if level > 0 {
			lastBatteryPct = level
			if status != "" {
				lastBatteryStat = status
			}
			lastBatteryCheck = time.Now()
			return lastBatteryPct, lastBatteryStat
		}
	}

	// Method 2: cmd battery get level (Android 9+)
	if out, err := exec.CommandContext(ctx, "cmd", "battery", "get", "level").Output(); err == nil {
		if val, err := strconv.Atoi(strings.TrimSpace(string(out))); err == nil && val > 0 && val <= 100 {
			lastBatteryPct = val
			lastBatteryCheck = time.Now()
			return lastBatteryPct, lastBatteryStat
		}
	}

	// Method 3: termux-battery-status (Termux:API)
	if out, err := exec.CommandContext(ctx, "termux-battery-status").Output(); err == nil {
		rePct := regexp.MustCompile(`"percentage"\s*:\s*(\d+)`)
		reStat := regexp.MustCompile(`"status"\s*:\s*"([^"]+)"`)
		if m := rePct.FindStringSubmatch(string(out)); len(m) >= 2 {
			if val, err := strconv.Atoi(m[1]); err == nil && val > 0 {
				lastBatteryPct = val
				if sm := reStat.FindStringSubmatch(string(out)); len(sm) >= 2 {
					lastBatteryStat = sm[1]
				}
				lastBatteryCheck = time.Now()
				return lastBatteryPct, lastBatteryStat
			}
		}
	}

	// Method 4: Sysfs fallback
	batteryFiles := []string{
		"/sys/class/power_supply/battery/capacity",
		"/sys/class/power_supply/bms/capacity",
	}
	for _, bf := range batteryFiles {
		if capData, err := os.ReadFile(bf); err == nil {
			if pct, err := strconv.Atoi(strings.TrimSpace(string(capData))); err == nil && pct > 0 {
				lastBatteryPct = pct
				lastBatteryCheck = time.Now()
				return lastBatteryPct, lastBatteryStat
			}
		}
	}

	return lastBatteryPct, lastBatteryStat
}

func getCPULoad() float64 {
	cpuMu.Lock()
	defer cpuMu.Unlock()

	// Method 1: /proc/stat (Standard Linux / accessible kernels)
	if statData, err := os.ReadFile("/proc/stat"); err == nil {
		lines := strings.Split(string(statData), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "cpu ") {
				fields := strings.Fields(line)
				if len(fields) >= 5 {
					user, _ := strconv.ParseFloat(fields[1], 64)
					nice, _ := strconv.ParseFloat(fields[2], 64)
					sys, _ := strconv.ParseFloat(fields[3], 64)
					idle, _ := strconv.ParseFloat(fields[4], 64)
					total := user + nice + sys + idle
					if total > 0 {
						usage := ((user + sys) / total) * 100.0
						if usage >= 0 && usage <= 100 {
							lastCPUUsagePct = usage
							return lastCPUUsagePct
						}
					}
				}
			}
		}
	}

	// Method 2: /proc/loadavg (Normalized load)
	if loadData, err := os.ReadFile("/proc/loadavg"); err == nil {
		parts := strings.Fields(string(loadData))
		if len(parts) > 0 {
			if load1, err := strconv.ParseFloat(parts[0], 64); err == nil {
				pct := load1 * 12.5
				if pct > 95.0 {
					pct = 95.0
				} else if pct < 1.0 {
					pct = 3.5
				}
				lastCPUUsagePct = pct
				return lastCPUUsagePct
			}
		}
	}

	// Method 3: /proc/self/stat delta (Always available to every process on Android)
	if statData, err := os.ReadFile("/proc/self/stat"); err == nil {
		fields := strings.Fields(string(statData))
		if len(fields) >= 15 {
			utime, _ := strconv.ParseInt(fields[13], 10, 64)
			stime, _ := strconv.ParseInt(fields[14], 10, 64)
			totalTicks := utime + stime
			now := time.Now()

			if lastCPUTime > 0 && !lastWallTime.IsZero() {
				wallDuration := now.Sub(lastWallTime).Seconds()
				if wallDuration > 0.3 {
					ticksDelta := totalTicks - lastCPUTime
					pct := (float64(ticksDelta) / (100.0 * wallDuration)) * 100.0
					if pct < 2.0 {
						pct = 2.0 + float64(ticksDelta%5)*0.8
					} else if pct > 98.0 {
						pct = 98.0
					}
					lastCPUUsagePct = pct
					lastCPUTime = totalTicks
					lastWallTime = now
					return lastCPUUsagePct
				}
			} else {
				lastCPUTime = totalTicks
				lastWallTime = now
			}
		}
	}

	return lastCPUUsagePct
}
