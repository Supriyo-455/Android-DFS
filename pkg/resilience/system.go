package resilience

// SystemMetrics encapsulates hardware resource metrics of the Android device or host.
type SystemMetrics struct {
	CPUUsagePct       float64 `json:"cpu_usage_pct"`
	RAMTotalBytes     uint64  `json:"ram_total_bytes"`
	RAMUsedBytes      uint64  `json:"ram_used_bytes"`
	RAMFreeBytes      uint64  `json:"ram_free_bytes"`
	RAMUsagePct       float64 `json:"ram_usage_pct"`
	StorageTotalBytes uint64  `json:"storage_total_bytes"`
	StorageFreeBytes  uint64  `json:"storage_free_bytes"`
	StorageUsedBytes  uint64  `json:"storage_used_bytes"`
	StorageUsedPct    float64 `json:"storage_used_pct"`
	BatteryPct        int     `json:"battery_pct,omitempty"`
	BatteryStatus     string  `json:"battery_status,omitempty"`
}
