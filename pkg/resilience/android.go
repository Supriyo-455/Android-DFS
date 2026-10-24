package resilience

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// AndroidInfo encapsulates the diagnostic state of the host Android environment.
type AndroidInfo struct {
	IsAndroid            bool   `json:"is_android"`
	IsTermux             bool   `json:"is_termux"`
	AndroidVersion       int    `json:"android_version"`
	SDKVersion           int    `json:"sdk_version"`
	PhantomKillerActive  bool   `json:"phantom_killer_active"`
	ChildProcessLimit    int    `json:"child_process_limit"`
	WakeLockActive       bool   `json:"wakelock_active"`
	BatteryExempted      bool   `json:"battery_exempted"`
	FilesystemType       string `json:"filesystem_type"`
}

// InspectEnvironment performs a non-invasive diagnostic check on the Android operating system.
func InspectEnvironment() AndroidInfo {
	info := AndroidInfo{
		IsAndroid:         runtime.GOOS == "android" || os.Getenv("TERMUX_VERSION") != "" || fileExists("/data/data/com.termux"),
		IsTermux:          os.Getenv("TERMUX_VERSION") != "" || fileExists("/data/data/com.termux"),
		ChildProcessLimit: 32, // Default AOSP Android 12+ limit
	}

	if !info.IsAndroid {
		info.FilesystemType = runtime.GOOS
		return info
	}

	// Probe Android build version via getprop if available
	if out, err := exec.Command("getprop", "ro.build.version.release").Output(); err == nil {
		vStr := strings.TrimSpace(string(out))
		if v, err := strconv.Atoi(vStr); err == nil {
			info.AndroidVersion = v
		}
	}

	if out, err := exec.Command("getprop", "ro.build.version.sdk").Output(); err == nil {
		sdkStr := strings.TrimSpace(string(out))
		if sdk, err := strconv.Atoi(sdkStr); err == nil {
			info.SDKVersion = sdk
		}
	}

	// Android 12 is SDK 31, Android 13 is SDK 33, Android 14 is SDK 34, Android 15 is SDK 35
	if info.AndroidVersion >= 12 || info.SDKVersion >= 31 {
		info.PhantomKillerActive = true
	}

	// Check if filesystem is F2FS
	if out, err := exec.Command("df", "-T", "/data").Output(); err == nil {
		outStr := string(out)
		if strings.Contains(strings.ToLower(outStr), "f2fs") {
			info.FilesystemType = "f2fs"
		} else if strings.Contains(strings.ToLower(outStr), "ext4") {
			info.FilesystemType = "ext4"
		} else {
			info.FilesystemType = "android_storage"
		}
	} else {
		info.FilesystemType = "f2fs (recommended)"
	}

	return info
}

// LogResilienceBanner prints actionable advice for the system administrator running on Termux.
func LogResilienceBanner(info AndroidInfo) string {
	var sb strings.Builder
	sb.WriteString("\n====================================================================\n")
	sb.WriteString(" [RESILIENCE] ANDROID EXECUTION ENVIRONMENT & PHANTOM PROCESS AUDIT \n")
	sb.WriteString("====================================================================\n")

	if !info.IsAndroid {
		sb.WriteString(" Host OS: Standard Non-Android Platform (" + runtime.GOOS + " " + runtime.GOARCH + ").\n")
		sb.WriteString(" Phantom Process Killer: INACTIVE (No AOSP process limits enforced).\n")
		sb.WriteString("====================================================================\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf(" Detected Android OS: Version %d (API Level %d)\n", info.AndroidVersion, info.SDKVersion))
	sb.WriteString(fmt.Sprintf(" Termux Runtime Environment: %v\n", info.IsTermux))
	sb.WriteString(fmt.Sprintf(" Storage Filesystem Target: %s\n", info.FilesystemType))

	if info.PhantomKillerActive {
		sb.WriteString("\n [!] CRITICAL: Android 12+ Phantom Process Killer Detected!\n")
		sb.WriteString("     AOSP commits 15755084 & 5706277f enforce a 32-child-process limit.\n")
		sb.WriteString("     Mitigation Strategy:\n")
		if info.AndroidVersion >= 14 {
			sb.WriteString("     - Android 14/15: Enable 'Disable child process restrictions' in\n")
			sb.WriteString("       Settings -> Developer Options.\n")
		} else {
			sb.WriteString("     - Android 12/13: Run ADB command from host PC:\n")
			sb.WriteString("       adb shell /system/bin/device_config put activity_manager max_phantom_processes 2147483647\n")
			sb.WriteString("       adb shell /system/bin/device_config set_sync_disabled_for_tests persistent\n")
		}
		sb.WriteString("     - Battery Optimization: Exclude Termux in 'Battery -> Unrestricted'\n")
	} else {
		sb.WriteString(" Phantom Process Killer: Not present in Android <= 11.\n")
	}
	sb.WriteString("====================================================================\n\n")

	return sb.String()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
