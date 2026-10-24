#!/bin/bash
# ==============================================================================
# Android Phantom Process Killer Mitigation & Battery Optimization Overrides
# Targets: Android 12 (API 31), Android 13 (API 33), Android 14 (API 34), Android 15 (API 35)
# AOSP Commits: 15755084 & 5706277f
# ==============================================================================

echo "===================================================================="
echo " Android Distributed Storage System: OS Resilience Helper"
echo "===================================================================="

# Check if running on Android/Termux or host PC via ADB
if [ -z "$ANDROID_ROOT" ]; then
    CMD_PREFIX="adb shell"
    echo "[INFO] Running from PC host workstation via ADB."
else
    CMD_PREFIX=""
    echo "[INFO] Running directly on Android / Termux device."
fi

# 1. Android 12 & 13 Phantom Process Killer Limit Override
echo ""
echo "[1/4] Applying Android 12/13 max_phantom_processes bypass..."
$CMD_PREFIX /system/bin/device_config put activity_manager max_phantom_processes 2147483647
$CMD_PREFIX /system/bin/device_config set_sync_disabled_for_tests persistent

# Verify setting
echo "Verifying device_config activity_manager..."
$CMD_PREFIX /system/bin/device_config get activity_manager max_phantom_processes

# 2. Battery Optimization & Doze Exclusion
echo ""
echo "[2/4] Whitelisting Termux from Android Doze & App Standby Buckets..."
$CMD_PREFIX dumpsys deviceidle whitelist +com.termux

# 3. Android 14 & 15 Instructions
echo ""
echo "[3/4] Android 14 & 15 System Setting Note:"
echo "     On Android 14/15, navigate on the phone to:"
echo "     Settings -> System -> Developer Options -> 'Disable child process restrictions' (Toggle ON)."
echo "     This permanently overrides child process monitoring without requiring ADB re-runs."

# 4. Verify Active Partial Wake Locks
echo ""
echo "[4/4] Probing PowerManager Partial WakeLock status..."
$CMD_PREFIX dumpsys power | grep -i "PARTIAL_WAKE_LOCK" | head -n 10

echo ""
echo "===================================================================="
echo " Resilience configuration complete. Termux can now run uninterrupted."
echo "===================================================================="
