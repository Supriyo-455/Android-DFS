#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# Android Distributed Storage Node - Termux:Boot Auto-Start Script
# Target Path: ~/.termux/boot/boot_storage_node.sh
# Requires: Termux:Boot add-on (registers BOOT_COMPLETED broadcast)
# ==============================================================================

# Ensure Termux bin is in PATH
export PATH="/data/data/com.termux/files/usr/bin:$PATH"

# Log file destination
LOG_DIR="/data/data/com.termux/files/home/.distributed-storage"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/boot.log"

echo "[$(date)] Storage node boot script initialized." >> "$LOG_FILE"

# Step 1: Acquire Partial WakeLock (prevents Android CPU sleep when screen locks)
if command -v termux-wake-lock >/dev/null 2>&1; then
    termux-wake-lock
    echo "[$(date)] Acquired PARTIAL_WAKE_LOCK via termux-wake-lock." >> "$LOG_FILE"
else
    echo "[$(date)] WARNING: termux-wake-lock not found in PATH." >> "$LOG_FILE"
fi

# Step 2: Binary Path Resolution
NODE_BIN="/data/data/com.termux/files/home/storage-node"
if [ ! -f "$NODE_BIN" ]; then
    NODE_BIN="/data/data/com.termux/files/home/go/bin/storage-node"
fi

if [ ! -f "$NODE_BIN" ]; then
    echo "[$(date)] ERROR: storage-node binary not found! Please build or download it first." >> "$LOG_FILE"
    exit 1
fi

chmod +x "$NODE_BIN"

# Step 3: Launch daemon with F2FS data directory
DATA_DIR="$LOG_DIR/data"
mkdir -p "$DATA_DIR"

echo "[$(date)] Launching storage node daemon..." >> "$LOG_FILE"

# Execute in background, immune to hangups
nohup "$NODE_BIN" \
    -android=true \
    -wakelock=true \
    -http=":8080" \
    -data-dir="$DATA_DIR" \
    >> "$LOG_DIR/daemon.log" 2>&1 &

DAEMON_PID=$!
echo "[$(date)] Daemon running with PID $DAEMON_PID." >> "$LOG_FILE"

# Step 4: Optional Root OOM adjustment if device is rooted
if [ -w "/proc/$DAEMON_PID/oom_score_adj" ]; then
    echo -1000 > "/proc/$DAEMON_PID/oom_score_adj" 2>/dev/null && \
    echo "[$(date)] oom_score_adj set to -1000 (immune from Low Memory Killer)." >> "$LOG_FILE"
fi
