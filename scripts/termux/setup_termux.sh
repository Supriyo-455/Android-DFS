#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# One-Click Termux Setup & Environment Bootstrap
# Run inside Termux: curl -sSL <url> | bash
# ==============================================================================

set -e

echo "=== Setting up Android Distributed Storage Node in Termux ==="

# Update package manager
pkg update -y && pkg upgrade -y

# Install required dependencies
pkg install -y termux-api golang git

# Request Battery Optimization Exemption
echo "Requesting battery optimization exclusion..."
am start -a android.settings.IGNORE_BATTERY_OPTIMIZATION_SETTINGS 2>/dev/null || true

# Setup Termux:Boot folder
BOOT_DIR="$HOME/.termux/boot"
mkdir -p "$BOOT_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/boot_storage_node.sh" ]; then
    cp "$SCRIPT_DIR/boot_storage_node.sh" "$BOOT_DIR/"
    chmod +x "$BOOT_DIR/boot_storage_node.sh"
    echo "Installed auto-boot script into $BOOT_DIR/boot_storage_node.sh"
fi

# Acquire initial wake lock
termux-wake-lock
echo "Acquired termux-wake-lock."

echo ""
echo "=== Setup complete! ==="
echo "Build the node with: go build -o ~/storage-node ./cmd/storage-node"
echo "Or start directly with: ~/storage-node -android=true -wakelock=true"
