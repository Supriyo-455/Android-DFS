#!/usr/bin/env bash
# ==============================================================================
# Cross-Compilation Pipeline for Android Distributed Storage Node
# Generates static Go binaries with embedded UI for Android and Linux
# ==============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"
mkdir -p "$BIN_DIR"

cd "$PROJECT_ROOT"

echo "=========================================================="
echo " Building Mobile Distributed Storage Node"
echo "=========================================================="

# 1. Android ARM64 (Primary modern Android target: Snapdragon, Dimensity, Tensor)
echo "--> Compiling for Android ARM64 (aarch64)..."
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
    -ldflags="-s -w -X main.version=1.0.0-mobilefs" \
    -trimpath \
    -o "$BIN_DIR/storage-node-android-arm64" \
    ./cmd/storage-node
echo "    Output: $BIN_DIR/storage-node-android-arm64"

# 2. Android ARMv7 (Legacy 32-bit devices)
echo "--> Compiling for Android ARMv7 (armhf)..."
CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o "$BIN_DIR/storage-node-android-armv7" \
    ./cmd/storage-node
echo "    Output: $BIN_DIR/storage-node-android-armv7"

# 3. Linux x86_64 (Edge servers, emulators, WSL2)
echo "--> Compiling for Linux AMD64..."
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o "$BIN_DIR/storage-node-linux-amd64" \
    ./cmd/storage-node
echo "    Output: $BIN_DIR/storage-node-linux-amd64"

# 4. Windows (Workstation development)
echo "--> Compiling for Windows AMD64..."
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o "$BIN_DIR/storage-node-windows.exe" \
    ./cmd/storage-node
echo "    Output: $BIN_DIR/storage-node-windows.exe"

echo "=========================================================="
echo " All target binaries compiled successfully into $BIN_DIR!"
echo "=========================================================="
