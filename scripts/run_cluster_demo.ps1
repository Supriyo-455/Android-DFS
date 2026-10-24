# ==============================================================================
# PowerShell Simulation Launcher: 3-Node Mobile Storage Cluster
# Launches 3 storage nodes on ports 8080, 8081, 8082 with independent data stores
# ==============================================================================

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " Launching 3-Node Mobile Distributed Storage Mesh" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$baseDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$root = Split-Path -Parent $baseDir

# Check if Go is installed
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Warning "Go binary not found in PATH! Please ensure Go 1.21+ is installed to launch native binaries."
    Write-Host "You can preview the dashboard UI directly by opening: file:///$root/pkg/ui/dist/index.html" -ForegroundColor Green
    Start-Process "$root/pkg/ui/dist/index.html"
    exit
}

# Build binary
Write-Host "Compiling storage-node binary..." -ForegroundColor Yellow
Set-Location $root
go build -o "$root\bin\storage-node.exe" ./cmd/storage-node

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed!"
    exit
}

Write-Host "Starting Node 1 (Host Node on :8080)..." -ForegroundColor Green
Start-Process "$root\bin\storage-node.exe" -ArgumentList "-http=:8080 -gossip-port=9090 -name=Host-Alpha -data-dir=$root\data\node1"

Start-Sleep -Seconds 2

Write-Host "Starting Node 2 (Edge Peer on :8081)..." -ForegroundColor Green
Start-Process "$root\bin\storage-node.exe" -ArgumentList "-http=:8081 -gossip-port=9091 -name=Edge-Bravo -data-dir=$root\data\node2"

Start-Sleep -Seconds 1

Write-Host "Starting Node 3 (Edge Peer on :8082)..." -ForegroundColor Green
Start-Process "$root\bin\storage-node.exe" -ArgumentList "-http=:8082 -gossip-port=9092 -name=Edge-Charlie -data-dir=$root\data\node3"

Write-Host ""
Write-Host "Cluster active!" -ForegroundColor Cyan
Write-Host "Opening Mobile Storage Dashboard at http://localhost:8080/ ..." -ForegroundColor Yellow
Start-Process "http://localhost:8080/"
