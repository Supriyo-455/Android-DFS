package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/android-dfs/storage-node/pkg/api"
	"github.com/android-dfs/storage-node/pkg/config"
	"github.com/android-dfs/storage-node/pkg/discovery"
	"github.com/android-dfs/storage-node/pkg/entropy"
	"github.com/android-dfs/storage-node/pkg/gossip"
	"github.com/android-dfs/storage-node/pkg/resilience"
	"github.com/android-dfs/storage-node/pkg/storage"
	"github.com/android-dfs/storage-node/pkg/ui"
)

const version = "1.0.0-mobilefs"

func main() {
	cfg := config.DefaultConfig()

	// Command line flag overrides
	flag.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "Unique cluster node ID")
	flag.StringVar(&cfg.NodeName, "name", cfg.NodeName, "Friendly node display name")
	flag.StringVar(&cfg.HTTPAddr, "http", cfg.HTTPAddr, "HTTP API & Web Dashboard listen address")
	flag.StringVar(&cfg.MulticastAddr, "multicast", cfg.MulticastAddr, "UDP Multicast discovery group address")
	flag.IntVar(&cfg.GossipPort, "gossip-port", cfg.GossipPort, "Port for P2P state exchange")
	flag.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "Directory path for CAS chunks and metadata")
	flag.BoolVar(&cfg.AndroidMode, "android", cfg.AndroidMode, "Force Android/Termux execution environment mode")
	flag.BoolVar(&cfg.WakeLockEnabled, "wakelock", cfg.WakeLockEnabled, "Acquire PARTIAL_WAKE_LOCK to prevent CPU sleep")
	var seedPeers string
	var overrideIP string
	flag.StringVar(&overrideIP, "ip", "", "Explicit IP address to advertise for this node (overrides auto-detection)")
	flag.StringVar(&seedPeers, "peer", "", "Optional peer address to connect to on startup (e.g. 192.168.0.110:8080)")
	flag.StringVar(&seedPeers, "peers", "", "Optional comma-separated peer addresses to connect to on startup")
	flag.IntVar(&cfg.CDCAvgSize, "cdc-avg-size", cfg.CDCAvgSize, "FastCDC target chunk size in bytes")
	flag.Parse()

	log.Printf("[INIT] Starting Android Distributed Storage System Daemon v%s", version)
	log.Printf("[INIT] Node ID: %s | Node Name: %s | HTTP: %s", cfg.NodeID, cfg.NodeName, cfg.HTTPAddr)

	// Step 1: Android Environment & Phantom Process Killer Diagnostics
	androidInfo := resilience.InspectEnvironment()
	fmt.Print(resilience.LogResilienceBanner(androidInfo))

	// Step 2: Acquire Partial Wake Lock if on Android
	wakeLock := resilience.NewWakeLockManager()
	if cfg.WakeLockEnabled && androidInfo.IsAndroid {
		if err := wakeLock.Acquire(); err != nil {
			log.Printf("[WARNING] Unable to acquire termux-wake-lock: %v", err)
		} else {
			androidInfo.WakeLockActive = true
		}
	}

	// Step 3: Initialize CAS (Content-Addressable Storage) and FastCDC
	if err := cfg.EnsureDirs(); err != nil {
		log.Fatalf("[FATAL] Failed to configure storage directories: %v", err)
	}

	casStore, err := storage.NewCAS(cfg.DataDir)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize CAS storage engine: %v", err)
	}

	manifestStore, err := storage.NewManifestStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize file manifest store: %v", err)
	}

	fastcdc := storage.NewFastCDC(cfg.CDCMinSize, cfg.CDCAvgSize, cfg.CDCMaxSize, cfg.CDCNormalization)
	f2fsOpt := storage.NewF2FSOptimizer(cfg.F2FSOptimized)

	// Initial Merkle tree calculation
	hashes, _ := casStore.ListHashes()
	initialTree := entropy.NewMerkleTree(hashes)
	log.Printf("[STORAGE] CAS initialized at %s (%d existing chunks). Root: %s", cfg.DataDir, len(hashes), initialTree.RootHash())

	// Start background hardware telemetry collector
	resilience.StartMetricsCollector(cfg.DataDir)

	// Step 4: Routing Table & Real-Time Event Hub
	eventChannel := make(chan gossip.ClusterEvent, 100)
	routingTable := gossip.NewRoutingTable(cfg.NodeID, 15*time.Second, eventChannel)
	hub := api.NewHub()
	go hub.Run()

	// Forward cluster events to WebSocket hub
	go func() {
		for evt := range eventChannel {
			hub.BroadcastEvent(evt)
		}
	}()

	// Step 5: Gossip Engine (Epidemic Routing & Anti-Entropy Reconciliation)
	portStr := cfg.HTTPAddr
	if strings.HasPrefix(portStr, ":") {
		portStr = portStr[1:]
	}
	localIP := getLocalIP(overrideIP)
	localAddr := fmt.Sprintf("%s:%s", localIP, portStr)

	gossipEngine := gossip.NewGossipEngine(cfg.NodeID, routingTable, casStore, cfg.GossipInterval, eventChannel)
	gossipEngine.SetLocalDetails(cfg.NodeName, localAddr)
	gossipEngine.Start()

	// If seed peers were provided, connect to them
	if seedPeers != "" {
		go func() {
			time.Sleep(1 * time.Second)
			for _, peerAddr := range strings.Split(seedPeers, ",") {
				peerAddr = strings.TrimSpace(peerAddr)
				if peerAddr != "" {
					if err := gossipEngine.ConnectPeer(peerAddr); err != nil {
						log.Printf("[PEER] Initial connection to %s: %v", peerAddr, err)
					}
				}
			}
		}()
	}

	// Step 6: HTTP API & Embedded Web Dashboard
	apiServer := api.NewServer(cfg, casStore, manifestStore, fastcdc, f2fsOpt, routingTable, gossipEngine, hub, androidInfo)
	apiServer.SetLocalAddr(localAddr)

	announcement := discovery.PeerAnnouncement{
		NodeID:      cfg.NodeID,
		NodeName:    cfg.NodeName,
		Role:        "slave",
		HTTPAddr:    localAddr,
		GossipPort:  cfg.GossipPort,
		MerkleRoot:  initialTree.RootHash(),
		TotalChunks: casStore.Stats().TotalChunks,
		StoredBytes: casStore.Stats().StoredBytes,
		IsAndroid:   androidInfo.IsAndroid,
		Timestamp:   time.Now(),
	}

	// Step 7: Zero-Configuration Autonomous Discovery (Multicast + Subnet Scan)
	// Master-Slave Architecture:
	// - Android phones run as storage slave workers.
	// - The desktop client acts as the central master orchestrator.
	handleDiscoveredPeer := func(p discovery.PeerAnnouncement) {
		if p.Leaving {
			routingTable.RemovePeer(p.NodeID)
			return
		}

		// Check if discovered node is the Desktop Master Controller
		if p.Role == "master" || p.NodeID == "desktop-controller" {
			log.Printf("[AUTODISCOVERY] Recognized Desktop Master Controller at %s", p.HTTPAddr)
			apiServer.SetMasterAddr(p.HTTPAddr)

			// Register this phone as an active storage slave node with the Desktop Master
			go func(masterAddr string) {
				regURL := fmt.Sprintf("http://%s/api/cluster/phones/add?addr=%s", masterAddr, localAddr)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "POST", regURL, nil)
				if err == nil {
					resp, err := http.DefaultClient.Do(req)
					if err == nil {
						_ = resp.Body.Close()
						log.Printf("[FLEET] Registered storage slave node (%s) with Desktop Master (%s)", localAddr, masterAddr)
					}
				}
			}(p.HTTPAddr)

			// Desktop client is the master controller, NOT a storage slave peer!
			// Do NOT add to routingTable; do NOT connect gossipEngine.
			return
		}

		// Discovered a fellow phone storage slave:
		routingTable.Upsert(gossip.PeerNode{
			ID:          p.NodeID,
			Name:        p.NodeName,
			HTTPAddr:    p.HTTPAddr,
			GossipPort:  p.GossipPort,
			MerkleRoot:  p.MerkleRoot,
			TotalChunks: p.TotalChunks,
			StoredBytes: p.StoredBytes,
			IsAndroid:   p.IsAndroid,
			Status:      gossip.StatusActive,
			LastSeen:    time.Now(),
		})
	}

	multicast, err := discovery.NewMulticastDiscovery(cfg.MulticastAddr, announcement, handleDiscoveredPeer)
	if err != nil {
		log.Printf("[WARNING] UDP Multicast initialization warning: %v. Running in localized discovery mode.", err)
	} else {
		if err := multicast.Start(); err != nil {
			log.Printf("[WARNING] Multicast start failed: %v", err)
		}
	}

	// Autonomous Wi-Fi Subnet Scanner (discovers Desktop Master and peers without manual IP configuration)
	subnetScanner := discovery.NewSubnetScanner(cfg.NodeID, localIP, portStr, handleDiscoveredPeer)
	subnetScanner.Start()

	mux := http.NewServeMux()
	apiServer.RegisterRoutes(mux)

	// Mount embedded SPA with client-side fallback
	spaHandler, err := ui.NewSPAHandler()
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize embedded SPA handler: %v", err)
	}
	mux.Handle("/", spaHandler)

	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		port := cfg.HTTPAddr
		if strings.HasPrefix(port, ":") {
			port = port[1:]
		}
		log.Printf("[HTTP] Mobile Cluster Dashboard & REST API online:")
		log.Printf("       -> Phone Wi-Fi URL:      http://%s:%s/", localIP, port)
		log.Printf("       -> SSH Forwarded URL:    http://localhost:%s/ (if using ssh -L %s:localhost:%s)", port, port, port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	// Step 8: Graceful Shutdown Signal Handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("[SHUTDOWN] Received signal %v. Initiating graceful cluster departure...", sig)

	// Stop multicast & subnet scanner
	if multicast != nil {
		multicast.Stop()
	}
	subnetScanner.Stop()

	// Stop gossip
	gossipEngine.Stop()

	// Shutdown HTTP
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)

	// Release Android Wake Lock
	if wakeLock.IsAcquired() {
		_ = wakeLock.Release()
	}

	log.Printf("[SHUTDOWN] Node terminated cleanly.")
}

func getLocalIP(override string) string {
	if override != "" {
		return strings.TrimSpace(override)
	}

	// Technique 1: Kernel routing table query via UDP socket connect.
	// In the Linux/Android kernel, net.Dial on UDP does not transmit network packets,
	// but causes the kernel to populate the socket's source IP with the active Wi-Fi interface IP.
	// This works reliably on Android 14/15 where net.InterfaceAddrs() is blocked by SELinux.
	probeAddrs := []string{
		"8.8.8.8:80",
		"1.1.1.1:80",
		"224.0.0.1:80",
		"192.168.0.1:80",
		"192.168.1.1:80",
		"10.0.0.1:80",
	}
	for _, target := range probeAddrs {
		conn, err := net.DialTimeout("udp", target, 400*time.Millisecond)
		if err == nil {
			localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
			_ = conn.Close()
			if ok && localAddr.IP != nil && !localAddr.IP.IsLoopback() && localAddr.IP.To4() != nil {
				ipStr := localAddr.IP.String()
				if !strings.HasPrefix(ipStr, "127.") {
					return ipStr
				}
			}
		}
	}

	// Technique 2: "ip route get" shell command in Termux
	if out, err := exec.Command("ip", "route", "get", "1.1.1.1").Output(); err == nil {
		fields := strings.Fields(string(out))
		for i, f := range fields {
			if f == "src" && i+1 < len(fields) {
				candidate := fields[i+1]
				if ip := net.ParseIP(candidate); ip != nil && !ip.IsLoopback() && ip.To4() != nil {
					return candidate
				}
			}
		}
	}

	// Technique 3: Android DHCP properties via getprop
	if out, err := exec.Command("getprop", "dhcp.wlan0.ipaddress").Output(); err == nil {
		candidate := strings.TrimSpace(string(out))
		if ip := net.ParseIP(candidate); ip != nil && !ip.IsLoopback() && ip.To4() != nil {
			return candidate
		}
	}

	// Technique 4: Traditional interface inspection
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addrs {
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return ipnet.IP.String()
				}
			}
		}
	}

	return "127.0.0.1"
}
