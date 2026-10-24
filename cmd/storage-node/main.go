package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	fastcdc := storage.NewFastCDC(cfg.CDCMinSize, cfg.CDCAvgSize, cfg.CDCMaxSize, cfg.CDCNormalization)
	f2fsOpt := storage.NewF2FSOptimizer(cfg.F2FSOptimized)

	// Initial Merkle tree calculation
	hashes, _ := casStore.ListHashes()
	initialTree := entropy.NewMerkleTree(hashes)
	log.Printf("[STORAGE] CAS initialized at %s (%d existing chunks). Root: %s", cfg.DataDir, len(hashes), initialTree.RootHash())

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
	gossipEngine := gossip.NewGossipEngine(cfg.NodeID, routingTable, casStore, cfg.GossipInterval, eventChannel)
	gossipEngine.Start()

	// Step 6: UDP Multicast Peer Discovery (with SO_REUSEADDR & SO_REUSEPORT)
	announcement := discovery.PeerAnnouncement{
		NodeID:      cfg.NodeID,
		NodeName:    cfg.NodeName,
		HTTPAddr:    cfg.HTTPAddr,
		GossipPort:  cfg.GossipPort,
		MerkleRoot:  initialTree.RootHash(),
		TotalChunks: casStore.Stats().TotalChunks,
		StoredBytes: casStore.Stats().StoredBytes,
		IsAndroid:   androidInfo.IsAndroid,
		Timestamp:   time.Now(),
	}

	multicast, err := discovery.NewMulticastDiscovery(cfg.MulticastAddr, announcement, func(p discovery.PeerAnnouncement) {
		if p.Leaving {
			routingTable.RemovePeer(p.NodeID)
		} else {
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
	})

	if err != nil {
		log.Printf("[WARNING] UDP Multicast initialization warning: %v. Running in localized discovery mode.", err)
	} else {
		if err := multicast.Start(); err != nil {
			log.Printf("[WARNING] Multicast start failed: %v", err)
		}
	}

	// Step 7: HTTP API & Embedded Web Dashboard
	apiServer := api.NewServer(cfg, casStore, fastcdc, f2fsOpt, routingTable, gossipEngine, hub, androidInfo)
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
		log.Printf("[HTTP] Mobile Cluster Dashboard & REST API online at http://%s/", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	// Step 8: Graceful Shutdown Signal Handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("[SHUTDOWN] Received signal %v. Initiating graceful cluster departure...", sig)

	// Stop multicast & leave cluster
	if multicast != nil {
		multicast.Stop()
	}

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
