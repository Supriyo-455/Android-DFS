package main

import (
	"context"
	_ "embed"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/android-dfs/storage-node/pkg/discovery"
)

//go:embed ui/index.html
var uiHTML []byte

func main() {
	var port string
	var initialPhones string
	var multicastAddr string
	var dataDir string

	flag.StringVar(&port, "port", ":9000", "Desktop Web Dashboard listen port (e.g. :9000)")
	flag.StringVar(&initialPhones, "phones", "192.168.0.110:8080,192.168.0.105:8080", "Initial comma-separated phone addresses to manage")
	flag.StringVar(&multicastAddr, "multicast", "239.0.0.1:40000", "UDP Multicast group for auto-discovering phone nodes")
	flag.StringVar(&dataDir, "data-dir", "desktop_cloud_data", "Local directory for manifest catalog")
	flag.Parse()

	log.Println("=================================================================")
	log.Println("  MobileDFS | Personal Desktop Cloud Storage Controller          ")
	log.Println("=================================================================")

	// Initialize desktop server
	server, err := NewDesktopServer(dataDir)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize desktop cloud server: %v", err)
	}

	// Register initial phones from command-line arguments
	if initialPhones != "" {
		for _, addr := range strings.Split(initialPhones, ",") {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				server.AddPhone(addr)
			}
		}
	}

	// Start background poller to monitor phone telemetry, RAM, CPU, Battery, and Storage
	server.StartBackgroundPoller()

	// Zero-Configuration LAN Auto-Discovery via UDP Multicast
	announcement := discovery.PeerAnnouncement{
		NodeID:    "desktop-controller",
		NodeName:  "Desktop Client",
		HTTPAddr:  port,
		IsAndroid: false,
		Timestamp: time.Now(),
	}

	multicast, err := discovery.NewMulticastDiscovery(multicastAddr, announcement, func(p discovery.PeerAnnouncement) {
		if !p.Leaving && p.HTTPAddr != "" && p.NodeID != "desktop-controller" {
			server.AddPhone(p.HTTPAddr)
		}
	})
	if err == nil {
		if err := multicast.Start(); err != nil {
			log.Printf("[WARNING] UDP Multicast listener error: %v", err)
		} else {
			log.Printf("[DISCOVERY] Listening for phone broadcasts on %s", multicastAddr)
		}
	}

	// Set up HTTP mux
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	// Serve embedded desktop cloud portal dashboard
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && !strings.HasPrefix(r.URL.Path, "/api/") {
			// Fallback to SPA root
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(uiHTML)
			return
		}
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(uiHTML)
			return
		}
		http.NotFound(w, r)
	})

	listenAddr := port
	if !strings.Contains(listenAddr, ":") {
		listenAddr = ":" + listenAddr
	}

	srv := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	go func() {
		cleanPort := strings.TrimPrefix(listenAddr, ":")
		log.Printf("[PORTAL] Desktop Cloud Storage Portal online:")
		log.Printf("         -> Open in Browser: http://localhost:%s/", cleanPort)
		log.Printf("         -> Connected Phones: %s", initialPhones)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server failed: %v", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("[SHUTDOWN] Exiting desktop cloud controller...")
	if multicast != nil {
		multicast.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("[SHUTDOWN] Controller stopped cleanly.")
}
