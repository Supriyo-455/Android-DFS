package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SubnetScanner performs fast, non-blocking zero-configuration autonomous sweeps
// across local Wi-Fi /24 subnets to discover storage nodes and desktop clients.
type SubnetScanner struct {
	selfID      string
	selfPort    string
	localIP     string
	portsToScan []string
	onPeerFound func(PeerAnnouncement)
	httpClient  *http.Client
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewSubnetScanner initializes the autonomous LAN scanner.
func NewSubnetScanner(
	selfID string,
	localIP string,
	selfPort string,
	onPeerFound func(PeerAnnouncement),
) *SubnetScanner {
	ctx, cancel := context.WithCancel(context.Background())

	cleanPort := strings.TrimPrefix(selfPort, ":")
	if cleanPort == "" {
		cleanPort = "8080"
	}

	ports := []string{"8080", "9000"}
	found := false
	for _, p := range ports {
		if p == cleanPort {
			found = true
			break
		}
	}
	if !found {
		ports = append(ports, cleanPort)
	}

	transport := &http.Transport{
		DisableKeepAlives: true,
		MaxIdleConns:      100,
	}

	return &SubnetScanner{
		selfID:      selfID,
		selfPort:    cleanPort,
		localIP:     localIP,
		portsToScan: ports,
		onPeerFound: onPeerFound,
		httpClient: &http.Client{
			Timeout:   1200 * time.Millisecond,
			Transport: transport,
		},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins periodic autonomous subnet sweeps.
func (s *SubnetScanner) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Initial immediate autonomous sweep (200ms after startup)
		time.Sleep(200 * time.Millisecond)
		s.ScanOnce()

		ticker := time.NewTicker(8 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.ScanOnce()
			}
		}
	}()

	log.Printf("[AUTODISCOVERY] Autonomous subnet engine active (scanning ports %v)", s.portsToScan)
}

// ScanOnce performs a fast parallel sweep across all 254 addresses in all detected local /24 subnets.
func (s *SubnetScanner) ScanOnce() {
	prefixes := getLocalSubnetPrefixes(s.localIP)
	if len(prefixes) == 0 {
		return
	}

	type target struct {
		ip   string
		port string
	}

	var targets []target
	for _, prefix := range prefixes {
		for i := 1; i <= 254; i++ {
			hostIP := fmt.Sprintf("%s%d", prefix, i)
			for _, port := range s.portsToScan {
				targets = append(targets, target{ip: hostIP, port: port})
			}
		}
	}

	jobs := make(chan target, len(targets))
	for _, t := range targets {
		jobs <- t
	}
	close(jobs)

	// Worker pool of 45 parallel goroutines
	workerCount := 45
	var scanWg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		scanWg.Add(1)
		go func() {
			defer scanWg.Done()
			for t := range jobs {
				select {
				case <-s.ctx.Done():
					return
				default:
					s.probeTarget(t.ip, t.port)
				}
			}
		}()
	}
	scanWg.Wait()
}

func (s *SubnetScanner) probeTarget(host, port string) {
	targetAddr := fmt.Sprintf("%s:%s", host, port)

	// Probe DFS status endpoint directly
	reqURL := fmt.Sprintf("http://%s/api/status", targetAddr)
	req, err := http.NewRequestWithContext(s.ctx, "GET", reqURL, nil)
	if err != nil {
		return
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return // Host offline or port closed
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var status struct {
		NodeID      string `json:"node_id"`
		NodeName    string `json:"node_name"`
		HTTPAddr    string `json:"http_addr"`
		MerkleRoot  string `json:"merkle_root"`
		AndroidInfo struct {
			IsAndroid bool `json:"is_android"`
		} `json:"android_info"`
		CASStats struct {
			TotalChunks int64 `json:"total_chunks"`
			StoredBytes int64 `json:"stored_bytes"`
		} `json:"cas_stats"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return
	}

	if status.NodeID == "" || status.NodeID == s.selfID {
		return // Ignore self
	}

	// ALWAYS use targetAddr: this is the verified reachable IP:Port that just answered!
	ann := PeerAnnouncement{
		NodeID:      status.NodeID,
		NodeName:    status.NodeName,
		HTTPAddr:    targetAddr,
		MerkleRoot:  status.MerkleRoot,
		TotalChunks: status.CASStats.TotalChunks,
		StoredBytes: status.CASStats.StoredBytes,
		IsAndroid:   status.AndroidInfo.IsAndroid,
		Timestamp:   time.Now(),
	}

	log.Printf("[AUTODISCOVERY] Autonomous sweep discovered %s at %s", status.NodeName, targetAddr)
	if s.onPeerFound != nil {
		s.onPeerFound(ann)
	}
}

// Stop cleanly terminates the scanner.
func (s *SubnetScanner) Stop() {
	s.cancel()
	s.wg.Wait()
}

func getLocalSubnetPrefixes(preferredIP string) []string {
	var prefixes []string
	seen := make(map[string]bool)

	addPrefix := func(ip net.IP) {
		if ip == nil || ip.IsLoopback() {
			return
		}
		v4 := ip.To4()
		if v4 == nil || v4.IsLoopback() {
			return
		}
		pfx := fmt.Sprintf("%d.%d.%d.", v4[0], v4[1], v4[2])
		if !seen[pfx] {
			seen[pfx] = true
			prefixes = append(prefixes, pfx)
		}
	}

	// 1. Check preferred IP if passed
	if preferredIP != "" {
		addPrefix(net.ParseIP(preferredIP))
	}

	// 2. Enumerate all active network interfaces
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				addPrefix(ipnet.IP)
			}
		}
	}

	// 3. UDP routing probes (queries kernel routing table without transmitting packets)
	probeTargets := []string{"8.8.8.8:80", "1.1.1.1:80", "192.168.0.1:80", "192.168.1.1:80"}
	for _, t := range probeTargets {
		conn, err := net.DialTimeout("udp", t, 300*time.Millisecond)
		if err == nil {
			if uAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok && uAddr.IP != nil {
				addPrefix(uAddr.IP)
			}
			_ = conn.Close()
		}
	}

	// 4. Default fallback: 192.168.0. and 192.168.1.
	if len(prefixes) == 0 {
		prefixes = append(prefixes, "192.168.0.", "192.168.1.")
	}

	return prefixes
}
