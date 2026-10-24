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
// across the local Wi-Fi /24 subnet to discover storage nodes and desktop clients.
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

	return &SubnetScanner{
		selfID:      selfID,
		selfPort:    cleanPort,
		localIP:     localIP,
		portsToScan: ports,
		onPeerFound: onPeerFound,
		httpClient:  &http.Client{Timeout: 800 * time.Millisecond},
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start begins periodic autonomous subnet sweeps.
func (s *SubnetScanner) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Initial sweep immediately after node boot
		time.Sleep(500 * time.Millisecond)
		s.ScanOnce()

		ticker := time.NewTicker(15 * time.Second)
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

	log.Printf("[AUTODISCOVERY] Autonomous subnet engine initialized for %s (ports %v)", s.localIP, s.portsToScan)
}

// ScanOnce performs a fast parallel sweep across all 254 addresses in the local /24 subnet.
func (s *SubnetScanner) ScanOnce() {
	ip := net.ParseIP(s.localIP)
	if ip == nil || ip.IsLoopback() {
		s.localIP = detectLocalIPv4()
		ip = net.ParseIP(s.localIP)
	}
	ipv4 := ip.To4()
	if ipv4 == nil || ipv4.IsLoopback() {
		return
	}

	prefix := fmt.Sprintf("%d.%d.%d.", ipv4[0], ipv4[1], ipv4[2])
	myLastByte := int(ipv4[3])

	type target struct {
		ip   string
		port string
	}

	jobs := make(chan target, 255*len(s.portsToScan))
	for i := 1; i <= 254; i++ {
		if i == myLastByte {
			continue // skip self
		}
		hostIP := fmt.Sprintf("%s%d", prefix, i)
		for _, port := range s.portsToScan {
			jobs <- target{ip: hostIP, port: port}
		}
	}
	close(jobs)

	// Worker pool of 35 parallel goroutines
	workerCount := 35
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

	// Quick TCP pre-check (250ms timeout)
	d := net.Dialer{Timeout: 250 * time.Millisecond}
	conn, err := d.DialContext(s.ctx, "tcp", targetAddr)
	if err != nil {
		return // Host offline or port closed
	}
	_ = conn.Close()

	// HTTP DFS status probe
	reqURL := fmt.Sprintf("http://%s/api/status", targetAddr)
	req, err := http.NewRequestWithContext(s.ctx, "GET", reqURL, nil)
	if err != nil {
		return
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return
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

	actualAddr := targetAddr
	if status.HTTPAddr != "" && !strings.HasPrefix(status.HTTPAddr, "127.") {
		actualAddr = status.HTTPAddr
	}

	ann := PeerAnnouncement{
		NodeID:      status.NodeID,
		NodeName:    status.NodeName,
		HTTPAddr:    actualAddr,
		MerkleRoot:  status.MerkleRoot,
		TotalChunks: status.CASStats.TotalChunks,
		StoredBytes: status.CASStats.StoredBytes,
		IsAndroid:   status.AndroidInfo.IsAndroid,
		Timestamp:   time.Now(),
	}

	log.Printf("[AUTODISCOVERY] Subnet scanner autonomously discovered %s at %s", status.NodeName, actualAddr)
	if s.onPeerFound != nil {
		s.onPeerFound(ann)
	}
}

// Stop cleanly terminates the scanner.
func (s *SubnetScanner) Stop() {
	s.cancel()
	s.wg.Wait()
}

func detectLocalIPv4() string {
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", 400*time.Millisecond)
	if err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr.IP != nil && !addr.IP.IsLoopback() {
			if v4 := addr.IP.To4(); v4 != nil {
				return v4.String()
			}
		}
	}
	return "127.0.0.1"
}
