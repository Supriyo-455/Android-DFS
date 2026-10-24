package resilience

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
)

// WakeLockManager handles acquiring and releasing Android PARTIAL_WAKE_LOCK via Termux utilities.
type WakeLockManager struct {
	mu       sync.Mutex
	acquired bool
}

// NewWakeLockManager creates an instance of WakeLockManager.
func NewWakeLockManager() *WakeLockManager {
	return &WakeLockManager{}
}

// Acquire requests a PARTIAL_WAKE_LOCK from Android OS via `termux-wake-lock`.
func (w *WakeLockManager) Acquire() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.acquired {
		return nil
	}

	// First verify if termux-wake-lock binary is available in PATH or Termux prefix
	path, err := exec.LookPath("termux-wake-lock")
	if err != nil {
		// Try explicit Termux path
		path = "/data/data/com.termux/files/usr/bin/termux-wake-lock"
	}

	cmd := exec.Command(path)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to acquire wake-lock via %s: %w", path, err)
	}

	w.acquired = true
	log.Printf("[WAKELOCK] Acquired PARTIAL_WAKE_LOCK to prevent CPU sleep during background replication.")
	return nil
}

// Release releases the wake lock via `termux-wake-unlock`.
func (w *WakeLockManager) Release() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.acquired {
		return nil
	}

	path, err := exec.LookPath("termux-wake-unlock")
	if err != nil {
		path = "/data/data/com.termux/files/usr/bin/termux-wake-unlock"
	}

	cmd := exec.Command(path)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to release wake-lock via %s: %w", path, err)
	}

	w.acquired = false
	log.Printf("[WAKELOCK] Released PARTIAL_WAKE_LOCK.")
	return nil
}

// IsAcquired returns current in-memory status.
func (w *WakeLockManager) IsAcquired() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acquired
}

// VerifyDumpsys queries `dumpsys power` if running with permissions (or root) to confirm wake lock status.
func (w *WakeLockManager) VerifyDumpsys() (bool, string) {
	out, err := exec.Command("dumpsys", "power").Output()
	if err != nil {
		return false, "dumpsys unavailable without elevated permissions"
	}
	outStr := string(out)
	if strings.Contains(outStr, "PARTIAL_WAKE_LOCK") && strings.Contains(outStr, "com.termux") {
		return true, "Verified: Termux PARTIAL_WAKE_LOCK active in kernel power manager."
	}
	return false, "No active Termux PARTIAL_WAKE_LOCK found in dumpsys."
}
