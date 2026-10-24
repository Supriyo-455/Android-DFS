//go:build !linux && !android

package discovery

import (
	"syscall"
)

// SocketControl sets standard socket reuse options on non-Linux platforms (Windows/macOS).
func SocketControl(network, address string, c syscall.RawConn) error {
	var err error
	cErr := c.Control(func(fd uintptr) {
		// SO_REUSEADDR on Windows / Darwin
		if sErr := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); sErr != nil {
			err = sErr
			return
		}
	})
	if cErr != nil {
		return cErr
	}
	return err
}
