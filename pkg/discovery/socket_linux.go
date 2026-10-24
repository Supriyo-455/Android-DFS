//go:build linux || android

package discovery

import (
	"syscall"
)

// SocketControl sets SO_REUSEADDR and SO_REUSEPORT on Linux/Android platforms.
// SO_REUSEADDR permits a socket to forcefully bind to a port currently in use.
// SO_REUSEPORT enables horizontal scaling across CPU cores within a single Android device.
func SocketControl(network, address string, c syscall.RawConn) error {
	var err error
	cErr := c.Control(func(fd uintptr) {
		// Set SO_REUSEADDR (0x2)
		if sErr := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); sErr != nil {
			err = sErr
			return
		}
		// Set SO_REUSEPORT (0xf on Linux/Android)
		const SO_REUSEPORT = 15
		if sErr := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, SO_REUSEPORT, 1); sErr != nil {
			err = sErr
			return
		}
	})
	if cErr != nil {
		return cErr
	}
	return err
}
