/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: contains a method for opening a listener.
 */

package pkg

import (
	"net"
	"os"
	"strconv"
	"syscall"
)

// WinEADDRINUSE is errno to support Windows machines
const WinEADDRINUSE = syscall.Errno(10048)

// OpenListener listens on the specified port
func OpenListener(port string) (net.Listener, int, error) {
	conn, err := net.Listen("tcp", ":"+port)
	if err != nil {
		if portInUse(err) {
			Err.Printf("Port %v is already in use\n", port)
		}
		return nil, -1, err
	}

	_, port, err = net.SplitHostPort(conn.Addr().String())
	if err != nil {
		return nil, -1, err
	}

	portID, err := strconv.Atoi(port)
	if err != nil {
		return nil, -1, err
	}

	return conn, portID, err
}

// Checks if the given error is a port in use error
func portInUse(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if osErr, ok := opErr.Err.(*os.SyscallError); ok {
			return osErr.Err == syscall.EADDRINUSE || osErr.Err == WinEADDRINUSE
		}
	}
	return false
}
