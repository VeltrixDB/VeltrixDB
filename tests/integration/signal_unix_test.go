//go:build !windows

package integration_test

import "os"

// gracefulShutdownSignal returns SIGTERM on Unix systems.
func gracefulShutdownSignal() os.Signal {
	return os.Interrupt
}
