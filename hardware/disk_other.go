//go:build !linux

package hardware

// On macOS / Windows (dev-only) we always assume NVMe; /sys/block is unavailable.
func diskKind(_ string) DiskKind { return DiskKindNVMe }
