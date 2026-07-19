//go:build !linux

package hardware

// SysTune is a no-op on non-Linux platforms (macOS is dev-only).
func SysTune(_ *Profile) []error { return nil }
