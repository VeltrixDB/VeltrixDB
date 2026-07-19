//go:build !(linux && cgo && go1.21)

package storage

// vlogFlushViaBridge is a no-op stub on non-Linux / non-CGO builds.
// Always returns false so VLogBatcher.Flush uses the WriteAt fallback.
func vlogFlushViaBridge(_ *VLogBatcher, _ int) bool { return false }
