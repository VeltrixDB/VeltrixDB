//go:build !linux || !cgo

package storage

// cgoBatchEngine is a no-op stub compiled on macOS / Windows dev builds, or
// whenever CGO_ENABLED=0.  All method calls are safe with a nil receiver so
// StorageEngine can always hold a *cgoBatchEngine field without nil checks at
// every call site.

type cgoBatchEngine struct{}

func newCGOBatchEngine(_ int) *cgoBatchEngine { return nil }

func (e *cgoBatchEngine) close() {}

// batchPutViaCGO always returns 0, signalling the caller to fall back to the
// pure-Go MultiPut path.
func (e *cgoBatchEngine) batchPutViaCGO(_ []MultiPutRequest) int { return 0 }

func (e *cgoBatchEngine) PutsTotal() uint64 { return 0 }
func (e *cgoBatchEngine) ThreadCount() int  { return 0 }
