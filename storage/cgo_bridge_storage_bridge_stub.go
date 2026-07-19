//go:build !(linux && cgo && go1.21)

package storage

// cgoStorageBridge stub for non-Linux / non-CGO builds.
// All methods are no-ops that return zero / nil so call sites compile
// unconditionally without build-tag guards at every call site.
type cgoStorageBridge struct{}

func newCGOStorageBridge(_ int, _ bool) *cgoStorageBridge { return nil }

func (b *cgoStorageBridge) close() {}

func (b *cgoStorageBridge) submitVLogBatch(_, _ int, _ []stagedRecord) int { return 0 }

func (b *cgoStorageBridge) bridgeStats() (submits, completions, errors uint64) { return }
