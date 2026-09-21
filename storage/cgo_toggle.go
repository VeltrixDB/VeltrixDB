package storage

import (
	"os"
	"strconv"
	"sync"
)

// CGOEngineDisabledEnv turns off the C++ acceleration layer at runtime.
//
// Set VELTRIXDB_DISABLE_CGO_ENGINE=1 to skip constructing the io_uring
// storage bridge and the C++ batch engine, leaving the pure-Go paths in
// place. Has no effect on a CGO_ENABLED=0 build, where both are already
// no-op stubs.
//
// # Why this exists
//
// Both were unconditional: with cgo compiled in, every NewStorageEngine
// created an io_uring bridge with SQPOLL enabled (a kernel thread per ring,
// busy-polling for sq_poll_idle_ms before sleeping), registered fixed
// buffers, and spawned runtime.NumCPU() C++ worker threads. There was no way
// to opt out short of rebuilding with CGO_ENABLED=0.
//
// That is fine for a long-lived production server on dedicated hardware. It
// is actively harmful anywhere that constructs many short-lived engines on a
// constrained box — a test suite, most obviously. CI hit exactly this: the
// -race job requires cgo, which silently switched the storage engine onto the
// C++ path, and dozens of SQPOLL rings on a 2-core runner starved the job
// into a 30-minute timeout (SIGTERM, exit 143).
//
// It is also the knob the docs used to claim existed. Earlier revisions told
// operators to pass --cgo-batch-engine / --sqpoll-reader; no such flags were
// ever defined. This is the honest version: one env var, off by default,
// documented to do exactly what it says.
const CGOEngineDisabledEnv = "VELTRIXDB_DISABLE_CGO_ENGINE"

var (
	cgoDisabledOnce sync.Once
	cgoDisabledVal  bool
)

// onceReset returns a fresh sync.Once. Test-only seam: cgoEngineDisabled
// memoises, and the tests need to re-evaluate the environment per case.
func onceReset() sync.Once { return sync.Once{} }

// cgoEngineDisabled reports whether the C++ layer should be skipped.
// Evaluated once — this is read on every engine construction and the
// environment does not change under a running process.
func cgoEngineDisabled() bool {
	cgoDisabledOnce.Do(func() {
		v := os.Getenv(CGOEngineDisabledEnv)
		if v == "" {
			return
		}
		// Accept 1/true/yes; anything else (including "0") leaves it enabled.
		if b, err := strconv.ParseBool(v); err == nil {
			cgoDisabledVal = b
			return
		}
		cgoDisabledVal = v == "yes" || v == "on"
	})
	return cgoDisabledVal
}
