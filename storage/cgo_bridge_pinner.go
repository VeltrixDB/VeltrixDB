//go:build linux && cgo && go1.21

package storage

/*
#cgo linux CXXFLAGS: -std=c++17 -O3 -march=native -I${SRCDIR}/../cpp/include
#cgo linux LDFLAGS:  -lstdc++ -luring -lpthread
#include "../cpp/include/batch_engine.hpp"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"runtime"
	"sync/atomic"
	"unsafe"
)

// cgoBatchEngine wraps the C++ VeltrixBatchEngine.
// On Go 1.21+ builds (this file), batchPutViaCGO uses runtime.Pinner so that
// pinned Go backing-array pointers can be stored directly in C heap memory —
// no uintptr encoding required and the CGO pointer checker stays satisfied.
type cgoBatchEngine struct {
	handle  *C.VeltrixBatchEngine
	enabled atomic.Bool
}

func newCGOBatchEngine(numThreads int) *cgoBatchEngine {
	cfg := C.BatchEngineConfig{
		num_threads: C.int(numThreads),
		numa_aware:  C.bool(true),
		numa_node:   C.int(-1), // auto-detect from NVMe sysfs
	}
	h := C.veltrix_batch_engine_create_ex(cfg)
	if h == nil {
		return nil
	}
	e := &cgoBatchEngine{handle: h}
	e.enabled.Store(true)
	return e
}

func (e *cgoBatchEngine) close() {
	if e == nil || e.handle == nil {
		return
	}
	e.enabled.Store(false)
	C.veltrix_batch_engine_destroy(e.handle)
	e.handle = nil
}

// batchPutViaCGO passes reqs to the C++ engine using runtime.Pinner.
//
// Zero-copy pointer safety (Go 1.21+)
// ─────────────────────────────────────
// runtime.Pinner.Pin() tells the GC to not move the pinned object for the
// duration of the pin (until Unpin() is called via defer).  This satisfies
// the CGO rule added in Go 1.21:
//
//   "A Go pointer to a pinned Go variable may be stored in C memory."
//
// Therefore we can store &keyBytes[0] and &r.Value[0] — both in pinned
// Go heap memory — directly in the C heap BatchPutEntry.key.ptr / .value.ptr
// fields WITHOUT the uintptr encoding workaround needed in Go <1.21.
//
// The C++ engine reads the backing arrays read-only and synchronously;
// pinner.Unpin() is called (via defer) AFTER veltrix_batch_put returns,
// so the pin is held for the entire duration of the C++ call.
//
// Memory alignment: C.calloc returns 16-byte-aligned memory on glibc.
// BatchPutEntry has natural 8-byte alignment (largest field is size_t = 8B).
// The SliceView.ptr field within each entry is therefore always 8-byte
// aligned — no misaligned-access penalties.
func (e *cgoBatchEngine) batchPutViaCGO(reqs []MultiPutRequest) int {
	if len(reqs) == 0 || e == nil || !e.enabled.Load() {
		return 0
	}

	n := len(reqs)

	// C heap allocation: contains no Go pointers before we fill it, so the
	// CGO checker is satisfied.  We fill it with PINNED Go pointers below,
	// which is explicitly allowed in Go 1.21+ per the rule quoted above.
	entries := (*C.BatchPutEntry)(C.calloc(C.size_t(n), C.sizeof_BatchPutEntry))
	if entries == nil {
		return 0
	}
	defer C.free(unsafe.Pointer(entries))

	entSlice := unsafe.Slice(entries, n)

	// Pin all key and value backing arrays before storing their addresses in
	// C memory.  A single Pinner can pin an arbitrary number of objects.
	var pinner runtime.Pinner
	defer pinner.Unpin()

	for i, r := range reqs {
		// ── key ──────────────────────────────────────────────────────────
		// unsafe.StringData (Go 1.20+) returns a *byte to the string's
		// immutable backing array without any allocation.
		if len(r.Key) > 0 {
			keyPtr := unsafe.StringData(r.Key) // *byte, zero alloc
			pinner.Pin(keyPtr)                 // GC must not move this array
			entSlice[i].key.ptr = unsafe.Pointer(keyPtr)
			entSlice[i].key.len = C.size_t(len(r.Key))
		}

		// ── value ─────────────────────────────────────────────────────────
		if len(r.Value) > 0 {
			pinner.Pin(&r.Value[0])
			entSlice[i].value.ptr = unsafe.Pointer(&r.Value[0])
			entSlice[i].value.len = C.size_t(len(r.Value))
		}

		entSlice[i].ttl_seconds = C.int(r.TTL)
		entSlice[i].shard_hint = C.ushort(fnv64a(r.Key) & (numShards - 1))
	}

	// The Pinner keeps all backing arrays live and immovable until Unpin().
	// No KeepAlive needed — the Pinner itself is on the stack and anchors
	// all pinned objects transitively.
	result := C.veltrix_batch_put(e.handle, entries, C.size_t(n))
	return int(result)
}

func (e *cgoBatchEngine) PutsTotal() uint64 {
	if e == nil || e.handle == nil {
		return 0
	}
	return uint64(C.veltrix_batch_engine_puts_total(e.handle))
}

func (e *cgoBatchEngine) ThreadCount() int {
	if e == nil || e.handle == nil {
		return 0
	}
	return int(C.veltrix_batch_engine_thread_count(e.handle))
}
