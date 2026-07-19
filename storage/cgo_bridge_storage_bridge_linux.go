//go:build linux && cgo && go1.21

package storage

/*
// CGO preamble is compiled by cc (C compiler), not g++.  We must only include
// pure-C headers here.  storage_bridge_capi.h exposes the same C API as the
// extern "C" block in storage_bridge.hpp without pulling in any C++ headers.
#cgo linux CFLAGS:   -I${SRCDIR}/../cpp/include
#cgo linux LDFLAGS:  -lstdc++ -luring -lpthread
#include "../cpp/include/storage_bridge_capi.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// cgoStorageBridge wraps the C++ VeltrixStorageBridge.
// One bridge is created per storage engine; it owns 8 io_uring rings (one per
// NVMe disk) with SQPOLL enabled.  All VLog batch writes on Linux go through
// this bridge instead of N separate pwrite syscalls.
type cgoStorageBridge struct {
	handle *C.VeltrixStorageBridge
}

// newCGOStorageBridge creates and initialises the io_uring bridge.
// numDisks must match the number of VLog files (1–8).
// sqPoll enables IORING_SETUP_SQPOLL (kernel polls the SQ without syscalls).
// Returns nil on failure (old kernel, missing CAP_SYS_ADMIN, no liburing).
func newCGOStorageBridge(numDisks int, sqPoll bool) *cgoStorageBridge {
	if numDisks < 1 || numDisks > 8 {
		numDisks = 8
	}

	cfg := C.BridgeConfig{
		num_disks:         C.int(numDisks),
		ring_depth:        C.int(1024),
		sq_poll_idle_ms:   C.uint(2000),
		fixed_buf_size:    C.uint64_t(4096),
		fixed_bufs_per_ring: C.uint64_t(256),
	}
	if sqPoll {
		cfg.sq_poll = C.int(1)
	}
	// -1 = let kernel choose the SQ thread's CPU (no affinity pinning).
	for i := 0; i < 8; i++ {
		cfg.sq_thread_cpu[i] = C.int(-1)
	}

	h := C.veltrix_bridge_create(cfg)
	if h == nil {
		return nil
	}
	return &cgoStorageBridge{handle: h}
}

func (b *cgoStorageBridge) close() {
	if b != nil && b.handle != nil {
		C.veltrix_bridge_destroy(b.handle)
		b.handle = nil
	}
}

// submitVLogBatch routes a VLogBatcher's staged records through the io_uring
// bridge.  This replaces N sequential pwrite syscalls with a single
// io_uring_submit call; all SQEs are linked with IOSQE_IO_LINK in the C++
// layer so NVMe write ordering is preserved.
//
// Memory contract (Go 1.21+ Pinner):
//
//	Each rec.buf is an ioPool-allocated, sector-aligned Go slice.  We pin
//	the backing array with runtime.Pinner before storing its address in C
//	heap memory (BridgeWriteRequest.data).  The pinner is held until
//	veltrix_bridge_submit_batch returns (synchronous call), then released.
//	This satisfies the CGO rule: "A Go pointer to a pinned Go variable may
//	be stored in C memory."
//
// Returns the number of writes that completed with res > 0.
// A return value < len(staged) indicates at least one I/O error; the caller
// must treat the batch as failed and return an error to upstream.
func (b *cgoStorageBridge) submitVLogBatch(fd, diskIdx int, staged []stagedRecord) int {
	if b == nil || b.handle == nil || len(staged) == 0 {
		return 0
	}

	n := len(staged)

	// Allocate the C-side request array.  calloc returns zeroed memory so
	// result fields start at 0.
	reqs := (*C.BridgeWriteRequest)(C.calloc(C.size_t(n), C.sizeof_BridgeWriteRequest))
	if reqs == nil {
		return 0
	}
	defer C.free(unsafe.Pointer(reqs))

	reqSlice := unsafe.Slice(reqs, n)

	// Pin all Go backing arrays before storing their addresses in C memory.
	var pinner runtime.Pinner
	defer pinner.Unpin()

	for i, rec := range staged {
		if len(rec.buf) == 0 {
			continue
		}
		// Pin the ioPool buffer so the GC cannot move it while the C++ bridge
		// has a raw pointer to its backing array.
		pinner.Pin(&rec.buf[0])

		reqSlice[i].disk_idx    = C.int(diskIdx)
		reqSlice[i].fd          = C.int(fd)
		reqSlice[i].file_offset = C.uint64_t(uint64(rec.offset))
		reqSlice[i].data        = unsafe.Pointer(&rec.buf[0])
		reqSlice[i].len         = C.uint(uint32(rec.alignedSz))
		// kNoFixedBuf = UINT32_MAX: buffer is from ioPool, not from the
		// bridge's registered fixed-buffer pool.  io_uring_prep_write is
		// used (correct, just without pre-registered DMA mapping).  Still
		// faster than N separate pwrite syscalls because SQPOLL eliminates
		// the io_uring_enter cost on the hot path.
		reqSlice[i].buf_idx = C.uint(0xFFFFFFFF)
	}

	// Synchronous: blocks until all CQEs are collected by the C++ bridge.
	result := C.veltrix_bridge_submit_batch(b.handle, reqs, C.size_t(n))
	return int(result)
}

// bridgeStats returns aggregate counters from all 8 disk rings.
func (b *cgoStorageBridge) bridgeStats() (submits, completions, errors uint64) {
	if b == nil || b.handle == nil {
		return
	}
	submits     = uint64(C.veltrix_bridge_total_submits(b.handle))
	completions = uint64(C.veltrix_bridge_total_completions(b.handle))
	errors      = uint64(C.veltrix_bridge_total_errors(b.handle))
	return
}
