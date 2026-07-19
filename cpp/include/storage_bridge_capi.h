/*
 * storage_bridge_capi.h — pure-C API for the VeltrixDB io_uring write bridge.
 *
 * This header is included by the CGO preamble (compiled with cc/gcc, not g++).
 * It must contain NO C++ headers or syntax — only C99/C11-compatible types and
 * the extern "C" function declarations from storage_bridge.hpp.
 *
 * The full C++ implementation lives in:
 *   cpp/include/storage_bridge.hpp  (C++ header, included by storage_bridge.cpp)
 *   cpp/src/storage_bridge.cpp      (implementation, compiled with g++ -std=c++20)
 */

#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __linux__

/* Opaque handle — Go holds a pointer to this type, never inspects it. */
typedef struct VeltrixStorageBridge VeltrixStorageBridge;

/*
 * Per-write-request descriptor.  Passed as an array from Go to
 * veltrix_bridge_submit_batch().
 *
 * Fixed-buffer path (buf_idx != UINT32_MAX):
 *   1. Obtain buffer pointer: veltrix_bridge_get_fixed_buf(bridge, disk, idx)
 *   2. Copy payload into it.
 *   3. Set data = that pointer, buf_idx = idx.
 *   The kernel reuses the pre-registered DMA mapping — zero per-call overhead.
 *
 * Fallback path (buf_idx == UINT32_MAX):
 *   data = any 512-byte-aligned pointer.  The kernel maps/unmaps on every
 *   io_uring_prep_write call (correct but slightly slower than fixed buffers).
 */
typedef struct {
    int      disk_idx;      /* target disk index (0 … numDisks-1) */
    int      fd;            /* open file descriptor */
    uint64_t file_offset;   /* byte offset in file */
    void*    data;          /* write buffer (512-byte-aligned for O_DIRECT) */
    uint32_t len;           /* byte count (sector-multiple for O_DIRECT) */
    uint32_t buf_idx;       /* registered buffer index; UINT32_MAX = regular */
    int32_t  result;        /* filled by bridge: bytes written or -errno */
} BridgeWriteRequest;

/*
 * Construction config — mirrors StorageBridgeConfig in storage_bridge.hpp.
 * All fields must be set before passing to veltrix_bridge_create().
 */
typedef struct {
    int      num_disks;             /* number of NVMe disks (1-8) */
    int      ring_depth;            /* io_uring SQ/CQ depth per ring */
    int      sq_poll;               /* non-zero → enable IORING_SETUP_SQPOLL */
    uint32_t sq_poll_idle_ms;       /* SQPOLL thread idle timeout in ms */
    int      sq_thread_cpu[8];      /* per-disk SQ thread CPU; -1 = any */
    uint64_t fixed_buf_size;        /* size in bytes of each fixed buffer */
    uint64_t fixed_bufs_per_ring;   /* number of fixed buffers per ring */
} BridgeConfig;

#ifdef __cplusplus
extern "C" {
#endif

/* Create and initialise a bridge.  Returns NULL on failure (no liburing,
 * kernel too old, insufficient privileges, etc.). */
VeltrixStorageBridge* veltrix_bridge_create(BridgeConfig cfg);

/* Drain in-flight I/O, destroy io_uring rings, free all memory. */
void veltrix_bridge_destroy(VeltrixStorageBridge* bridge);

/*
 * Submit up to 1024 write requests.  Blocks until all CQEs are collected.
 * reqs[i].result is filled with bytes written (> 0) or -errno on completion.
 * Returns the total number of successful completions (result > 0).
 */
int veltrix_bridge_submit_batch(VeltrixStorageBridge* bridge,
                                BridgeWriteRequest*   reqs,
                                size_t                count);

/* Returns a pointer to fixed buffer buf_idx for the given disk ring.
 * The caller copies the payload here, then sets BridgeWriteRequest.buf_idx. */
void* veltrix_bridge_get_fixed_buf(VeltrixStorageBridge* bridge,
                                   int                   disk_idx,
                                   unsigned              buf_idx);

/* Size in bytes of each fixed buffer (same for all disks). */
uint64_t veltrix_bridge_fixed_buf_size(const VeltrixStorageBridge* bridge);

/* Number of fixed buffers registered per disk ring. */
unsigned veltrix_bridge_fixed_bufs_per_ring(const VeltrixStorageBridge* bridge);

/* Shard-to-disk mapping: returns shardID % numDisks.
 * Must match the Go storage/engine.go routing invariant. */
int veltrix_bridge_disk_for_shard(const VeltrixStorageBridge* bridge,
                                  uint16_t                    shard_id);

/* Aggregate I/O counters (monotonically increasing). */
uint64_t veltrix_bridge_total_submits(const VeltrixStorageBridge* bridge);
uint64_t veltrix_bridge_total_completions(const VeltrixStorageBridge* bridge);
uint64_t veltrix_bridge_total_errors(const VeltrixStorageBridge* bridge);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __linux__ */
