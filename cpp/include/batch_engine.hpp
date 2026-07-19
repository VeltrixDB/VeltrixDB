#pragma once
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * SliceView — a (ptr, len) pair mirroring a Go slice / string header.
 *
 * When passed from Go via the CGO bridge, `ptr` points into Go-managed memory.
 * The C++ layer MUST NOT retain these pointers after the batch call returns.
 * Layout (LP64):
 *   offset 0 : ptr  (8 B, const void*)
 *   offset 8 : len  (8 B, size_t)
 *   total    : 16 B, 8-byte aligned — no padding required
 */
typedef struct {
    const void* ptr;
    size_t      len;
} SliceView;

/*
 * BatchPutEntry — one record in a vectorized write batch.
 *
 * __attribute__((aligned(8))) makes the 8-byte alignment requirement explicit.
 * Without it, the compiler is free to place the struct at any alignment ≥ 1.
 * With it, arrays of BatchPutEntry are guaranteed to start on 8-byte boundaries
 * so that every SliceView.ptr field (offset 0 and 16) is always 8-byte aligned
 * — preventing misaligned-load penalties on the CPU's load-store units.
 *
 * Field layout (LP64, verified by static_assert in batch_engine.cpp):
 *   offset  0 : key          (SliceView, 16 B)
 *   offset 16 : value        (SliceView, 16 B)
 *   offset 32 : ttl_seconds  (int32_t,    4 B) — -1 = immortal
 *   offset 36 : shard_hint   (uint16_t,   2 B) — FNV-1a(key)%8192; 0 = recompute
 *   offset 38 : _pad[2]      (uint8_t,    2 B)
 *   total     : 40 B
 */
typedef struct __attribute__((aligned(8))) {
    SliceView key;
    SliceView value;
    int32_t   ttl_seconds;
    uint16_t  shard_hint;
    uint8_t   _pad[2];
} BatchPutEntry;

/*
 * BatchEngineConfig — extended constructor configuration.
 * Used by veltrix_batch_engine_create_ex() (Go 1.21+ Pinner path).
 */
typedef struct {
    int  num_threads; /* worker thread count; ≤ 0 defaults to logical CPU count */
    bool numa_aware;  /* pin worker threads to NUMA node of NVMe IRQ CPUs       */
    int  numa_node;   /* preferred NUMA node (-1 = auto-detect from sysfs)       */
} BatchEngineConfig;

/* Opaque engine handle. All functions are thread-safe with respect to
 * concurrent enqueue calls; only lifecycle functions require exclusivity. */
typedef struct VeltrixBatchEngine VeltrixBatchEngine;

/* ── Lifecycle ──────────────────────────────────────────────────────────────
 *
 * veltrix_batch_engine_create — simple constructor (backward-compat).
 * Workers are NOT NUMA-pinned; use veltrix_batch_engine_create_ex for that.
 */
VeltrixBatchEngine* veltrix_batch_engine_create(int num_threads);

/*
 * veltrix_batch_engine_create_ex — NUMA-aware constructor.
 * When cfg.numa_aware is true, each worker thread is pinned to the CPUs
 * of the NUMA node that owns the NVMe IRQ vectors (auto-detected from sysfs
 * when cfg.numa_node == -1).  This eliminates cross-NUMA memory fetches
 * on the critical I/O completion path.
 */
VeltrixBatchEngine* veltrix_batch_engine_create_ex(BatchEngineConfig cfg);

/* veltrix_batch_engine_destroy — drain in-flight work, join all threads, free
 * all resources.  Must not be called concurrently with any other function on
 * the same engine. */
void veltrix_batch_engine_destroy(VeltrixBatchEngine* engine);

/* ── Vectorized write ───────────────────────────────────────────────────────
 *
 * veltrix_batch_put — process `count` BatchPutEntry records in parallel.
 *
 * Algorithm:
 *   1. Group entry indices by shard: shard = FNV-1a(key) % 256.
 *      If entry.shard_hint != 0, the pre-computed hint is used directly.
 *   2. Dispatch each non-empty shard group to a worker thread.
 *      Up to num_threads shard groups execute concurrently.
 *   3. Block until all workers complete (synchronous call).
 *
 * Memory contract:
 *   entry.key.ptr and entry.value.ptr address the caller's memory (Go heap or
 *   C heap).  The C++ engine reads but never stores or frees these pointers.
 *   The caller guarantees the backing data remains live for the duration of the
 *   call.  All pointers are invalid after this function returns.
 *
 * Returns: number of entries successfully processed (== count on success).
 */
int veltrix_batch_put(
    VeltrixBatchEngine*  engine,
    const BatchPutEntry* entries,
    size_t               count
);

/* ── Vectorized read ────────────────────────────────────────────────────────
 *
 * veltrix_batch_get — look up `key_count` keys in parallel.
 *
 * `results` must be pre-allocated by the caller (at least key_count entries).
 *   Hit:  results[i].ptr → engine-owned buffer valid until next batch call.
 *         results[i].len → value length in bytes.
 *   Miss: results[i].ptr == NULL, results[i].len == 0.
 *
 * Returns: number of keys found.
 */
int veltrix_batch_get(
    VeltrixBatchEngine* engine,
    const SliceView*    keys,
    size_t              key_count,
    SliceView*          results
);

/* ── Diagnostics ────────────────────────────────────────────────────────────*/

uint64_t veltrix_batch_engine_puts_total(const VeltrixBatchEngine* engine);
uint64_t veltrix_batch_engine_gets_total(const VeltrixBatchEngine* engine);
int      veltrix_batch_engine_thread_count(const VeltrixBatchEngine* engine);

#ifdef __cplusplus
}  /* extern "C" */
#endif
