/*
 * native_index_capi.h — pure-C API for the off-heap Index Vault shard table.
 *
 * Included by the cgo preamble in index_table_native.go, so it must stay C99:
 * no C++ headers or syntax. The implementation is native_index.cpp in this
 * directory, compiled straight into the Go package by cgo (see that file for
 * why it is not under cpp/).
 *
 * One VxIndexTable holds one index shard. It is NOT internally synchronised:
 * the Go caller holds indexShard.mu around every call — RLock for the const
 * functions, Lock for the rest. Any number of concurrent const calls on one
 * table is safe; a mutating call must be alone.
 *
 * Entries are exactly 64 bytes and opaque to C with two exceptions, both
 * mirrored by static assertions on the Go side (index_table_native.go):
 *   bytes [0, 8)   the 64-bit key hash (IndexEntry.KeyHash), compared before
 *                  the key bytes on lookup;
 *   bytes [56, 64) owned by the table (IndexEntry._reserved). Whatever the
 *                  caller passes there is ignored, and it reads back as zero.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VX_IDX_ENTRY_SIZE 64

typedef struct VxIndexTable VxIndexTable;

/* One IndexEntry, passed BY VALUE across the boundary. Returning and taking
 * entries by value — rather than through out-pointers — is deliberate: cgo
 * forces any Go pointer handed to C onto the heap, so an out-parameter would
 * cost an allocation on every lookup. (#cgo noescape would avoid that, but it
 * needs a go.mod language version of 1.23 and this module targets 1.19.) */
typedef struct {
    uint8_t b[VX_IDX_ENTRY_SIZE];
} VxEntry;

/* Entry plus outcome. status meanings are per function. */
typedef struct {
    VxEntry e;
    int32_t status;
} VxResult;

/* Create an empty table. Allocates nothing but the handle; slots and the key
 * arena are allocated on first insert. Returns NULL on allocation failure. */
VxIndexTable* vx_idx_new(void);

/* Release everything the table owns, then the handle itself. NULL is a no-op. */
void vx_idx_free(VxIndexTable* t);

/* Look up key. status 1 = found (e holds the entry), 0 = absent.
 * key may be NULL when klen == 0. */
VxResult vx_idx_get(const VxIndexTable* t, uint64_t hash,
                    const char* key, size_t klen);

/* Insert or overwrite with in. status 1 = key existed (e holds the entry it
 * replaced), 0 = fresh insert, -1 = allocation failure or a key the arena
 * cannot address (> 4 GiB of keys in one shard), in which case the table is
 * unchanged. */
VxResult vx_idx_put(VxIndexTable* t, uint64_t hash,
                    const char* key, size_t klen, VxEntry in);

/* Remove key. Returns 1 if it was present, 0 if not. */
int vx_idx_del(VxIndexTable* t, uint64_t hash, const char* key, size_t klen);

/* Number of entries. */
size_t vx_idx_len(const VxIndexTable* t);

/* Batched iteration, so a full-shard walk costs one cgo transition per
 * batch instead of one per key.
 *
 * Resumes at slot *cursor (start with 0) and copies up to max entries into
 * entries (max * 64 bytes). When keybuf != NULL, each key's bytes are packed
 * back to back into keybuf (capacity keycap) and its length written to
 * klens[i]; when keybuf == NULL keys are skipped entirely.
 *
 * Returns the number of entries copied and advances *cursor. The walk is
 * complete when the function returns 0 and *need == 0. If it returns 0 with
 * *need > 0, the next key does not fit in keycap bytes: retry with a keybuf
 * of at least *need bytes.
 *
 * The table must not be mutated between calls of one walk. */
size_t vx_idx_scan(const VxIndexTable* t, uint64_t* cursor,
                   void* entries, size_t max,
                   char* keybuf, size_t keycap, uint32_t* klens,
                   size_t* need);

/* Bytes currently allocated by this table (slots + key arena). */
uint64_t vx_idx_bytes(const VxIndexTable* t);

/* Bytes currently allocated by ALL live tables in the process. */
uint64_t vx_idx_total_bytes(void);

#ifdef __cplusplus
} /* extern "C" */
#endif
