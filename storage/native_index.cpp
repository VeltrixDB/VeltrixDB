/*
 * native_index.cpp — off-heap Index Vault shard table. C API and contract:
 * native_index_capi.h.
 *
 * Why this lives in storage/ and not cpp/src/: cgo compiles any .cpp in the
 * package directory directly, and the go build cache tracks those files. A
 * shim that #includes ../cpp/src/x.cpp (the pattern the io_uring bindings
 * use) is NOT tracked — edit the included file and `go test` silently keeps
 * running the stale object until something forces `go build -a`. That was
 * measured here: an injected bug in the probe loop passed the model test
 * until the cache was bypassed. Keep this file, and its header, in storage/.
 *
 * Portable (mmap + malloc, no io_uring), so it builds on every cgo
 * target including the macOS dev loop. Not part of cpp/CMakeLists.txt.
 *
 * Why this exists
 * ───────────────
 * The Go index was map[string]*IndexEntry: per key a map slot, a string
 * header, the key bytes and a separately allocated 64 B entry, every one of
 * them a pointer the GC must trace on every cycle, with GOGC=100 headroom on
 * top. This table holds the same bytes in memory the Go runtime never sees:
 * the GC does not scan it and does not size heap headroom against it.
 *
 * Layout — three arrays per shard
 * ───────────────────────────────
 *   recs   the 64-byte IndexEntry records, DENSE: record i is live for every
 *          i < count, so no memory goes to empty slots. Bytes [0, 8) hold the
 *          full key hash; bytes [56, 64) hold the table's key reference —
 *          arena offset (u32) and key length (u32).
 *   slots  the hash table proper: open addressing, linear probing,
 *          power-of-two capacity, max load 3/4, 8 bytes per slot:
 *            high 32 bits  tag = high 32 bits of mix(hash)
 *            low 32 bits   record position + 1   (0 = empty slot)
 *          The home slot is the top log2(cap) bits of mix(hash), which are
 *          also the top bits of the tag — so a slot knows its own home, and
 *          growing or shifting slots never touches a record.
 *   arena  key bytes back to back. Offset 0 is never handed out. Deletes
 *          leave garbage; the arena is compacted once garbage outweighs live
 *          bytes, so it stays within ~2x of live.
 *
 * An earlier revision stored the 64-byte records directly in the slots. At
 * the table's average load (~0.56 between growths) that spent ~50 B/key on
 * empty cache lines: measured 168 B/key against 122 B/key for this layout
 * (2M 24-byte keys). Keeping empty slots at 8 bytes is what makes the
 * off-heap index actually smaller than the Go map, not just invisible to GC.
 *
 * Probing starts at a mixed hash, not the raw FNV value: every key in one
 * shard shares its low 13 bits (that is how it was routed here), so indexing
 * on the raw hash would pile the whole shard into 1/8192 of the table.
 *
 * Deletes use backward-shift deletion on the slots (no tombstones, so probe
 * runs never degrade with churn) and swap-remove on the records (the last
 * record moves into the hole and its one slot is repointed).
 *
 * Arrays of a page and up are individually mmap'd (see "Where the memory
 * comes from" below for the measurement that forced this); from 2 MiB they
 * are also MADV_HUGEPAGE'd on Linux, so a large index is TLB-friendly without
 * pre-reserving hugetlbfs pages.
 *
 * Concurrency: none here, by design. The Go caller serialises writers and
 * lets readers share via indexShard.mu, exactly as it did for the map. Every
 * const function is safe to run concurrently with other const functions.
 */

#include "native_index_capi.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>

#ifdef __linux__
#  ifndef _GNU_SOURCE
#    define _GNU_SOURCE 1 // mremap
#  endif
#endif
#include <sys/mman.h>

namespace {

constexpr size_t   kEntry        = VX_IDX_ENTRY_SIZE;
constexpr size_t   kRefOff       = 56;  // key reference lives in [56, 64)
constexpr size_t   kMinSlots     = 16;
constexpr size_t   kMinRecs      = 8;
constexpr size_t   kMinArena     = 1024;
[[maybe_unused]] constexpr size_t kMmapMin = size_t(2) << 20; // MADV_HUGEPAGE threshold (Linux)
constexpr uint64_t kMaxArena     = UINT32_MAX;  // offsets are u32
constexpr size_t   kCompactFloor = 64 * 1024;   // don't compact tiny arenas on delete

std::atomic<uint64_t> g_total_bytes{0};

struct Rec {
    unsigned char b[kEntry];
};
static_assert(sizeof(Rec) == kEntry, "a record is exactly one IndexEntry");

inline uint64_t load_hash(const Rec& r) {
    uint64_t h;
    std::memcpy(&h, r.b, 8);
    return h;
}
inline uint32_t load_off(const Rec& r) {
    uint32_t v;
    std::memcpy(&v, r.b + kRefOff, 4);
    return v;
}
inline uint32_t load_len(const Rec& r) {
    uint32_t v;
    std::memcpy(&v, r.b + kRefOff + 4, 4);
    return v;
}
inline void store_ref(Rec& r, uint32_t off, uint32_t len) {
    std::memcpy(r.b + kRefOff, &off, 4);
    std::memcpy(r.b + kRefOff + 4, &len, 4);
}
// Copy a record out to the caller: the key reference is table-private, so
// the caller sees zeros where IndexEntry._reserved is.
inline void copy_out(void* dst, const Rec& r) {
    std::memcpy(dst, r.b, kRefOff);
    std::memset(static_cast<unsigned char*>(dst) + kRefOff, 0, kEntry - kRefOff);
}

// splitmix64 finaliser. FNV-1a's low bits are fixed within a shard and its
// high bits are weakly mixed for short keys; this spreads both.
inline uint64_t mix(uint64_t h) {
    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return h;
}

inline uint32_t tag_of(uint64_t h) { return uint32_t(mix(h) >> 32); }
inline uint64_t make_slot(uint32_t tag, size_t pos) { return (uint64_t(tag) << 32) | uint64_t(pos + 1); }
inline uint32_t slot_tag(uint64_t s) { return uint32_t(s >> 32); }
inline size_t   slot_pos(uint64_t s) { return size_t(uint32_t(s)) - 1; }

// Where the memory comes from — measured, not stylistic.
//
// All 8192 shards of an index grow at about the same rate, through the same
// sequence of sizes. With the system allocator that is pathological: the
// block a shard frees when it grows is a size no other shard will ask for
// again (they have all outgrown it), and its neighbours are other shards'
// live arrays, so it can neither be reused nor coalesced. Measured with 5M
// keys: 113 B/key allocated, 345-360 B/key RSS — worse than the Go map (168)
// it replaces. The same run with arrays presized (no growth at all) settled
// at 132 B/key, which pinned the cause on growth.
//
// So every array of a page or more is its own mmap: freeing it hands the
// pages straight back to the OS, and growth on Linux is mremap, which moves
// page-table entries instead of copying. Only sub-page arrays (the first few
// growths of each shard) use malloc; the waste those can strand is bounded by
// ~8 KiB per array per shard however large the index gets.
//
// Cost: a large index holds ~3 mappings per shard, ~25K per engine. Linux
// defaults vm.max_map_count to 65530; scripts/sysctl.conf raises it.
// Arrays of kMmapMin and up are also MADV_HUGEPAGE'd on Linux.

constexpr size_t kPage = 4096;

inline size_t page_round(size_t bytes) { return (bytes + kPage - 1) & ~(kPage - 1); }

// What an allocation of `bytes` really occupies — the unit g_total_bytes
// counts, so alloc and free always add and subtract the same amount.
inline size_t footprint(size_t bytes) { return bytes >= kPage ? page_round(bytes) : bytes; }

void advise(void* p, size_t mapped) {
#ifdef MADV_HUGEPAGE
    if (mapped >= kMmapMin) madvise(p, mapped, MADV_HUGEPAGE);
#else
    (void)p;
    (void)mapped;
#endif
}

void* map_pages(size_t bytes) {
    const size_t len = page_round(bytes);
    void* p = mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) return nullptr;
    advise(p, len);
    return p;
}

// Zeroed allocation (fresh anonymous pages are zero).
void* raw_alloc(size_t bytes) {
    void* p = bytes >= kPage ? map_pages(bytes) : std::calloc(1, bytes);
    if (!p) return nullptr;
    g_total_bytes.fetch_add(footprint(bytes), std::memory_order_relaxed);
    return p;
}

void raw_free(void* p, size_t bytes) {
    if (!p) return;
    if (bytes >= kPage) {
        munmap(p, page_round(bytes));
    } else {
        std::free(p);
    }
    g_total_bytes.fetch_sub(footprint(bytes), std::memory_order_relaxed);
}

// Grow p from old_bytes to new_bytes keeping the first keep bytes. Contents
// past keep are unspecified — callers only read what they wrote. Returns
// nullptr on failure with p still valid.
void* raw_realloc(void* p, size_t old_bytes, size_t new_bytes, size_t keep) {
    void* np = nullptr;
    if (new_bytes < kPage) {
        np = std::realloc(p, new_bytes); // both sub-page, so both malloc'd
    } else if (p && old_bytes >= kPage) {
#ifdef __linux__
        np = mremap(p, page_round(old_bytes), page_round(new_bytes), MREMAP_MAYMOVE);
        if (np == MAP_FAILED) return nullptr;
        advise(np, page_round(new_bytes));
#else
        np = map_pages(new_bytes);
        if (!np) return nullptr;
        if (keep) std::memcpy(np, p, keep);
        munmap(p, page_round(old_bytes));
#endif
    } else {
        np = map_pages(new_bytes);
        if (!np) return nullptr;
        if (keep) std::memcpy(np, p, keep);
        std::free(p); // sub-page (or null) before
    }
    if (!np) return nullptr;
    g_total_bytes.fetch_add(footprint(new_bytes) - footprint(old_bytes), std::memory_order_relaxed);
    return np;
}

// Grow-by-half, so a dense array's slack averages ~17% rather than the ~33%
// of doubling.
inline size_t grown(size_t cur, size_t need, size_t floor) {
    size_t n = cur + cur / 2;
    if (n < need) n = need;
    if (n < floor) n = floor;
    return n;
}

} // namespace

struct VxIndexTable {
    uint64_t* slots   = nullptr;
    size_t    cap     = 0;  // slot count: power of two, or 0 before the first insert
    unsigned  bits    = 0;  // log2(cap)
    Rec*      recs    = nullptr;
    size_t    rcap    = 0;
    size_t    count   = 0;
    char*     arena   = nullptr;
    size_t    acap    = 0;
    size_t    aused   = 0;  // includes the reserved byte at offset 0
    size_t    garbage = 0;  // arena bytes owned by deleted keys

    size_t mask() const { return cap - 1; }
    // Home slot from the tag: the top `bits` bits of mix(hash). cap never
    // exceeds 2^32 (record positions are u32), so bits <= 32.
    size_t home_tag(uint32_t tag) const { return bits ? size_t(tag >> (32 - bits)) : 0; }

    bool rec_eq(const Rec& r, uint64_t h, const char* key, size_t klen) const {
        if (load_hash(r) != h || load_len(r) != klen) return false;
        return klen == 0 || std::memcmp(arena + load_off(r), key, klen) == 0;
    }

    // Slot index holding key, or cap when absent. Terminates because the
    // load factor keeps at least a quarter of the slots empty.
    size_t find(uint64_t h, const char* key, size_t klen) const {
        if (cap == 0) return cap;
        const uint32_t tag = tag_of(h);
        for (size_t i = home_tag(tag);; i = (i + 1) & mask()) {
            const uint64_t s = slots[i];
            if (s == 0) return cap;
            if (slot_tag(s) == tag && rec_eq(recs[slot_pos(s)], h, key, klen)) return i;
        }
    }

    bool grow_slots() {
        const size_t ncap = cap ? cap * 2 : kMinSlots;
        if (ncap > (size_t(1) << 32)) return false;
        auto* ns = static_cast<uint64_t*>(raw_alloc(ncap * sizeof(uint64_t)));
        if (!ns) return false;
        unsigned nbits = 0;
        while ((size_t(1) << nbits) < ncap) ++nbits;
        for (size_t i = 0; i < cap; ++i) {
            const uint64_t s = slots[i];
            if (s == 0) continue;
            size_t j = size_t(slot_tag(s) >> (32 - nbits));
            while (ns[j] != 0) j = (j + 1) & (ncap - 1);
            ns[j] = s;
        }
        raw_free(slots, cap * sizeof(uint64_t));
        slots = ns;
        cap   = ncap;
        bits  = nbits;
        return true;
    }

    bool grow_recs() {
        const size_t ncap = grown(rcap, count + 1, kMinRecs);
        if (ncap - 1 > UINT32_MAX - 1) return false; // positions are u32, +1 in the slot
        auto* nr = static_cast<Rec*>(raw_realloc(recs, rcap * sizeof(Rec), ncap * sizeof(Rec), count * sizeof(Rec)));
        if (!nr) return false;
        recs = nr;
        rcap = ncap;
        return true;
    }

    // Rebuild the arena at capacity ncap holding only live keys. A fresh
    // block, not a realloc: live keys are gathered out of the old one.
    bool compact_arena(size_t ncap) {
        char* na = static_cast<char*>(raw_alloc(ncap));
        if (!na) return false;
        size_t used = 1;
        for (size_t i = 0; i < count; ++i) {
            Rec& r = recs[i];
            const uint32_t len = load_len(r);
            if (len) std::memcpy(na + used, arena + load_off(r), len);
            store_ref(r, uint32_t(used), len);
            used += len;
        }
        raw_free(arena, acap);
        arena   = na;
        acap    = ncap;
        aused   = used;
        garbage = 0;
        return true;
    }

    // Append key bytes; returns the offset, or 0 on failure.
    uint32_t arena_append(const char* key, size_t klen) {
        if (aused == 0) aused = 1; // offset 0 is never a real key
        const size_t live = aused - garbage;
        if (uint64_t(live) + klen > kMaxArena) return 0;
        if (aused + klen > acap) {
            if (garbage > live || uint64_t(aused) + klen > kMaxArena) {
                // Mostly garbage (or out of offset space): rebuild instead of growing.
                size_t ncap = grown(live, live + klen, kMinArena);
                if (ncap > kMaxArena) ncap = size_t(kMaxArena);
                if (!compact_arena(ncap)) return 0;
            } else {
                size_t ncap = grown(acap, aused + klen, kMinArena);
                if (ncap > kMaxArena) ncap = size_t(kMaxArena);
                char* na = static_cast<char*>(raw_realloc(arena, acap, ncap, aused));
                if (!na) return 0;
                arena = na;
                acap  = ncap;
            }
        }
        const uint32_t off = uint32_t(aused);
        if (klen) std::memcpy(arena + off, key, klen);
        aused += klen;
        return off;
    }

    void release_all() {
        raw_free(slots, cap * sizeof(uint64_t));
        raw_free(recs, rcap * sizeof(Rec));
        raw_free(arena, acap);
        slots = nullptr;
        recs  = nullptr;
        arena = nullptr;
        cap = rcap = count = 0;
        bits = 0;
        acap = aused = garbage = 0;
    }

    int put(uint64_t h, const char* key, size_t klen, const void* in, void* old) {
        if (klen > UINT32_MAX) return -1;
        const size_t at = find(h, key, klen);
        if (at != cap) {
            Rec& r = recs[slot_pos(slots[at])];
            if (old) copy_out(old, r);
            std::memcpy(r.b, in, kRefOff); // keep the key reference
            std::memcpy(r.b, &h, 8);
            return 1;
        }
        // Reserve everything first so a failure leaves the table unchanged.
        if ((count + 1) * 4 > cap * 3 && !grow_slots()) return -1;
        if (count == rcap && !grow_recs()) return -1;
        const uint32_t off = arena_append(key, klen);
        if (off == 0) return -1;

        const size_t pos = count++;
        Rec& r = recs[pos];
        std::memcpy(r.b, in, kRefOff);
        std::memcpy(r.b, &h, 8);
        store_ref(r, off, uint32_t(klen));

        const uint32_t tag = tag_of(h);
        size_t i = home_tag(tag);
        while (slots[i] != 0) i = (i + 1) & mask();
        slots[i] = make_slot(tag, pos);
        return 0;
    }

    int del(uint64_t h, const char* key, size_t klen) {
        const size_t at = find(h, key, klen);
        if (at == cap) return 0;
        const size_t pos = slot_pos(slots[at]);
        garbage += load_len(recs[pos]);

        // 1. Backward-shift delete the slot: pull later members of the probe
        //    run into the hole unless their home lies cyclically in (hole, j].
        size_t i = at;
        for (size_t j = (i + 1) & mask(); slots[j] != 0; j = (j + 1) & mask()) {
            const size_t k = home_tag(slot_tag(slots[j]));
            const bool stays = i <= j ? (i < k && k <= j) : (i < k || k <= j);
            if (stays) continue;
            slots[i] = slots[j];
            i = j;
        }
        slots[i] = 0;

        // 2. Swap-remove the record: move the last one into the hole and
        //    repoint the single slot that referenced it.
        const size_t last = --count;
        if (pos != last) {
            recs[pos] = recs[last];
            const uint32_t tag = tag_of(load_hash(recs[pos]));
            for (size_t s = home_tag(tag);; s = (s + 1) & mask()) {
                if (slots[s] != 0 && slot_pos(slots[s]) == last) {
                    slots[s] = make_slot(tag, pos);
                    break;
                }
            }
        }

        if (count == 0) {
            release_all(); // an empty shard costs nothing
            return 1;
        }
        const size_t live = aused - garbage;
        if (garbage > kCompactFloor && garbage > live) {
            compact_arena(grown(live, live, kMinArena)); // best effort: old arena stays valid on failure
        }
        return 1;
    }
};

extern "C" {

static_assert(sizeof(VxEntry) == kEntry, "VxEntry is one IndexEntry");

VxIndexTable* vx_idx_new(void) {
    void* p = std::calloc(1, sizeof(VxIndexTable));
    if (!p) return nullptr;
    return new (p) VxIndexTable();
}

void vx_idx_free(VxIndexTable* t) {
    if (!t) return;
    t->release_all();
    t->~VxIndexTable();
    std::free(t);
}

VxResult vx_idx_get(const VxIndexTable* t, uint64_t hash, const char* key, size_t klen) {
    VxResult r;
    const size_t at = t->find(hash, key, klen);
    if (at == t->cap) {
        r.status = 0;
        return r;
    }
    copy_out(r.e.b, t->recs[slot_pos(t->slots[at])]);
    r.status = 1;
    return r;
}

VxResult vx_idx_put(VxIndexTable* t, uint64_t hash, const char* key, size_t klen, VxEntry in) {
    VxResult r;
    r.status = t->put(hash, key, klen, in.b, r.e.b);
    return r;
}

int vx_idx_del(VxIndexTable* t, uint64_t hash, const char* key, size_t klen) {
    return t->del(hash, key, klen);
}

size_t vx_idx_len(const VxIndexTable* t) { return t->count; }

size_t vx_idx_scan(const VxIndexTable* t, uint64_t* cursor, void* entries, size_t max,
                   char* keybuf, size_t keycap, uint32_t* klens, size_t* need) {
    *need = 0;
    size_t n  = 0;
    size_t kb = 0;
    uint64_t i = *cursor;
    auto* out = static_cast<unsigned char*>(entries);
    // Records are dense, so the cursor is simply a record position.
    for (; i < t->count && n < max; ++i) {
        const Rec& r = t->recs[i];
        if (keybuf) {
            const uint32_t len = load_len(r);
            if (kb + len > keycap) {
                if (n == 0) *need = len;
                break;
            }
            if (len) std::memcpy(keybuf + kb, t->arena + load_off(r), len);
            klens[n] = len;
            kb += len;
        }
        copy_out(out + n * kEntry, r);
        ++n;
    }
    *cursor = i;
    return n;
}

uint64_t vx_idx_bytes(const VxIndexTable* t) {
    return footprint(t->cap * sizeof(uint64_t)) + footprint(t->rcap * sizeof(Rec)) + footprint(t->acap);
}

uint64_t vx_idx_total_bytes(void) { return g_total_bytes.load(std::memory_order_relaxed); }

} // extern "C"
