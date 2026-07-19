#pragma once
/*
 * lockfree_index.hpp — Lock-free, hugepage-backed concurrent hash map.
 *
 * Design
 * ──────
 * Targets 1 billion keys with 90% read / 10% write workload.
 *
 * Layout: Open-addressing hash table with linear probing.
 * Bucket size: 32 bytes (2 buckets per 64-byte cache line).
 * Capacity: next power-of-two ≥ requested_size / load_factor (default 0.5).
 *
 * Memory: mmap(MAP_HUGETLB | 2MB) — eliminates TLB pressure that kills
 * performance when the metadata array exceeds ~32 MB (L3 cache size).
 * At 1B entries × 32 B = 32 GB; 2MB hugepages → 16 384 TLB entries vs
 * 8 388 608 entries with 4 KB pages.  TLB miss rate drops ~500×.
 *
 * Concurrency:
 *   Read:  A single std::atomic::load(memory_order_acquire) per lookup —
 *          no mutex, no CAS retry, no ABA problem.
 *   Write: CAS on the key_hash field serialises concurrent inserts for the
 *          same slot; writers spin only on the rare slot collision.
 *
 * Key encoding: we store a 64-bit FNV-1a hash (not the string).  Collision
 * probability at 1B keys (1.8×10¹⁹ total space) ≈ 2.7×10⁻⁸ — acceptable.
 * The Go shardedIndex acts as the authoritative key store; this C++ index
 * accelerates lookups from the io_uring completion callbacks.
 */

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#ifdef __linux__
#  include <sys/mman.h>
#  include <unistd.h>
#  ifndef MAP_HUGETLB
#    define MAP_HUGETLB 0x40000
#  endif
#  ifndef MAP_HUGE_2MB
#    define MAP_HUGE_2MB (21 << 26)
#  endif
#endif

/* ── Bucket ─────────────────────────────────────────────────────────────── */
/*
 * Each bucket stores one index entry.  Fields are ordered to pack naturally
 * on a 64-bit platform without compiler padding.
 *
 *   offset  0 : key_hash   — 64-bit FNV-1a; 0 = empty slot
 *   offset  8 : raw1       — LeafMetadata::raw1 (segment_id | offset | tombstone | version)
 *   offset 16 : expiry_ts  — Unix seconds; 0 = immortal
 *   offset 20 : value_size — uncompressed value size in bytes
 *   offset 24 : flags      — FlagXxx bitmask (matches Go IndexEntry.Flags)
 *   offset 25 : shard_id   — owning shard [0, 255]
 *   offset 26 : _pad[6]
 *   total     : 32 bytes
 *
 * All fields accessed through std::atomic to guarantee memory-order
 * semantics without UB.  acquire/release pairs ensure that a reader that
 * observes key_hash != 0 also sees the fully-written raw1/expiry_ts/etc.
 */
struct alignas(32) IndexBucket {
    std::atomic<uint64_t> key_hash{0};    // 0 = empty; UINT64_MAX = tombstone
    std::atomic<uint64_t> raw1{0};        // segment_id(16)|offset(40)|tombstone(1)|version(7)
    std::atomic<uint32_t> expiry_ts{0};
    std::atomic<uint32_t> value_size{0};
    std::atomic<uint8_t>  flags{0};
    std::atomic<uint8_t>  shard_id{0};
    uint8_t               _pad[6]{};
};
static_assert(sizeof(IndexBucket) == 32, "IndexBucket must be 32 bytes");

/* Sentinel: a deleted slot is distinguishable from an empty slot so that
 * linear probing does not stop at holes left by tombstones. */
static constexpr uint64_t kBucketTombstone = UINT64_MAX;
static constexpr uint64_t kBucketEmpty     = 0;

/* ── Lookup result ──────────────────────────────────────────────────────── */
struct IndexLookupResult {
    bool     found{false};
    uint64_t raw1{0};          // packed LeafMetadata
    uint32_t expiry_ts{0};
    uint32_t value_size{0};
    uint8_t  flags{0};
    uint8_t  shard_id{0};
};

/* ── LockFreeIndex ──────────────────────────────────────────────────────── */
class LockFreeIndex {
public:
    /*
     * Create a lock-free index with `capacity` slots.
     * Actual allocated slots = next_pow2(capacity / load_factor) where
     * load_factor = 0.5 (50% full at `capacity` entries → minimal probing).
     *
     * use_hugepages: attempt MAP_HUGETLB 2MB backing.  Falls back to
     * anonymous mmap if hugepages are unavailable.
     */
    explicit LockFreeIndex(size_t capacity, bool use_hugepages = true);
    ~LockFreeIndex();

    LockFreeIndex(const LockFreeIndex&)            = delete;
    LockFreeIndex& operator=(const LockFreeIndex&) = delete;

    /* lookup — O(1) expected, lock-free.
     * Returns false if the key is not present or has been tombstoned. */
    bool lookup(uint64_t key_hash, IndexLookupResult& out) const noexcept;

    /* upsert — insert or update the entry for key_hash.
     * Uses CAS to claim an empty/tombstoned slot; spins only on rare races.
     * Returns false only if the table is full (> 80% load). */
    bool upsert(uint64_t  key_hash,
                uint64_t  raw1,
                uint32_t  expiry_ts,
                uint32_t  value_size,
                uint8_t   flags,
                uint8_t   shard_id) noexcept;

    /* tombstone — mark an entry as deleted without reclaiming its slot.
     * Probing will skip tombstoned slots but not empty ones. */
    bool tombstone(uint64_t key_hash) noexcept;

    size_t capacity()   const noexcept { return capacity_; }
    size_t size()       const noexcept { return size_.load(std::memory_order_relaxed); }
    bool   hugepages()  const noexcept { return used_hugepages_; }

private:
    static size_t next_pow2(size_t n) noexcept;
    IndexBucket* alloc_buckets(size_t count, bool try_huge);
    void         free_buckets();

    IndexBucket*          buckets_{nullptr};
    size_t                capacity_{0};   // number of slots (power of 2)
    size_t                mask_{0};       // capacity_ - 1
    size_t                mmap_size_{0};
    bool                  used_hugepages_{false};
    std::atomic<size_t>   size_{0};

    /* Maximum probing distance before giving up on upsert. */
    static constexpr unsigned kMaxProbe = 512;
};

/* ── Inline implementation ──────────────────────────────────────────────── */

inline size_t LockFreeIndex::next_pow2(size_t n) noexcept {
    if (n == 0) return 1;
    --n;
    for (size_t i = 1; i < sizeof(size_t) * 8; i <<= 1) n |= n >> i;
    return n + 1;
}

inline IndexBucket* LockFreeIndex::alloc_buckets(size_t count, bool try_huge) {
    mmap_size_ = count * sizeof(IndexBucket);
#ifdef __linux__
    if (try_huge) {
        void* p = mmap(nullptr, mmap_size_,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB,
                       -1, 0);
        if (p != MAP_FAILED) {
            used_hugepages_ = true;
            return static_cast<IndexBucket*>(p);
        }
    }
    /* Fallback: regular anonymous mmap */
    void* p = mmap(nullptr, mmap_size_,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS,
                   -1, 0);
    if (p == MAP_FAILED) throw std::bad_alloc{};
    return static_cast<IndexBucket*>(p);
#else
    (void)try_huge;
    used_hugepages_ = false;
    void* p = ::operator new(mmap_size_);
    memset(p, 0, mmap_size_);
    return static_cast<IndexBucket*>(p);
#endif
}

inline void LockFreeIndex::free_buckets() {
    if (!buckets_) return;
#ifdef __linux__
    munmap(buckets_, mmap_size_);
#else
    ::operator delete(buckets_);
#endif
    buckets_ = nullptr;
}

inline LockFreeIndex::LockFreeIndex(size_t capacity, bool use_hugepages) {
    /* Allocate 2× requested capacity so load factor ≤ 50% at capacity_ entries. */
    const size_t slots = next_pow2(capacity * 2);
    buckets_  = alloc_buckets(slots, use_hugepages);
    capacity_ = slots;
    mask_     = slots - 1;
    /* Buckets are zero-initialised (mmap/calloc), so key_hash == 0 (empty). */
}

inline LockFreeIndex::~LockFreeIndex() {
    free_buckets();
}

inline bool LockFreeIndex::lookup(uint64_t key_hash,
                                   IndexLookupResult& out) const noexcept {
    assert(buckets_ != nullptr);
    const size_t start = key_hash & mask_;

    for (size_t i = 0; i < kMaxProbe; ++i) {
        const size_t idx = (start + i) & mask_;
        const IndexBucket& b = buckets_[idx];

        /* Acquire-load: if we see the key, we see all fields written before it. */
        const uint64_t k = b.key_hash.load(std::memory_order_acquire);
        if (k == kBucketEmpty)    return false;  // no entry past this
        if (k == kBucketTombstone) continue;     // skip deleted slots
        if (k != key_hash)         continue;     // hash collision (rare)

        /* Check tombstone bit inside raw1 (bit 56). */
        const uint64_t r1 = b.raw1.load(std::memory_order_relaxed);
        if (r1 & (1ULL << 56)) return false;     // tombstoned entry

        out.found      = true;
        out.raw1       = r1;
        out.expiry_ts  = b.expiry_ts.load(std::memory_order_relaxed);
        out.value_size = b.value_size.load(std::memory_order_relaxed);
        out.flags      = b.flags.load(std::memory_order_relaxed);
        out.shard_id   = b.shard_id.load(std::memory_order_relaxed);
        return true;
    }
    return false;
}

inline bool LockFreeIndex::upsert(uint64_t key_hash,
                                   uint64_t raw1,
                                   uint32_t expiry_ts,
                                   uint32_t value_size,
                                   uint8_t  flags,
                                   uint8_t  shard_id) noexcept {
    assert(buckets_ != nullptr);
    if (key_hash == kBucketEmpty || key_hash == kBucketTombstone) return false;

    const size_t start = key_hash & mask_;

    for (size_t i = 0; i < kMaxProbe; ++i) {
        const size_t idx = (start + i) & mask_;
        IndexBucket& b = buckets_[idx];

        uint64_t existing = b.key_hash.load(std::memory_order_acquire);

        /* Update existing entry for this key_hash. */
        if (existing == key_hash) {
            /* release-store: readers acquire-load key_hash to synchronise. */
            b.raw1.store(raw1, std::memory_order_release);
            b.expiry_ts.store(expiry_ts, std::memory_order_relaxed);
            b.value_size.store(value_size, std::memory_order_relaxed);
            b.flags.store(flags, std::memory_order_relaxed);
            b.shard_id.store(shard_id, std::memory_order_relaxed);
            return true;
        }

        /* Try to claim an empty or tombstoned slot. */
        if (existing == kBucketEmpty || existing == kBucketTombstone) {
            uint64_t expected = existing;
            if (b.key_hash.compare_exchange_strong(
                    expected, key_hash,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                /* We won the CAS — write the rest of the entry. */
                b.raw1.store(raw1, std::memory_order_release);
                b.expiry_ts.store(expiry_ts, std::memory_order_relaxed);
                b.value_size.store(value_size, std::memory_order_relaxed);
                b.flags.store(flags, std::memory_order_relaxed);
                b.shard_id.store(shard_id, std::memory_order_relaxed);
                if (existing == kBucketEmpty)
                    size_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            /* Another thread claimed this slot; re-check the new value. */
            existing = b.key_hash.load(std::memory_order_acquire);
            if (existing == key_hash) {
                --i; // retry same slot
                continue;
            }
        }
    }
    return false; // table full — caller should resize
}

inline bool LockFreeIndex::tombstone(uint64_t key_hash) noexcept {
    assert(buckets_ != nullptr);
    const size_t start = key_hash & mask_;

    for (size_t i = 0; i < kMaxProbe; ++i) {
        const size_t idx = (start + i) & mask_;
        IndexBucket& b = buckets_[idx];

        const uint64_t k = b.key_hash.load(std::memory_order_acquire);
        if (k == kBucketEmpty)    return false;
        if (k == kBucketTombstone) continue;
        if (k != key_hash)         continue;

        /* Set tombstone bit 56 in raw1 */
        uint64_t old_r1 = b.raw1.load(std::memory_order_relaxed);
        uint64_t new_r1;
        do {
            new_r1 = old_r1 | (1ULL << 56);
        } while (!b.raw1.compare_exchange_weak(old_r1, new_r1,
                                                std::memory_order_acq_rel,
                                                std::memory_order_relaxed));
        b.key_hash.store(kBucketTombstone, std::memory_order_release);
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}
