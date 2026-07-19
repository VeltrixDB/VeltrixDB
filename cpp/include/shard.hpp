#pragma once
#include "index_entry.hpp"
#include "lirs_cache.hpp"
#include "allocator.hpp"
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

// Forward-declare io_uring to avoid pulling in liburing in every TU that
// includes shard.hpp.  The actual ring is only used in shard.cpp.
struct io_uring;

namespace veltrix {

// ── Per-shard configuration ────────────────────────────────────────────────────

struct ShardConfig {
    uint16_t    shard_id{0};
    int         segment_fd{-1};                   // O_DIRECT segment file fd
    std::size_t write_block_bytes{128u * 1024};   // flush threshold (128 KB)
    std::size_t cache_bytes{256u * 1024 * 1024};  // 256 MB LIRS quota
    double      lir_ratio{0.99};                  // LIR fraction of cache
    unsigned    io_uring_depth{128};              // SQ/CQ ring depth
};

// ── On-disk record header ──────────────────────────────────────────────────────
//
// Written immediately before every key+value pair in the segment file.
// Padded to 64 bytes so the header occupies exactly one cache line and the
// immediately following key data starts on a cache-line boundary.
// The entire record (header + key + value) is further padded to the next
// 512-byte boundary for O_DIRECT compatibility.

struct alignas(64) RecordHeader {
    static constexpr uint32_t kMagic = 0xA0DBFF;

    uint32_t magic{kMagic};       // 4  @ 0  sanity marker for crash-recovery scans
    uint16_t key_len{0};          // 2  @ 4  byte length of key (follows header)
    uint8_t  _pad0[2]{};          // 2  @ 6  explicit padding (prevents implicit 2-byte gap)
    uint32_t value_len{0};        // 4  @ 8  byte length of value (follows key)
    uint32_t crc32c{0};           // 4  @ 12 CRC32C of (key || value)
    uint64_t write_ts_us{0};      // 8  @ 16 write timestamp, µs since epoch
    uint16_t ttl_encoded{0};      // 2  @ 24 (unix_sec >> 2); 0 = immortal
    uint8_t  flags{0};            // 1  @ 26 bit 0 = TOMBSTONE, bit 1 = COMPRESSED
    uint8_t  _pad[37]{};          // 37 @ 27 → total = 4+2+2+4+4+8+2+1+37 = 64
};

static_assert(sizeof(RecordHeader) == 64, "RecordHeader must be 64 bytes");

// ── Shard ─────────────────────────────────────────────────────────────────────
//
// A Shard implements the shared-nothing model: it owns its CPU core, its
// portion of the Index Vault, its LIRS Data Cache, its write buffer, and its
// io_uring ring.  No mutex is needed for any intra-shard operation.
//
// Cross-shard operations (e.g., the Defragmenter reading index entries) use
// the AtomicIndexEntry CAS interface and never hold any shard-internal lock.
//
// alignas(64) prevents false sharing between adjacent Shards in a shards[]
// array — each Shard starts on its own cache line.

class alignas(64) Shard {
public:
    explicit Shard(ShardConfig cfg);
    ~Shard();

    Shard(const Shard&)            = delete;
    Shard& operator=(const Shard&) = delete;
    Shard(Shard&&)                 = delete;
    Shard& operator=(Shard&&)      = delete;

    // ── Write path ─────────────────────────────────────────────────────────────
    //
    // 1. Build IndexEntry (Offset = current write_cursor_).
    // 2. Atomic-store into Index Vault.
    // 3. Serialise RecordHeader + key + value into the write buffer.
    // 4. If write buffer ≥ write_block_bytes_: submit io_uring write SQE.
    // 5. Insert value into LIRS cache.
    //
    // Returns false only if the io_uring SQ is full (backpressure signal).
    bool put(std::string_view key,
             std::span<const uint8_t> value,
             uint16_t ttl_seconds = 0);

    // ── Read path ──────────────────────────────────────────────────────────────
    //
    // 1. Check Index Vault: NOT FOUND → return nullopt.
    //                       TOMBSTONE  → return nullopt.
    //                       EXPIRED    → mark tombstone, return nullopt.
    // 2. Check LIRS cache.  HIT → return span.
    // 3. Submit async io_uring read (O_DIRECT) at entry.offset().
    //    Returns nullopt; the caller must call poll_completions() and retry.
    std::optional<std::vector<uint8_t>> get(std::string_view key);

    // ── Delete (tombstone) ─────────────────────────────────────────────────────
    //
    // CAS-loops on the AtomicIndexEntry until the TOMBSTONE bit is set.
    // Evicts the key from the LIRS cache.
    // Writes a tombstone record to the WAL buffer.
    bool del(std::string_view key);

    // ── I/O event loop ─────────────────────────────────────────────────────────

    // Flush write buffer to disk: submit an io_uring write SQE and wait for
    // the CQE.  Called by the shard event loop when idle or on shutdown.
    void flush();

    // Drain available CQEs from the ring.  For each completion, if the result
    // is negative report an error; otherwise mark the write as confirmed and
    // free the IO token.  Call this from the shard's polling loop.
    void poll_completions();

    // ── Cross-shard / Defragmenter interface ───────────────────────────────────

    // Read-only view of the Index Vault for the Defragmenter.
    // The Defragmenter uses AtomicIndexEntry::load() — no lock needed.
    const std::unordered_map<std::string, AtomicIndexEntry>& index() const noexcept {
        return index_;
    }

    // Mutable access for the Defragmenter to CAS new offsets after compaction.
    std::unordered_map<std::string, AtomicIndexEntry>& index_mutable() noexcept {
        return index_;
    }

    // ── Stats ──────────────────────────────────────────────────────────────────
    [[nodiscard]] uint16_t    shard_id()    const noexcept { return cfg_.shard_id; }
    [[nodiscard]] uint64_t    writes()      const noexcept { return writes_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t    reads()       const noexcept { return reads_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t    tombstones()  const noexcept { return tombstones_.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t    cache_hits()  const noexcept { return cache_.hits(); }
    [[nodiscard]] uint64_t    cache_miss()  const noexcept { return cache_.misses(); }
    [[nodiscard]] std::size_t index_size()  const noexcept { return index_.size(); }

private:
    // ── Internal helpers ───────────────────────────────────────────────────────

    // Serialise a record into write_buf_ at write_cursor_.
    // Returns the byte offset in the segment file where the record starts.
    uint64_t pack_record(std::string_view key,
                         std::span<const uint8_t> value,
                         uint16_t ttl_encoded,
                         bool is_tombstone);

    // Submit a write SQE for the bytes in write_buf_[0..len).
    // Returns false if SQ is full.
    bool submit_write_sqe(const void* buf, std::size_t len, uint64_t file_offset);

    // Pad record_size to next 512-byte boundary (O_DIRECT requirement).
    static constexpr std::size_t sector_align(std::size_t n) noexcept {
        return (n + 511u) & ~511u;
    }

    // Hardware-accelerated CRC32C using SSE4.2 intrinsics.
    static uint32_t crc32c(std::span<const uint8_t> data) noexcept;

    // Encode TTL: (unix_expire_sec) >> 2, clamped to 14 bits.
    static uint16_t encode_ttl(uint16_t ttl_seconds) noexcept;

    // Check whether an IndexEntry is past its TTL.
    static bool is_expired(const IndexEntry& e) noexcept;

    // ── Members ────────────────────────────────────────────────────────────────

    ShardConfig cfg_;

    // Index Vault: per-shard, no lock.
    std::unordered_map<std::string, AtomicIndexEntry> index_;

    // LIRS Data Cache.
    LIRSCache cache_;

    // io_uring ring — allocated on heap to avoid sizeof dependency on liburing.
    io_uring* ring_{nullptr};

    // Write buffer: 4096-byte aligned for O_DIRECT.
    // We over-allocate to 4 MB to avoid page splits during pack_record.
    static constexpr std::size_t kWriteBufSize = 4u * 1024u * 1024u;
    alignas(4096) uint8_t write_buf_[kWriteBufSize]{};

    std::size_t write_cursor_{0};    // next free byte in write_buf_
    uint64_t    segment_offset_{0};  // byte offset of next write in the file

    // Memory pool for IO tokens and scratchpad allocations.
    SegmentedPool pool_;

    // Monotonic counters — written only by the owning thread; read freely.
    std::atomic<uint64_t> writes_{0};
    std::atomic<uint64_t> reads_{0};
    std::atomic<uint64_t> tombstones_{0};
    std::atomic<uint64_t> io_errors_{0};
};

} // namespace veltrix
