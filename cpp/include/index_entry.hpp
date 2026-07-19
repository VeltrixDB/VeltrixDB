#pragma once
#include <atomic>
#include <cstdint>
#include <cassert>

namespace veltrix {

// ── 64-bit Packed Index Entry ─────────────────────────────────────────────────
//
// Every key in the Index Vault maps to exactly one IndexEntry.  The entire
// entry fits inside one uint64_t so that reads and writes are single-instruction
// atomic operations on x86_64 and ARM64 — no lock, no cache-line crossing.
//
// Bit layout (LSB → MSB):
//
//   [0 ..15]  SegmentID : 16   which segment file (.dat) holds the value
//   [16..47]  Offset    : 32   byte offset inside that segment (4B granularity
//                               → max segment size 4 KB × 4 B = 16 TB)
//   [48..61]  Expiry    : 14   TTL encoded as (unix_sec >> 2); 0 = immortal;
//                               4-second granularity, ~65 536 s max TTL
//   [62..63]  Flags     : 2    bit 0 = TOMBSTONE, bit 1 = HOT (cache hint)
//
// To update an entry atomically the caller loads the current value, builds a
// modified IndexEntry, then CAS-loops until it wins.  Contention is
// structurally impossible within a shard (only one writer per shard) and rare
// between the shard thread and the Defragmenter.

struct IndexEntry {
    uint64_t raw{0};

    // ── Bit masks & shifts ─────────────────────────────────────────────────────
    static constexpr uint64_t kSegMask    = 0x000000000000FFFFull;  // [0 ..15]
    static constexpr uint64_t kOffMask    = 0x0000FFFFFFFF0000ull;  // [16..47]
    static constexpr uint64_t kExpMask    = 0x3FFF000000000000ull;  // [48..61]
    static constexpr uint64_t kFlagsMask  = 0xC000000000000000ull;  // [62..63]

    static constexpr unsigned kSegShift   = 0;
    static constexpr unsigned kOffShift   = 16;
    static constexpr unsigned kExpShift   = 48;
    static constexpr unsigned kFlagsShift = 62;

    // Flag bit positions within the 2-bit flags field.
    static constexpr uint8_t kFlagTombstone = 0b01;
    static constexpr uint8_t kFlagHot       = 0b10;

    // ── Field accessors ────────────────────────────────────────────────────────
    [[nodiscard]] constexpr uint16_t segment_id() const noexcept {
        return static_cast<uint16_t>((raw & kSegMask) >> kSegShift);
    }
    [[nodiscard]] constexpr uint32_t offset() const noexcept {
        return static_cast<uint32_t>((raw & kOffMask) >> kOffShift);
    }
    [[nodiscard]] constexpr uint16_t expiry() const noexcept {
        return static_cast<uint16_t>((raw & kExpMask) >> kExpShift);
    }
    [[nodiscard]] constexpr uint8_t flags() const noexcept {
        return static_cast<uint8_t>((raw & kFlagsMask) >> kFlagsShift);
    }

    [[nodiscard]] constexpr bool is_tombstone() const noexcept {
        return (flags() & kFlagTombstone) != 0;
    }
    [[nodiscard]] constexpr bool is_hot() const noexcept {
        return (flags() & kFlagHot) != 0;
    }
    [[nodiscard]] constexpr bool is_valid() const noexcept {
        return raw != 0 && !is_tombstone();
    }
    [[nodiscard]] constexpr bool is_expired(uint64_t now_sec) const noexcept {
        const uint16_t exp = expiry();
        if (exp == 0) return false;  // immortal
        return now_sec >= (static_cast<uint64_t>(exp) << 2);
    }

    // ── Factories ──────────────────────────────────────────────────────────────
    [[nodiscard]] static constexpr IndexEntry make(
        uint16_t seg, uint32_t off,
        uint16_t exp = 0, uint8_t fl = 0) noexcept
    {
        uint64_t r = 0;
        r |= static_cast<uint64_t>(seg)        << kSegShift;
        r |= static_cast<uint64_t>(off)        << kOffShift;
        r |= static_cast<uint64_t>(exp & 0x3FFF) << kExpShift;
        r |= static_cast<uint64_t>(fl  & 0x03)   << kFlagsShift;
        return IndexEntry{r};
    }

    // Return a copy with the TOMBSTONE bit set.
    [[nodiscard]] constexpr IndexEntry with_tombstone() const noexcept {
        const uint8_t fl = flags() | kFlagTombstone;
        return IndexEntry{(raw & ~kFlagsMask) |
                          (static_cast<uint64_t>(fl) << kFlagsShift)};
    }

    // Return a copy with the HOT bit set or cleared.
    [[nodiscard]] constexpr IndexEntry with_hot(bool hot) const noexcept {
        const uint8_t fl = hot ? (flags() | kFlagHot) : (flags() & ~kFlagHot);
        return IndexEntry{(raw & ~kFlagsMask) |
                          (static_cast<uint64_t>(fl) << kFlagsShift)};
    }

    // Return a copy with a new disk offset (after segment compaction).
    [[nodiscard]] constexpr IndexEntry with_offset(
        uint16_t new_seg, uint32_t new_off) const noexcept
    {
        uint64_t r = raw;
        r = (r & ~(kSegMask | kOffMask));
        r |= static_cast<uint64_t>(new_seg) << kSegShift;
        r |= static_cast<uint64_t>(new_off) << kOffShift;
        return IndexEntry{r};
    }
};

static_assert(sizeof(IndexEntry) == 8, "IndexEntry must be exactly 8 bytes");

// ── Atomic Index Entry ─────────────────────────────────────────────────────────
//
// Wraps IndexEntry in a std::atomic<uint64_t>.  The whole struct is
// alignas(64) so that each AtomicIndexEntry occupies one full cache line —
// preventing false sharing when adjacent entries are accessed by different
// cores (e.g., the shard thread and the Defragmenter).
//
// The owning shard thread is the sole writer.  The Defragmenter reads via
// load(acquire) and updates via CAS — if the shard made a concurrent write,
// the CAS fails and the Defragmenter skips that key until the next cycle.
struct alignas(64) AtomicIndexEntry {
    std::atomic<uint64_t> raw{0};

    // Padding fills the rest of the cache line so no two AtomicIndexEntries
    // share a line.  (64 - sizeof(atomic<uint64_t>) = 56 bytes of padding.)
    uint8_t _pad[56]{};

    [[nodiscard]] IndexEntry load() const noexcept {
        return IndexEntry{raw.load(std::memory_order_acquire)};
    }

    void store(IndexEntry e) noexcept {
        raw.store(e.raw, std::memory_order_release);
    }

    // CAS: atomically replace `expected` with `desired`.
    // Returns true on success.  On failure, `expected` is updated with the
    // actual current value so the caller can inspect it.
    bool compare_exchange(IndexEntry& expected, IndexEntry desired) noexcept {
        return raw.compare_exchange_strong(
            expected.raw, desired.raw,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }
};

static_assert(sizeof(AtomicIndexEntry) == 64,
              "AtomicIndexEntry must be exactly 64 bytes (one cache line)");

} // namespace veltrix
