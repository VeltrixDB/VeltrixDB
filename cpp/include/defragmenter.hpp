#pragma once
#include "index_entry.hpp"
#include <atomic>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

namespace veltrix {

class Shard;

// ── Defragmenter ──────────────────────────────────────────────────────────────
//
// The Defragmenter runs on its own CPU core (pinned via sched_setaffinity)
// and enforces two garbage-collection policies:
//
//  1. Tombstone Reaping
//     Once a tombstone's age exceeds gc_grace_seconds, the IndexEntry is
//     physically removed from the shard's Index Vault via a CAS.  The grace
//     period ensures that lagging replicas have time to apply the delete before
//     the tombstone disappears — preventing "zombie data" from re-appearing
//     during Read Repair.
//
//  2. Segment Compaction
//     Segment files accumulate garbage: stale copies (a key was re-written to
//     a new offset), tombstones past their grace period, and expired TTL
//     records.  The Defragmenter scores each segment by its garbage ratio.
//     When a segment exceeds garbage_threshold it is compacted:
//       a. Open a new segment file.
//       b. Copy live records (those whose IndexEntry still points here) using
//          large sequential writes (≥ 128 KB) to minimise SSD wear.
//       c. fsync the new segment.
//       d. CAS each affected IndexEntry to the new (segment_id, offset).
//       e. Unlink the old segment file.
//
// Thread safety:
//   The Defragmenter reads IndexEntries via AtomicIndexEntry::load() (acquire)
//   and writes via compare_exchange (acq_rel / acquire).  No shard mutex is
//   ever held, so the shard threads run without interruption.
//   If a shard concurrently updates an IndexEntry between the scan and the CAS
//   (step d), the CAS fails and the Defragmenter simply skips that key — it
//   will be revisited in the next cycle, by which time the shard's own entry
//   is already pointing to the new location anyway.

class Defragmenter {
public:
    // ── Configuration ─────────────────────────────────────────────────────────
    struct Config {
        // NSDMIs in a nested struct trigger CWG 1497 on GCC when the struct
        // is used in a default argument of the enclosing class.  Use a
        // constructor initializer list instead.
        Config()
            : garbage_threshold(0.30)
            , gc_grace_seconds(86400)
            , scan_interval_seconds(60)
            , max_compact_rate_bps(200u * 1024u * 1024u)
            , pinned_cpu(-1)
        {}

        // Compact a segment when its garbage fraction ≥ this value.
        double      garbage_threshold;

        // Tombstones younger than this are preserved (anti-zombie guard).
        uint32_t    gc_grace_seconds;     // 24 h

        // How often to scan all segments.
        uint32_t    scan_interval_seconds;

        // Token-bucket rate limit for compaction writes (bytes / second).
        // Prevents the Defragmenter from saturating the NVMe on a busy node.
        std::size_t max_compact_rate_bps; // 200 MB/s

        // CPU core to pin the Defragmenter thread to (-1 = no pinning).
        int         pinned_cpu;
    };

    // ── Segment record view (read-only, from a segment scan) ──────────────────
    struct RecordView {
        uint16_t segment_id;
        uint32_t offset;       // byte offset within segment
        uint32_t record_bytes; // total size on disk (header + key + value + padding)
        uint64_t key_hash;     // FNV-1a of key, for cross-shard lookup
        std::string key;       // needed for Index Vault lookup
        bool is_tombstone;
        uint64_t write_ts_us;  // for tombstone age check
    };

    // ── Segment score ──────────────────────────────────────────────────────────
    struct SegmentScore {
        uint16_t    segment_id{0};
        uint64_t    total_bytes{0};
        uint64_t    live_bytes{0};
        uint64_t    dead_bytes{0};
        double      garbage_ratio{0.0};  // dead_bytes / total_bytes

        [[nodiscard]] bool needs_compaction(double threshold) const noexcept {
            return total_bytes > 0 && garbage_ratio >= threshold;
        }
    };

    // ── Construction ──────────────────────────────────────────────────────────
    explicit Defragmenter(std::vector<Shard*> shards, Config cfg = {});
    ~Defragmenter();

    Defragmenter(const Defragmenter&)            = delete;
    Defragmenter& operator=(const Defragmenter&) = delete;

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    void start();
    void stop();   // signals the loop to exit; blocks until the thread joins

    // ── Stats ─────────────────────────────────────────────────────────────────
    [[nodiscard]] uint64_t tombstones_reaped()  const noexcept {
        return tombstones_reaped_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t segments_compacted() const noexcept {
        return segments_compacted_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t bytes_reclaimed() const noexcept {
        return bytes_reclaimed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t cycles_completed() const noexcept {
        return cycles_completed_.load(std::memory_order_relaxed);
    }

private:
    // ── Main loop ─────────────────────────────────────────────────────────────

    void run_loop();

    // ── Phase 1: tombstone reaping ─────────────────────────────────────────────

    // Scan all shard Index Vaults for tombstones past the grace period.
    // Remove them via CAS.
    void reap_expired_tombstones();

    // ── Phase 2: segment compaction ────────────────────────────────────────────

    // Score every live segment file.  Returns a sorted list (worst first).
    std::vector<SegmentScore> score_all_segments();

    // Score one segment: walk its records and classify each as live or dead.
    // A record is LIVE if:
    //   • The corresponding IndexEntry exists in some shard's Index Vault AND
    //   • The IndexEntry points to THIS segment AND this exact offset AND
    //   • The record is not a tombstone past its grace period AND
    //   • The record's TTL has not expired.
    // Everything else is DEAD (stale copy, expired, tombstone-past-grace).
    SegmentScore score_segment(uint16_t segment_id, int fd);

    // Compact segment `score.segment_id`:
    //   1. Create new segment file.
    //   2. Copy live records (large sequential writes).
    //   3. fsync new segment.
    //   4. CAS all affected IndexEntries to new (segment_id, offset).
    //   5. Unlink old segment.
    // Returns true on success.
    bool compact_segment(const SegmentScore& score);

    // ── Helpers ────────────────────────────────────────────────────────────────

    // Look up a key across all shards.  Returns a pointer to the first
    // AtomicIndexEntry found, or nullptr.
    AtomicIndexEntry* find_index_entry(const std::string& key);

    // True if the entry still points to (segment_id, offset).
    static bool entry_points_here(const IndexEntry& e,
                                  uint16_t segment_id,
                                  uint32_t offset) noexcept;

    // True if the tombstone is old enough to be reaped.
    bool tombstone_expired(const IndexEntry& e) const noexcept;

    // Pin the calling thread to cfg_.pinned_cpu (Linux only).
    void pin_thread();

    // ── Data members ──────────────────────────────────────────────────────────

    std::vector<Shard*> shards_;
    Config              cfg_;
    std::thread         thread_;
    std::atomic<bool>   running_{false};

    std::atomic<uint64_t> tombstones_reaped_{0};
    std::atomic<uint64_t> segments_compacted_{0};
    std::atomic<uint64_t> bytes_reclaimed_{0};
    std::atomic<uint64_t> cycles_completed_{0};
};

} // namespace veltrix
