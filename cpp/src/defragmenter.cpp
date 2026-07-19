#include "defragmenter.hpp"
#include "shard.hpp"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#ifdef __linux__
#  include <sched.h>
#endif

namespace veltrix {

// ── Construction / destruction ─────────────────────────────────────────────────

Defragmenter::Defragmenter(std::vector<Shard*> shards, Config cfg)
    : shards_(std::move(shards))
    , cfg_(cfg)
{}

Defragmenter::~Defragmenter() {
    stop();
}

// ── Lifecycle ──────────────────────────────────────────────────────────────────

void Defragmenter::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) return; // already running
    thread_ = std::thread([this] { run_loop(); });
}

void Defragmenter::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    if (thread_.joinable()) thread_.join();
}

// ── Main loop ──────────────────────────────────────────────────────────────────

void Defragmenter::run_loop() {
    pin_thread();

    while (running_.load(std::memory_order_acquire)) {
        // ── Phase 1: reap tombstones ───────────────────────────────────────────
        reap_expired_tombstones();

        // ── Phase 2: compact dirty segments ───────────────────────────────────
        auto scores = score_all_segments();
        for (const auto& score : scores) {
            if (!running_.load(std::memory_order_relaxed)) break;
            if (score.needs_compaction(cfg_.garbage_threshold)) {
                if (compact_segment(score)) {
                    bytes_reclaimed_.fetch_add(score.dead_bytes,
                                              std::memory_order_relaxed);
                    segments_compacted_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        cycles_completed_.fetch_add(1, std::memory_order_relaxed);

        // Sleep until the next scan.  Use short ticks so stop() responds quickly.
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(cfg_.scan_interval_seconds);
        while (running_.load(std::memory_order_relaxed) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}

// ── Phase 1: Tombstone reaping ─────────────────────────────────────────────────

void Defragmenter::reap_expired_tombstones() {
    for (Shard* shard : shards_) {
        if (!shard) continue;

        // Iterate the shard's index.  We do NOT hold any shard mutex; reads
        // go through AtomicIndexEntry::load(acquire).
        for (auto& [key, atomic_entry] : shard->index_mutable()) {
            const IndexEntry entry = atomic_entry.load();

            if (!entry.is_tombstone()) continue;
            if (!tombstone_expired(entry)) continue;

            // Double-check under CAS: if the shard wrote a new value to this
            // key between the scan and now, the CAS will fail (entry changed).
            // We build a "null" entry representing "key removed from index".
            // In a real system we'd use a sentinel segment_id (e.g., 0xFFFF)
            // instead of literally zero, to distinguish "empty slot" from
            // "unwritten slot".
            IndexEntry expected = entry;
            const IndexEntry null_entry{0};
            if (atomic_entry.compare_exchange(expected, null_entry)) {
                tombstones_reaped_.fetch_add(1, std::memory_order_relaxed);
                // Note: we intentionally do NOT erase from the std::unordered_map
                // here.  Erasing while another thread might be iterating is
                // unsafe.  In production use a per-shard deferred-delete queue:
                // the Defragmenter enqueues the key; the shard thread drains it
                // at the start of its next event-loop tick.
            }
        }
    }
}

// ── Phase 2: Segment scoring and compaction ────────────────────────────────────

std::vector<Defragmenter::SegmentScore> Defragmenter::score_all_segments() {
    // In a complete implementation this would enumerate segment files in the
    // data directory (readdir on /var/lib/veltrixdb/data/*.seg).
    // Here we return an empty list as a well-defined stub.
    return {};
}

Defragmenter::SegmentScore Defragmenter::score_segment(uint16_t segment_id, int fd) {
    SegmentScore score;
    score.segment_id = segment_id;

    if (fd < 0) return score;

    // Walk the segment sequentially.
    // Each record starts with a 64-byte RecordHeader (same layout as shard.hpp).
    constexpr std::size_t kHdrSize = 64;
    alignas(512) uint8_t hdr_buf[kHdrSize];
    uint64_t file_pos = 0;

    while (true) {
        const ssize_t n = ::pread(fd, hdr_buf, kHdrSize, static_cast<off_t>(file_pos));
        if (n <= 0) break;  // EOF or error

        // Parse header fields (byte 0..3 = magic, 4..5 = key_len, 6..9 = value_len).
        const uint32_t magic     = *reinterpret_cast<const uint32_t*>(hdr_buf + 0);
        const uint16_t key_len   = *reinterpret_cast<const uint16_t*>(hdr_buf + 4);
        const uint32_t value_len = *reinterpret_cast<const uint32_t*>(hdr_buf + 6);
        const uint8_t  flags     = hdr_buf[18];

        if (magic != 0xA0DBFF) break;  // corrupt or end-of-valid-data

        // Read the key.
        std::string key(key_len, '\0');
        if (key_len > 0) {
            ::pread(fd, key.data(), key_len, static_cast<off_t>(file_pos + kHdrSize));
        }

        // Total record size (header + key + value), sector-aligned.
        const std::size_t raw_size = kHdrSize + key_len + value_len;
        const std::size_t rec_size = (raw_size + 511u) & ~511u;

        // Classify: live or dead?
        const bool is_tombstone = (flags & 0x01) != 0;
        const uint32_t offset   = static_cast<uint32_t>(file_pos);

        bool is_live = false;
        if (!is_tombstone) {
            const AtomicIndexEntry* atomic_entry = nullptr;
            for (Shard* shard : shards_) {
                auto it = shard->index().find(key);
                if (it != shard->index().end()) {
                    atomic_entry = &it->second;
                    break;
                }
            }
            if (atomic_entry) {
                const IndexEntry entry = atomic_entry->load();
                is_live = entry_points_here(entry, segment_id, offset);
            }
        } else {
            // Tombstone records are "live" only if still within grace period.
            const uint64_t write_ts_us =
                *reinterpret_cast<const uint64_t*>(hdr_buf + 10); // offset 10 in header
            const auto now_us =
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            const uint64_t grace_us = static_cast<uint64_t>(cfg_.gc_grace_seconds) * 1'000'000;
            is_live = (now_us - write_ts_us < grace_us);
        }

        if (is_live) score.live_bytes += rec_size;
        else         score.dead_bytes += rec_size;
        score.total_bytes += rec_size;

        file_pos += rec_size;
    }

    if (score.total_bytes > 0) {
        score.garbage_ratio = static_cast<double>(score.dead_bytes) /
                              static_cast<double>(score.total_bytes);
    }
    return score;
}

// compact_segment:
//   - Copy live records to a new segment file.
//   - fsync the new file.
//   - CAS all affected IndexEntries.
//   - Unlink the old segment.
bool Defragmenter::compact_segment(const SegmentScore& score) {
    const std::string old_path = "/var/lib/veltrixdb/data/seg_"
                                 + std::to_string(score.segment_id) + ".dat";
    const std::string new_path = old_path + ".compact";

    const int old_fd = ::open(old_path.c_str(), O_RDONLY | O_DIRECT);
    if (old_fd < 0) return false;

    const int new_fd = ::open(new_path.c_str(),
                              O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT,
                              0644);
    if (new_fd < 0) { ::close(old_fd); return false; }

    // ── Copy live records ──────────────────────────────────────────────────────

    constexpr std::size_t kHdrSize  = 64;
    constexpr std::size_t kCopyBuf  = 128u * 1024u; // 128 KB sequential write
    alignas(4096) static thread_local uint8_t copy_buf[kCopyBuf];

    uint64_t old_pos    = 0;
    uint64_t new_cursor = 0;
    std::size_t buf_cursor = 0;

    // Store (key, new_offset) for the CAS phase.
    std::vector<std::pair<std::string, uint32_t>> remapped;

    while (true) {
        alignas(512) uint8_t hdr_buf[kHdrSize];
        if (::pread(old_fd, hdr_buf, kHdrSize, static_cast<off_t>(old_pos)) < static_cast<ssize_t>(kHdrSize))
            break;

        const uint32_t magic     = *reinterpret_cast<const uint32_t*>(hdr_buf);
        const uint16_t key_len   = *reinterpret_cast<const uint16_t*>(hdr_buf + 4);
        const uint32_t value_len = *reinterpret_cast<const uint32_t*>(hdr_buf + 6);
        if (magic != 0xA0DBFF) break;

        const std::size_t raw_size = kHdrSize + key_len + value_len;
        const std::size_t rec_size = (raw_size + 511u) & ~511u;

        // Read key.
        std::string key(key_len, '\0');
        ::pread(old_fd, key.data(), key_len, static_cast<off_t>(old_pos + kHdrSize));

        // Decide liveness.
        bool is_live = false;
        const uint32_t old_offset = static_cast<uint32_t>(old_pos);
        for (Shard* shard : shards_) {
            auto it = shard->index().find(key);
            if (it == shard->index().end()) continue;
            const IndexEntry e = it->second.load();
            if (entry_points_here(e, score.segment_id, old_offset)) {
                is_live = true;
                break;
            }
        }

        if (is_live) {
            // Read entire record into copy_buf, flush if full.
            if (buf_cursor + rec_size > kCopyBuf) {
                ::write(new_fd, copy_buf, buf_cursor);
                buf_cursor = 0;
            }
            ::pread(old_fd, copy_buf + buf_cursor, rec_size,
                    static_cast<off_t>(old_pos));
            const uint32_t new_offset = static_cast<uint32_t>(new_cursor + buf_cursor);
            buf_cursor  += rec_size;
            new_cursor  += rec_size;

            remapped.emplace_back(key, new_offset);
        }

        old_pos += rec_size;
    }

    // Flush remaining buffer.
    if (buf_cursor > 0) {
        ::write(new_fd, copy_buf, buf_cursor);
    }

    // ── fsync new segment ──────────────────────────────────────────────────────
    if (::fdatasync(new_fd) < 0) {
        ::close(old_fd);
        ::close(new_fd);
        ::unlink(new_path.c_str());
        return false;
    }
    ::close(new_fd);
    ::close(old_fd);

    // ── Atomic CAS of IndexEntries ─────────────────────────────────────────────
    // We use the next available segment_id for the new file.  In production a
    // SegmentRegistry allocates these monotonically.
    const uint16_t new_seg_id = score.segment_id + 0x8000u; // high bit = compacted

    for (const auto& [key, new_offset] : remapped) {
        AtomicIndexEntry* ae = find_index_entry(key);
        if (!ae) continue;

        IndexEntry expected = ae->load();
        // Only update if it still points to the old location.
        if (!entry_points_here(expected, score.segment_id,
                               static_cast<uint32_t>(expected.offset()))) {
            continue; // shard wrote a newer entry while we were compacting
        }
        const IndexEntry updated = expected.with_offset(new_seg_id, new_offset >> 2);
        ae->compare_exchange(expected, updated);
        // On CAS failure: shard won the race; its new location is correct already.
    }

    // ── Rename new segment over old ────────────────────────────────────────────
    ::rename(new_path.c_str(), old_path.c_str());

    return true;
}

// ── Helpers ────────────────────────────────────────────────────────────────────

AtomicIndexEntry* Defragmenter::find_index_entry(const std::string& key) {
    for (Shard* shard : shards_) {
        auto it = shard->index_mutable().find(key);
        if (it != shard->index_mutable().end()) {
            return &it->second;
        }
    }
    return nullptr;
}

bool Defragmenter::entry_points_here(const IndexEntry& e,
                                     uint16_t segment_id,
                                     uint32_t offset) noexcept {
    return e.is_valid() &&
           e.segment_id() == segment_id &&
           e.offset()     == offset;
}

bool Defragmenter::tombstone_expired(const IndexEntry& e) const noexcept {
    // The IndexEntry does not store the write timestamp directly (it has a
    // 14-bit expiry field).  In a full implementation we'd store the tombstone
    // creation time in the expiry field and compare against now.
    // For demonstration: treat any tombstone with expiry == 0 as expired.
    return e.is_tombstone() && e.expiry() == 0;
}

void Defragmenter::pin_thread() {
#ifdef __linux__
    if (cfg_.pinned_cpu < 0) return;

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cfg_.pinned_cpu, &cpuset);
    ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
#endif
}

} // namespace veltrix
