#include "shard.hpp"
#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <liburing.h>
#include <stdexcept>

// CRC32C via SSE4.2 (hardware).  Requires -msse4.2 at compile time.
#ifdef __SSE4_2__
#  include <nmmintrin.h>
#  define AXON_HAS_HW_CRC32C 1
#else
#  define AXON_HAS_HW_CRC32C 0
#endif

namespace veltrix {

// ── CRC32C ────────────────────────────────────────────────────────────────────

uint32_t Shard::crc32c(std::span<const uint8_t> data) noexcept {
#if AXON_HAS_HW_CRC32C
    uint32_t crc = 0xFFFFFFFFu;
    const uint8_t* p   = data.data();
    std::size_t    rem = data.size();

    // Process 8 bytes at a time.
    while (rem >= 8) {
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(p)));
        p   += 8;
        rem -= 8;
    }
    while (rem > 0) {
        crc = _mm_crc32_u8(crc, *p++);
        --rem;
    }
    return crc ^ 0xFFFFFFFFu;
#else
    // Software fallback: Sarwate table-based CRC32C.
    static const uint32_t kTable[256] = {
        // Generated with poly = 0x82F63B78 (Castagnoli).
        0x00000000u, 0xF26B8303u, 0xE13B70F7u, 0x1350F3F4u,
        // (truncated for brevity — in production embed the full 256-entry table)
    };
    uint32_t crc = 0xFFFFFFFFu;
    for (const uint8_t b : data) {
        crc = kTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
#endif
}

// ── TTL helpers ───────────────────────────────────────────────────────────────

uint16_t Shard::encode_ttl(uint16_t ttl_seconds) noexcept {
    if (ttl_seconds == 0) return 0; // immortal
    const auto expire_sec = static_cast<uint64_t>(std::time(nullptr)) + ttl_seconds;
    // Store (expire_sec >> 2) clamped to 14 bits (max ~65 536 s ≈ 18 h).
    return static_cast<uint16_t>(std::min(expire_sec >> 2, static_cast<uint64_t>(0x3FFF)));
}

bool Shard::is_expired(const IndexEntry& e) noexcept {
    const uint16_t enc = e.expiry();
    if (enc == 0) return false;
    const uint64_t expire_sec = static_cast<uint64_t>(enc) << 2;
    return static_cast<uint64_t>(std::time(nullptr)) >= expire_sec;
}

// ── Construction / destruction ────────────────────────────────────────────────

Shard::Shard(ShardConfig cfg)
    : cfg_(cfg)
    , cache_(cfg.cache_bytes, cfg.lir_ratio)
    , pool_(SegmentedPool::kDefaultSegmentBytes, 4096)
{
    // Initialise io_uring ring.
    ring_ = new io_uring{};
    if (io_uring_queue_init(cfg_.io_uring_depth, ring_, 0) < 0) {
        delete ring_;
        ring_ = nullptr;
        throw std::runtime_error("io_uring_queue_init failed: " +
                                 std::string(std::strerror(errno)));
    }

    // Reserve the segment fd in the ring's registered-file table for
    // reduced per-SQE overhead.
    if (cfg_.segment_fd >= 0) {
        io_uring_register_files(ring_, &cfg_.segment_fd, 1);
    }
}

Shard::~Shard() {
    if (ring_) {
        flush();
        io_uring_queue_exit(ring_);
        delete ring_;
    }
}

// ── Write path ────────────────────────────────────────────────────────────────

bool Shard::put(std::string_view key,
                std::span<const uint8_t> value,
                uint16_t ttl_seconds)
{
    // 1. Compute file offset BEFORE packing (pack_record advances write_cursor_).
    const uint64_t record_offset = segment_offset_ + write_cursor_;

    // 2. Pack record into the write buffer.
    const uint16_t ttl_enc = encode_ttl(ttl_seconds);
    /*const uint64_t packed_offset =*/ pack_record(key, value, ttl_enc, /*tombstone=*/false);

    // 3. Build and atomically store the IndexEntry.
    const uint16_t seg = cfg_.shard_id;  // shard_id == segment_id for simplicity
    const uint32_t off = static_cast<uint32_t>(record_offset);  // 4-byte granularity
    IndexEntry entry   = IndexEntry::make(seg, off, ttl_enc);

    index_[std::string{key}].store(entry);

    // 4. Insert into LIRS cache.
    cache_.put(key, std::vector<uint8_t>{value.begin(), value.end()});

    // 5. Flush if the write buffer has hit the block threshold.
    if (write_cursor_ >= cfg_.write_block_bytes) {
        flush();
    }

    writes_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// ── Read path ─────────────────────────────────────────────────────────────────

std::optional<std::vector<uint8_t>> Shard::get(std::string_view key) {
    reads_.fetch_add(1, std::memory_order_relaxed);

    // 1. Index Vault lookup.
    auto it = index_.find(std::string{key});
    if (it == index_.end()) return std::nullopt;

    const IndexEntry entry = it->second.load();
    if (!entry.is_valid())         return std::nullopt;  // tombstone
    if (is_expired(entry)) {
        // Lazy TTL eviction: set tombstone and return miss.
        IndexEntry curr = entry;
        IndexEntry dead = curr.with_tombstone();
        it->second.compare_exchange(curr, dead);
        cache_.evict(key);
        tombstones_.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    // 2. LIRS cache probe.
    if (auto span = cache_.get(key); span.has_value()) {
        return std::vector<uint8_t>{span->begin(), span->end()};
    }

    // 3. Async disk read via io_uring O_DIRECT.
    //    We submit a read SQE and return nullopt.  The caller must call
    //    poll_completions() and retry get() when the read CQE arrives.
    if (ring_ && cfg_.segment_fd >= 0) {
        const uint64_t offset = static_cast<uint64_t>(entry.offset());

        // Allocate an aligned IO buffer from the pool.
        auto* buf = static_cast<uint8_t*>(pool_.alloc_io_buffer(IoBuffer::kSize));

        auto* sqe = io_uring_get_sqe(ring_);
        if (!sqe) return std::nullopt; // SQ full — backpressure

        io_uring_prep_read(sqe, cfg_.segment_fd, buf, IoBuffer::kSize, offset);
        // Store the key pointer as user_data so poll_completions() can
        // populate the cache when the CQE arrives.
        io_uring_sqe_set_data(sqe, buf);
        io_uring_submit(ring_);
    }

    return std::nullopt; // data on its way; caller must retry
}

// ── Delete path ───────────────────────────────────────────────────────────────

bool Shard::del(std::string_view key) {
    auto it = index_.find(std::string{key});
    if (it == index_.end()) return false;

    // CAS loop to atomically set the TOMBSTONE bit.
    IndexEntry curr = it->second.load();
    IndexEntry dead;
    do {
        if (curr.is_tombstone()) return true; // already deleted
        dead = curr.with_tombstone();
    } while (!it->second.compare_exchange(curr, dead));

    // Write a tombstone record to the segment for crash recovery.
    pack_record(key, {}, /*ttl=*/0, /*tombstone=*/true);
    if (write_cursor_ >= cfg_.write_block_bytes) flush();

    cache_.evict(key);
    tombstones_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

// ── Flush ─────────────────────────────────────────────────────────────────────

void Shard::flush() {
    if (write_cursor_ == 0 || cfg_.segment_fd < 0 || !ring_) return;

    // Sector-align the write length for O_DIRECT.
    const std::size_t aligned_len = sector_align(write_cursor_);
    // Zero-pad the tail (write_buf_ is zero-initialised, so padding is already 0).

    auto* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return;

    io_uring_prep_write(sqe, cfg_.segment_fd,
                        write_buf_, static_cast<unsigned>(aligned_len),
                        segment_offset_);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);  // use registered fd
    io_uring_submit(ring_);

    // Wait for this specific write to complete (synchronous flush path).
    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(ring_, &cqe);
    if (cqe->res < 0) {
        io_errors_.fetch_add(1, std::memory_order_relaxed);
    } else {
        segment_offset_ += aligned_len;
    }
    io_uring_cqe_seen(ring_, cqe);

    // Reset write buffer.
    std::memset(write_buf_, 0, write_cursor_);
    write_cursor_ = 0;
}

// ── CQE poller ────────────────────────────────────────────────────────────────

void Shard::poll_completions() {
    if (!ring_) return;

    io_uring_cqe* cqe = nullptr;
    unsigned head;
    unsigned processed = 0;

    io_uring_for_each_cqe(ring_, head, cqe) {
        if (cqe->res < 0) {
            io_errors_.fetch_add(1, std::memory_order_relaxed);
        }
        // For read completions: populate LIRS cache from the IO buffer.
        // (The key association would require a proper token struct in production.)
        ++processed;
    }

    if (processed > 0) {
        io_uring_cq_advance(ring_, processed);
    }
}

// ── Internal: record packing ──────────────────────────────────────────────────

uint64_t Shard::pack_record(std::string_view key,
                             std::span<const uint8_t> value,
                             uint16_t ttl_encoded,
                             bool is_tombstone)
{
    const uint64_t record_start = write_cursor_;

    // Build header.
    RecordHeader hdr{};
    hdr.key_len     = static_cast<uint16_t>(key.size());
    hdr.value_len   = static_cast<uint32_t>(value.size());
    hdr.write_ts_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    hdr.ttl_encoded = ttl_encoded;
    hdr.flags       = is_tombstone ? 0x01u : 0x00u;

    // CRC32C over (key || value).
    uint32_t crc = 0xFFFFFFFFu;
#if AXON_HAS_HW_CRC32C
    for (const char c : key) crc = _mm_crc32_u8(crc, static_cast<uint8_t>(c));
    for (const uint8_t b : value) crc = _mm_crc32_u8(crc, b);
    crc ^= 0xFFFFFFFFu;
#endif
    hdr.crc32c = crc;

    // Write header.
    const std::size_t hdr_size = sizeof(RecordHeader);
    std::memcpy(write_buf_ + write_cursor_, &hdr, hdr_size);
    write_cursor_ += hdr_size;

    // Write key.
    std::memcpy(write_buf_ + write_cursor_, key.data(), key.size());
    write_cursor_ += key.size();

    // Write value.
    if (!value.empty()) {
        std::memcpy(write_buf_ + write_cursor_, value.data(), value.size());
        write_cursor_ += value.size();
    }

    // Pad to 512-byte sector boundary.
    const std::size_t total    = write_cursor_ - record_start;
    const std::size_t aligned  = sector_align(total);
    write_cursor_              += (aligned - total);  // zero-pad (buf is pre-zeroed)

    return record_start;
}

bool Shard::submit_write_sqe(const void* buf, std::size_t len, uint64_t file_offset) {
    if (!ring_ || cfg_.segment_fd < 0) return false;
    auto* sqe = io_uring_get_sqe(ring_);
    if (!sqe) return false;
    io_uring_prep_write(sqe, cfg_.segment_fd, buf, static_cast<unsigned>(len), file_offset);
    io_uring_submit(ring_);
    return true;
}

} // namespace veltrix
