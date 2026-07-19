/*
 * vlog.cpp — C++ Value Log: io_uring SQPOLL reader + sector-aligned writer.
 *
 * Compiled only on Linux (liburing required).  Included by the CMake target
 * "veltrixdb_engine" which links against -luring.
 *
 * Key design decisions
 * ────────────────────
 * 1. SQPOLL ring (IORING_SETUP_SQPOLL): the kernel polls the SQ continuously
 *    so submission requires no io_uring_enter syscall on the hot path.
 *    After sq_thread_idle_ms of inactivity the kernel thread sleeps; the first
 *    wake-up costs one IORING_ENTER_SQ_WAKEUP via io_uring_submit.
 *
 * 2. O_DIRECT: all reads bypass the page cache, routing directly through the
 *    NVMe queue.  The buffer pointer and length must be 512-byte aligned.
 *    We use posix_memalign for per-slot buffers (aligned to 4096).
 *
 * 3. BatchRead submits all SQEs in one io_uring_submit call (amortises the
 *    SQPOLL wake-up cost) then harvests all CQEs with io_uring_wait_cqe.
 *
 * 4. CRC32C is verified on every read to detect bit-rot.  The SSE4.2 path
 *    (_mm_crc32_u8) runs at ~1 GB/s on modern CPUs; for 128 B values the
 *    per-record overhead is ~128 ns — well within P99 = 5 ms budget.
 */

#ifdef __linux__

#include "vlog.hpp"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// ── CRC32C implementation ─────────────────────────────────────────────────────

#if defined(__SSE4_2__) && defined(__x86_64__)
#include <nmmintrin.h>
namespace veltrix {
uint32_t vlog_crc32c(const uint8_t* data, size_t len) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    // Process 8 bytes at a time with the 64-bit instruction where possible.
    const auto* p64 = reinterpret_cast<const uint64_t*>(data);
    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        uint64_t word;
        std::memcpy(&word, data + i, 8);
        crc = static_cast<uint32_t>(_mm_crc32_u64(crc, word));
    }
    for (; i < len; ++i) {
        crc = _mm_crc32_u8(crc, data[i]);
    }
    return crc ^ 0xFFFFFFFFu;
}
} // namespace veltrix
#else
// Portable CRC32C table fallback (Castagnoli polynomial 0x82F63B78 reversed).
namespace veltrix {
namespace {
constexpr uint32_t kCRCPoly = 0x82F63B78u;
uint32_t crc_table[256];
bool crc_table_init = [] {
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) c = (c >> 1) ^ (kCRCPoly & -(c & 1));
        crc_table[i] = c;
    }
    return true;
}();
} // namespace

uint32_t vlog_crc32c(const uint8_t* data, size_t len) noexcept {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) crc = (crc >> 8) ^ crc_table[(crc ^ data[i]) & 0xFF];
    return crc ^ 0xFFFFFFFFu;
}
} // namespace veltrix
#endif

// ── AlignedBuf ────────────────────────────────────────────────────────────────

namespace veltrix {

VLogReader::AlignedBuf::AlignedBuf(size_t sz) : cap(sz) {
    if (::posix_memalign(&ptr, 4096, sz) != 0) {
        ptr = nullptr;
        cap = 0;
        throw std::bad_alloc();
    }
    std::memset(ptr, 0, sz);
}

VLogReader::AlignedBuf::~AlignedBuf() {
    if (ptr) {
        ::free(ptr);
        ptr = nullptr;
    }
}

VLogReader::AlignedBuf::AlignedBuf(AlignedBuf&& o) noexcept
    : ptr(o.ptr), cap(o.cap) {
    o.ptr = nullptr;
    o.cap = 0;
}

VLogReader::AlignedBuf& VLogReader::AlignedBuf::operator=(AlignedBuf&& o) noexcept {
    if (this != &o) {
        if (ptr) ::free(ptr);
        ptr   = o.ptr;
        cap   = o.cap;
        o.ptr = nullptr;
        o.cap = 0;
    }
    return *this;
}

// ── VLogReader ────────────────────────────────────────────────────────────────

VLogReader::VLogReader(int fd, unsigned sq_thread_idle_ms)
    : fd_(fd), ring_ok_(false)
{
    io_uring_params params{};
    params.flags         = IORING_SETUP_SQPOLL;
    params.sq_thread_idle = sq_thread_idle_ms;

    int ret = io_uring_queue_init_params(kRingDepth, &ring_, &params);
    if (ret < 0) {
        // SQPOLL requires CAP_SYS_ADMIN or Linux 5.11+ unprivileged SQPOLL.
        // Fall back to a plain ring if SQPOLL is unavailable (e.g. dev VM).
        io_uring_params plain_params{};
        ret = io_uring_queue_init_params(kRingDepth, &ring_, &plain_params);
        if (ret < 0) {
            // io_uring not available at all — reads will fail gracefully.
            return;
        }
    }
    ring_ok_ = true;

    // Pre-allocate one 4 KB aligned buffer per ring slot.  Each buffer is
    // large enough to hold one sector-aligned VLog record.
    // 4 KB covers header(24) + value(≤ 4096-24 = 4072 B) + padding.
    // For values larger than 4072 B, AlignedBuf is reallocated in BatchRead.
    bufs_.reserve(kRingDepth);
    for (unsigned i = 0; i < kRingDepth; ++i) {
        bufs_.emplace_back(static_cast<size_t>(kVLogBlockSize)); // one 4 KB block
    }
}

VLogReader::~VLogReader() {
    if (ring_ok_) io_uring_queue_exit(&ring_);
}

VLogReadResult VLogReader::decode(const AlignedBuf& buf, int intra_block, uint32_t value_len, int res) {
    VLogReadResult result;
    if (res < 0) {
        result.err = -res;
        return result;
    }

    // intra_block: bytes from the 4K-aligned read start to the actual record header.
    // For new (4K-aligned) records this is always 0; for legacy 512B-aligned records
    // it is (record_offset mod 4096).
    const auto* hdr = static_cast<const uint8_t*>(buf.ptr) + intra_block;

    // Validate magic.
    uint32_t magic;
    std::memcpy(&magic, hdr + kVhdrMagic, 4);
    if (magic != kVLogMagic) {
        result.err = EIO;
        return result;
    }

    // Read stored CRC.
    uint32_t stored_crc;
    std::memcpy(&stored_crc, hdr + kVhdrCRC, 4);

    // Compute CRC over the value payload.
    const auto* payload = hdr + kVLogHeaderBytes;
    uint32_t computed_crc = vlog_crc32c(payload, value_len);
    if (computed_crc != stored_crc) {
        result.err = EIO; // bit-rot or corruption
        return result;
    }

    result.value.assign(payload, payload + value_len);
    result.ok = true;
    return result;
}

VLogReadResult VLogReader::ReadSync(uint64_t offset, uint32_t value_len) {
    if (!ring_ok_) {
        VLogReadResult r;
        r.err = ENOTSUP;
        return r;
    }

    int raw_len = kVLogHeaderBytes + static_cast<int>(value_len);

    // Round offset DOWN to 4 KB boundary for O_DIRECT compatibility on 4Kn NVMe.
    // Legacy records at 512B-aligned offsets are handled via intra_block extraction.
    uint64_t aligned_offset = offset & ~static_cast<uint64_t>(kVLogBlockSize - 1);
    int      intra_block    = static_cast<int>(offset - aligned_offset);
    int      read_size      = (intra_block + raw_len + kVLogBlockSize - 1) & ~(kVLogBlockSize - 1);

    AlignedBuf* slot = &bufs_[0];
    if (static_cast<size_t>(read_size) > slot->cap) {
        *slot = AlignedBuf(static_cast<size_t>(read_size));
    }

    // Submit one PREAD SQE.
    io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
        VLogReadResult r; r.err = EBUSY; return r;
    }
    io_uring_prep_read(sqe, fd_, slot->ptr, static_cast<unsigned>(read_size),
                       static_cast<off_t>(aligned_offset));
    sqe->user_data = 0; // slot index

    io_uring_submit(&ring_);

    // Wait for the CQE.
    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe(&ring_, &cqe);
    int res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);

    return decode(*slot, intra_block, value_len, res);
}

void VLogReader::BatchRead(
    const uint64_t* offsets,
    const uint32_t* value_lens,
    VLogReadResult* results,
    size_t          count
) {
    if (!ring_ok_ || count == 0) return;

    // Submit all SQEs in one batch (one io_uring_enter for SQPOLL wake-up).
    size_t submitted = 0;
    // Per-slot intra-block offsets stored so CQE harvest can pass the right value
    // to decode().  VLAs require C99/C++14; use a fixed array capped at kRingDepth.
    int intra_blocks[kRingDepth] = {};

    for (size_t i = 0; i < count; ++i) {
        int raw_len = kVLogHeaderBytes + static_cast<int>(value_lens[i]);

        // Round offset DOWN to 4 KB boundary (O_DIRECT safe on 4Kn NVMe).
        uint64_t aligned_offset = offsets[i] & ~static_cast<uint64_t>(kVLogBlockSize - 1);
        int      intra_block    = static_cast<int>(offsets[i] - aligned_offset);
        int      read_size      = (intra_block + raw_len + kVLogBlockSize - 1) & ~(kVLogBlockSize - 1);

        size_t slot_idx = i % kRingDepth;
        intra_blocks[slot_idx] = intra_block;
        AlignedBuf& slot = bufs_[slot_idx];
        if (static_cast<size_t>(read_size) > slot.cap) {
            slot = AlignedBuf(static_cast<size_t>(read_size));
        }

        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            // Ring full — flush what we have, then continue.
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
            if (!sqe) { results[i].err = EBUSY; continue; }
        }
        io_uring_prep_read(sqe, fd_, slot.ptr,
                           static_cast<unsigned>(read_size),
                           static_cast<off_t>(aligned_offset));
        sqe->user_data = static_cast<uint64_t>(i); // use i as the slot/result index
        ++submitted;
    }

    if (submitted > 0) io_uring_submit(&ring_);

    // Harvest all CQEs.
    for (size_t i = 0; i < submitted; ++i) {
        io_uring_cqe* cqe = nullptr;
        io_uring_wait_cqe(&ring_, &cqe);

        size_t idx      = static_cast<size_t>(cqe->user_data);
        size_t slot_idx = idx % kRingDepth;
        results[idx]    = decode(bufs_[slot_idx], intra_blocks[slot_idx], value_lens[idx], cqe->res);
        io_uring_cqe_seen(&ring_, cqe);
    }
}

// ── VLogWriter ────────────────────────────────────────────────────────────────

VLogWriter::VLogWriter(int fd, uint64_t write_offset)
    : fd_(fd), offset_(write_offset) {}

int64_t VLogWriter::Append(const uint8_t* value, uint32_t value_len) {
    int raw_len     = kVLogHeaderBytes + static_cast<int>(value_len);
    int aligned_len = (raw_len + kVLogBlockSize - 1) & ~(kVLogBlockSize - 1);

    void* buf_ptr = nullptr;
    if (::posix_memalign(&buf_ptr, 4096, static_cast<size_t>(aligned_len)) != 0) {
        return -1;
    }
    auto* buf = static_cast<uint8_t*>(buf_ptr);
    std::memset(buf, 0, static_cast<size_t>(aligned_len));

    // Fill header (little-endian, matching Go encoding/binary.LittleEndian).
    uint32_t magic = kVLogMagic;
    std::memcpy(buf + kVhdrMagic,  &magic,     4);
    std::memcpy(buf + kVhdrValLen, &value_len, 4);

    uint32_t crc = vlog_crc32c(value, value_len);
    std::memcpy(buf + kVhdrCRC, &crc, 4);

    // Timestamp: seconds since epoch × 1e6 (µs).  Best-effort; not used for
    // correctness, only observability.
    struct timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t writeUs = static_cast<int64_t>(ts.tv_sec) * 1'000'000
                    + static_cast<int64_t>(ts.tv_nsec) / 1'000;
    std::memcpy(buf + kVhdrWriteUs, &writeUs, 8);

    std::memcpy(buf + kVLogHeaderBytes, value, value_len);

    int64_t start_offset = static_cast<int64_t>(offset_);
    ssize_t written = ::pwrite(fd_, buf, static_cast<size_t>(aligned_len), start_offset);
    ::free(buf_ptr);

    if (written != static_cast<ssize_t>(aligned_len)) return -1;

    offset_ += static_cast<uint64_t>(aligned_len);
    return start_offset;
}

bool VLogWriter::Flush() {
    return ::fdatasync(fd_) == 0;
}

// ── VLogContext ───────────────────────────────────────────────────────────────

bool VLogContext::open(int idx, const char* vlog_path) {
    disk_idx = idx;

    // Read fd: O_RDONLY | O_DIRECT for io_uring SQPOLL reads.
    read_fd = ::open(vlog_path, O_RDONLY | O_DIRECT | O_CLOEXEC);
    if (read_fd < 0) return false;

    // Write fd: O_WRONLY | O_DIRECT | O_CREAT for sector-aligned appends.
    write_fd = ::open(vlog_path, O_WRONLY | O_CREAT | O_DIRECT | O_CLOEXEC, 0644);
    if (write_fd < 0) {
        ::close(read_fd);
        read_fd = -1;
        return false;
    }

    // Determine current file size to seed the writer offset.
    struct stat st{};
    if (::fstat(write_fd, &st) != 0) {
        ::close(read_fd);
        ::close(write_fd);
        read_fd = write_fd = -1;
        return false;
    }
    uint64_t file_size = static_cast<uint64_t>(st.st_size);

    // SQPOLL idle = 10 ms — keeps the kernel thread active during a sustained
    // 2 M RPS read burst without burning a full CPU core when idle.
    reader = std::make_unique<VLogReader>(read_fd, /*sq_poll_idle_ms=*/10);
    writer = std::make_unique<VLogWriter>(write_fd, file_size);
    return true;
}

void VLogContext::close() {
    reader.reset();
    writer.reset();
    if (read_fd  >= 0) { ::close(read_fd);  read_fd  = -1; }
    if (write_fd >= 0) { ::close(write_fd); write_fd = -1; }
}

} // namespace veltrix

#endif // __linux__
