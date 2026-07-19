#pragma once
/*
 * vlog.hpp — C++ Value Log with io_uring SQPOLL reader.
 *
 * Architecture
 * ────────────
 * One VLog per NVMe disk, mirroring the Go VLog.  The C++ layer owns the
 * high-speed read path: each VLog has a dedicated io_uring ring configured
 * with IORING_SETUP_SQPOLL so the kernel polls the SQ without a syscall on
 * the hot path.  The Go layer handles all writes (VLog::Append is called from
 * Go via the CGO bridge); C++ reads are triggered by the batch-get path.
 *
 * Record format (must match Go vlog.go exactly):
 *
 *   Offset  Size  Field
 *    0       4    Magic (0x564C5402 = "VLT\x02")
 *    4       4    ValLen  (value bytes, not including header or padding)
 *    8       4    CRC32C  of value bytes
 *   12       4    Reserved
 *   16       8    WriteTimestampUs  (int64 LE, µs since Unix epoch)
 *   ─── 24 bytes ───
 *   24    (ValLen)  Value bytes
 *   24+V   pad      Zero-bytes to next 512-byte sector boundary
 *
 * io_uring SQPOLL
 * ───────────────
 * IORING_SETUP_SQPOLL creates a kernel thread that continuously polls the SQ.
 * When sq_thread_idle ms elapse without submissions, the kernel thread sleeps
 * and the first SQE wakeup requires one io_uring_enter syscall.  After that
 * the thread polls without syscalls until idle again.
 *
 * At 2 M RPS with SQPOLL, syscalls drop from 2M/s to ~1/s for the SQ; the CQ
 * completion thread calls io_uring_wait_cqe which uses IORING_OP_NOP+TIMEOUT
 * internally — still one syscall per completion batch, not per I/O.
 *
 * Thread safety
 * ─────────────
 * VLogReader is not thread-safe for concurrent Submit() calls from different
 * goroutines.  The C++ batch-get path serialises all reads through the same
 * reader per disk; concurrent disks use independent readers and rings.
 */

#ifdef __linux__

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// liburing public API (shipped with liburing-dev).
#include <liburing.h>

namespace veltrix {

// ── Wire format constants (must match Go vlog.go) ────────────────────────────

static constexpr uint32_t kVLogMagic       = 0x564C5402u; // "VLT\x02"
static constexpr int      kVLogHeaderBytes  = 24;
static constexpr int      kVLogSectorSize   = 512;  // legacy; new records use kVLogBlockSize
static constexpr int      kVLogBlockSize    = 4096; // O_DIRECT block size for 4Kn NVMe; matches Go vlogBlockSize

// Byte offsets within the 24-byte header.
static constexpr int kVhdrMagic   = 0;
static constexpr int kVhdrValLen  = 4;
static constexpr int kVhdrCRC     = 8;
static constexpr int kVhdrWriteUs = 16;

// ── CRC32C (Castagnoli) ───────────────────────────────────────────────────────

// Hardware-accelerated CRC32C using the SSE4.2 crc32 instruction.
// Falls back to a table-based implementation when SSE4.2 is unavailable.
uint32_t vlog_crc32c(const uint8_t* data, size_t len) noexcept;

// ── VLogReadResult ────────────────────────────────────────────────────────────

struct VLogReadResult {
    std::vector<uint8_t> value; // decoded value bytes (empty on error)
    bool                 ok{false};
    int                  err{0}; // -errno on failure
};

// ── VLogReader ────────────────────────────────────────────────────────────────
//
// Per-disk io_uring SQPOLL reader.  One VLogReader per NVMe disk.
//
// Usage:
//   VLogReader reader(fd, /*sq_poll_idle_ms=*/10);
//   auto res = reader.ReadSync(offset, value_len);
//
class VLogReader {
public:
    // fd must be opened with O_RDONLY|O_DIRECT.
    // sq_thread_idle_ms: how long the SQPOLL kernel thread idles before
    //   sleeping.  Lower = less CPU burn when idle; higher = faster cold start.
    //   10 ms is a good balance for a dedicated NVMe reader thread.
    explicit VLogReader(int fd, unsigned sq_thread_idle_ms = 10);
    ~VLogReader();

    // Not copyable or movable — owns a live io_uring ring.
    VLogReader(const VLogReader&)            = delete;
    VLogReader& operator=(const VLogReader&) = delete;

    // ReadSync issues one PREAD to the ring and blocks until the CQE arrives.
    // offset must be sector-aligned (VLog Append guarantees this).
    // value_len is the unpadded value size stored in IndexEntry.ValueSize.
    VLogReadResult ReadSync(uint64_t offset, uint32_t value_len);

    // BatchRead submits `count` reads and blocks until all CQEs arrive.
    // offsets[i] / value_lens[i] map to results[i].
    // results must be pre-allocated to `count` entries.
    void BatchRead(
        const uint64_t* offsets,
        const uint32_t* value_lens,
        VLogReadResult* results,
        size_t          count
    );

    int fd() const noexcept { return fd_; }

private:
    static constexpr unsigned kRingDepth = 256; // SQ / CQ entry count

    int        fd_;
    io_uring   ring_;
    bool       ring_ok_{false};

    // Aligned buffer pool for O_DIRECT reads.  Each slot holds one sector-
    // aligned buffer sized to the largest record we expect to read in a batch.
    // For 128 B values: header(24) + value(128) + pad → 512 B per slot.
    // We pre-allocate kRingDepth slots so every in-flight read has its own buf.
    struct AlignedBuf {
        void*  ptr{nullptr};
        size_t cap{0};
        AlignedBuf() = default;
        explicit AlignedBuf(size_t sz);
        ~AlignedBuf();
        AlignedBuf(AlignedBuf&&) noexcept;
        AlignedBuf& operator=(AlignedBuf&&) noexcept;
    };

    std::vector<AlignedBuf> bufs_;

    // Decode a completed read buffer into a VLogReadResult.
    VLogReadResult decode(const AlignedBuf& buf, int intra_block, uint32_t value_len, int res);
};

// ── VLogWriter ────────────────────────────────────────────────────────────────
//
// C++ side of the append path.  The Go VLog::Append is the authoritative write
// path; VLogWriter is used by C++ code that needs to write values directly
// (e.g., during VLog GC when copying live values to a new file).
//
class VLogWriter {
public:
    // fd must be opened with O_WRONLY|O_CREAT|O_APPEND|O_DIRECT.
    // write_offset is the byte offset of the first byte to write (set to the
    // current file size on construction).
    explicit VLogWriter(int fd, uint64_t write_offset = 0);
    ~VLogWriter() = default;

    // Append writes value to the VLog and returns the record's byte offset.
    // Returns -1 on I/O error.
    int64_t Append(const uint8_t* value, uint32_t value_len);

    // Flush calls fdatasync on the underlying fd.
    bool Flush();

    int64_t current_offset() const noexcept { return static_cast<int64_t>(offset_); }

private:
    int      fd_;
    uint64_t offset_; // current write head
};

// ── Per-disk VLog context ─────────────────────────────────────────────────────
//
// VLogContext bundles a reader + writer for one NVMe disk.  One VLogContext
// per disk is held by the C++ batch engine so reads and writes stay on the
// same NVMe queue without cross-disk coordination.
//
struct VLogContext {
    int disk_idx{-1};
    int read_fd{-1};  // O_RDONLY|O_DIRECT — used by VLogReader
    int write_fd{-1}; // O_WRONLY|O_DIRECT — used by VLogWriter

    std::unique_ptr<VLogReader> reader;
    std::unique_ptr<VLogWriter> writer;

    // open() initialises read_fd / write_fd and constructs reader / writer.
    // vlog_path must be the absolute path to "vlog_active.dat" on the disk.
    bool open(int disk_idx, const char* vlog_path);
    void close();
};

} // namespace veltrix

#endif // __linux__
