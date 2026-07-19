#pragma once
/*
 * storage_bridge.hpp — 8-ring io_uring write bridge for VeltrixDB.
 *
 * Architecture
 * ────────────
 * StorageBridge owns one io_uring ring per NVMe disk (up to kMaxDisks=8).
 * Each ring is initialised with IORING_SETUP_SQPOLL so the kernel polls the
 * SQ without a syscall on the hot path; the SQ kernel thread is pinned to a
 * caller-specified CPU via IORING_SETUP_SQ_AFF to avoid scheduler migration.
 *
 * A fixed buffer pool (posix_memalign, 4 KB page-aligned) is registered with
 * io_uring_register_buffers per ring.  The kernel maps these buffers once at
 * registration time and reuses the mapping for every subsequent write, cutting
 * out the per-I/O DMA-map/unmap round-trip.
 *
 * SubmitBatch()
 * ─────────────
 * Accepts up to kBridgeBatchLimit (1024) WriteRequests in a single call.
 *
 *   1. Groups requests by disk_idx (shardID % numDisks).
 *   2. For each disk, prepares SQEs:
 *        – io_uring_prep_write_fixed when buf_idx != kNoFixedBuf and the
 *          buf_idx references one of the registered fixed buffers.
 *        – io_uring_prep_write otherwise (fallback, no DMA mapping benefit).
 *      Consecutive write SQEs within a disk batch are linked with
 *      IOSQE_IO_LINK to enforce NVMe write ordering without an fsync SQE.
 *   3. Issues exactly one io_uring_submit per disk ring to dispatch the whole
 *      batch in one syscall.
 *   4. Collects all CQEs (blocking) and fires on_complete callbacks inline.
 *
 * Shard routing invariant (must match Go storage/engine.go)
 * ─────────────────────────────────────────────────────────
 *   diskIdx = shardID % numDisks
 *
 * Thread safety
 * ─────────────
 * SubmitBatch is NOT thread-safe.  The Go layer serialises all calls to
 * SubmitBatch through the per-batcher flush goroutine; concurrent callers
 * must use separate StorageBridge instances.
 *
 * Linux only — io_uring is not available on macOS / Windows.
 */

#ifdef __linux__

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <sys/uio.h>
#include <vector>

// Forward-declare to avoid pulling in liburing.h in every translation unit.
struct io_uring;

namespace veltrix {

// ── Constants ─────────────────────────────────────────────────────────────────

static constexpr int      kMaxDisks          = 8;
static constexpr size_t   kBridgeBatchLimit   = 1024;
static constexpr int      kBridgeRingDepth    = 1024; // SQ/CQ slots per ring
static constexpr size_t   kBridgeFixedBufSize = 4096; // 4 KB, O_DIRECT aligned
static constexpr size_t   kBridgeFixedBufsN   = 256;  // fixed buffers per ring
// Sentinel value for WriteRequest::buf_idx meaning "not a fixed buffer".
static constexpr uint32_t kNoFixedBuf         = UINT32_MAX;

// ── WriteRequest ──────────────────────────────────────────────────────────────
//
// One write operation to be batched.
//
// Fixed-buffer writes (zero-copy DMA mapping):
//   1. Obtain a buffer:  ptr = bridge.get_fixed_buf(disk_idx, buf_idx)
//   2. Copy payload:     memcpy(ptr, payload, len)
//   3. Set fields:       req.data = ptr; req.buf_idx = buf_idx
//
// Fallback writes (data is any aligned pointer):
//   Set buf_idx = kNoFixedBuf; data = any 512-byte-aligned pointer.
//   The write goes through io_uring_prep_write — correct but the kernel must
//   re-map the buffer on every call.

struct WriteRequest {
    int          disk_idx{-1};       // target disk  (0 … numDisks-1)
    int          fd{-1};             // file descriptor (VLog or Segment file)
    uint64_t     file_offset{0};     // byte offset in the file
    const void*  data{nullptr};      // buffer to write (512-byte-aligned)
    uint32_t     len{0};             // byte count (sector-multiple for O_DIRECT)
    uint32_t     buf_idx{kNoFixedBuf}; // registered buffer index or kNoFixedBuf

    // Fired on the calling thread after the CQE arrives.
    // res > 0  → bytes written
    // res < 0  → -errno
    std::function<void(int32_t res)> on_complete;
};

// ── StorageBridgeConfig ───────────────────────────────────────────────────────

struct StorageBridgeConfig {
    int      num_disks{kMaxDisks};
    int      ring_depth{kBridgeRingDepth};

    // SQPOLL: kernel thread polls the SQ, eliminating io_uring_enter on the
    // hot path at the cost of one kernel thread per ring (negligible on NVMe).
    bool     sq_poll{true};
    uint32_t sq_poll_idle_ms{2000};    // SQPOLL thread sleep after N ms idle

    // IOPOLL: kernel polls the NVMe completion queue instead of raising an
    // interrupt per completed I/O.  Combined with SQPOLL this creates the
    // fully interrupt-free, syscall-free write path (+21% throughput on NVMe,
    // per "High-Performance DBMSs with io_uring", Dec 2025).
    // Requires O_DIRECT (already enforced) and NVMe hardware.
    // Falls back silently if the kernel or device does not support it.
    bool     io_poll{true};

    // Per-disk CPU affinity for the SQPOLL kernel thread.
    // -1 = let the kernel choose (IORING_SETUP_SQ_AFF not set for that ring).
    int      sq_thread_cpu[kMaxDisks]{-1,-1,-1,-1,-1,-1,-1,-1};

    size_t   fixed_buf_size{kBridgeFixedBufSize};
    size_t   fixed_bufs_per_ring{kBridgeFixedBufsN};
};

// ── Per-disk ring state ───────────────────────────────────────────────────────

struct DiskRing {
    io_uring*             ring{nullptr};
    int                   disk_idx{-1};
    bool                  initialized{false};
    bool                  iopoll_active{false}; // IORING_SETUP_IOPOLL accepted by kernel

    // Fixed buffer pool registered with this ring.
    std::vector<iovec>    fixed_iovecs;
    std::vector<void*>    buf_ptrs;
    size_t                buf_size{0};

    // Monotonic counters (relaxed ordering — monitoring only).
    std::atomic<uint64_t> submits{0};
    std::atomic<uint64_t> completions{0};
    std::atomic<uint64_t> errors{0};

    DiskRing()  = default;
    ~DiskRing() = default;
    DiskRing(const DiskRing&)            = delete;
    DiskRing& operator=(const DiskRing&) = delete;
};

// ── StorageBridge ─────────────────────────────────────────────────────────────

class StorageBridge {
public:
    explicit StorageBridge(StorageBridgeConfig cfg = {});
    ~StorageBridge();

    StorageBridge(const StorageBridge&)            = delete;
    StorageBridge& operator=(const StorageBridge&) = delete;

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    // init() must be called once before SubmitBatch().
    // Returns false if any ring or fixed-buffer registration fails.
    bool init();
    void shutdown();

    // ── Fixed buffer pool ─────────────────────────────────────────────────────
    // Returns a pointer to fixed buffer buf_idx for disk disk_idx.
    // Returns nullptr on invalid indices or if not initialised.
    [[nodiscard]] void*  get_fixed_buf(int disk_idx, unsigned buf_idx) const noexcept;
    [[nodiscard]] size_t fixed_buf_size() const noexcept { return cfg_.fixed_buf_size; }
    [[nodiscard]] unsigned fixed_bufs_per_ring() const noexcept {
        return static_cast<unsigned>(cfg_.fixed_bufs_per_ring);
    }

    // ── Shard routing ─────────────────────────────────────────────────────────
    // Invariant: diskIdx = shardID % numDisks  (matches Go storage/engine.go).
    [[nodiscard]] int disk_for_shard(uint16_t shard_id) const noexcept {
        return static_cast<int>(shard_id % static_cast<uint16_t>(cfg_.num_disks));
    }

    // ── Batch write ───────────────────────────────────────────────────────────
    // Groups reqs by disk, submits a linked SQE chain per disk ring in one
    // io_uring_submit call, blocks until all CQEs are collected.
    // Returns the number of requests that completed with res > 0.
    int SubmitBatch(const WriteRequest* reqs, size_t count);

    // ── Stats ─────────────────────────────────────────────────────────────────
    [[nodiscard]] uint64_t total_submits()     const noexcept;
    [[nodiscard]] uint64_t total_completions() const noexcept;
    [[nodiscard]] uint64_t total_errors()      const noexcept;

private:
    bool init_ring(int disk_idx);
    void shutdown_ring(int disk_idx);
    bool register_fixed_buffers(int disk_idx);

    // Submit and collect one disk's slice of the batch.
    // reqs[0..count-1] must all have disk_idx == disk_idx_in.
    int submit_disk_batch(int disk_idx_in,
                          const WriteRequest* const* reqs,
                          size_t count);

    StorageBridgeConfig cfg_;
    DiskRing            rings_[kMaxDisks];
    bool                initialized_{false};
};

} // namespace veltrix

// ── C API (for CGO bridge) ────────────────────────────────────────────────────
//
// All functions are Linux-only.  The opaque handle wraps StorageBridge and
// is heap-allocated so that Go's CGO can hold a stable pointer across GC.

extern "C" {

// Opaque handle.
typedef struct VeltrixStorageBridge VeltrixStorageBridge;

// Per-request descriptor passed from Go.
typedef struct {
    int      disk_idx;      // target disk
    int      fd;            // file descriptor
    uint64_t file_offset;   // byte offset in file
    void*    data;          // buffer pointer (512-byte-aligned for O_DIRECT)
    uint32_t len;           // byte count (must be sector-multiple)
    uint32_t buf_idx;       // fixed buffer index; UINT32_MAX = regular write
    int32_t  result;        // filled by bridge: bytes written or -errno
} BridgeWriteRequest;

// Construction config.  Mirrors StorageBridgeConfig for CGO compatibility.
typedef struct {
    int      num_disks;
    int      ring_depth;
    int      sq_poll;            // non-zero = enable SQPOLL
    uint32_t sq_poll_idle_ms;
    int      sq_thread_cpu[8];   // per-disk SQ thread CPU (-1 = any)
    uint64_t fixed_buf_size;
    uint64_t fixed_bufs_per_ring;
} BridgeConfig;

// Create a bridge.  Returns nullptr on failure.
// Calls StorageBridge::init() internally; caller must not call init() again.
VeltrixStorageBridge* veltrix_bridge_create(BridgeConfig cfg);

// Drain in-flight ops, destroy rings, free memory.
void veltrix_bridge_destroy(VeltrixStorageBridge* bridge);

// Submit a batch of up to 1024 write requests.
// reqs[i].result is filled with bytes written (>0) or -errno on completion.
// Returns total successful completions.
int veltrix_bridge_submit_batch(VeltrixStorageBridge* bridge,
                                BridgeWriteRequest*   reqs,
                                size_t                count);

// Returns pointer to fixed buffer buf_idx for disk disk_idx.
// The caller copies the payload here before passing buf_idx in BridgeWriteRequest.
void* veltrix_bridge_get_fixed_buf(VeltrixStorageBridge* bridge,
                                   int                   disk_idx,
                                   unsigned              buf_idx);

// Size in bytes of each fixed buffer.
uint64_t veltrix_bridge_fixed_buf_size(const VeltrixStorageBridge* bridge);

// Number of fixed buffers per disk ring.
unsigned veltrix_bridge_fixed_bufs_per_ring(const VeltrixStorageBridge* bridge);

// Shard-to-disk mapping: returns shardID % numDisks.
int veltrix_bridge_disk_for_shard(const VeltrixStorageBridge* bridge,
                                  uint16_t                    shard_id);

// Aggregate counters.
uint64_t veltrix_bridge_total_submits(const VeltrixStorageBridge* bridge);
uint64_t veltrix_bridge_total_completions(const VeltrixStorageBridge* bridge);
uint64_t veltrix_bridge_total_errors(const VeltrixStorageBridge* bridge);

} // extern "C"

#endif // __linux__
