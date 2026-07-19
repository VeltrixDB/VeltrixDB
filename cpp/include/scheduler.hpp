#pragma once

#include "art.hpp"
#include "ebpf_gc_throttle.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <vector>
#include <sys/uio.h>  // struct iovec (for fixed buffer registration)

// Forward-declare liburing types to avoid pulling in the full header here.
struct io_uring;
struct io_uring_sqe;

namespace veltrix {

// ── Task Priority Tiers ───────────────────────────────────────────────────────
//
// The scheduler enforces strict tier ordering using a min-heap keyed on
// TaskTier (lower value = higher urgency).  A Tier 3 (defrag) task is only
// submitted when no Tier 1 or Tier 2 tasks are waiting, so background
// work never competes with client I/O for SQ slots.
//
// Tier 1 — Client Reads   : lowest latency budget; served first
// Tier 2 — Client Writes  : batched to amortise NVMe write overhead
// Tier 3 — Defragmentation: runs only when the ring is otherwise idle

enum class TaskTier : uint8_t {
    READ   = 0, // Tier 1 — highest priority
    WRITE  = 1, // Tier 2
    DEFRAG = 2, // Tier 3 — lowest priority
};

// ── I/O Task ─────────────────────────────────────────────────────────────────
//
// Represents one logical operation to be submitted to io_uring.
// The scheduler fills an SQE from this descriptor and tracks the CQE
// via `user_data` (unique 64-bit token).
//
// `on_complete` is fired on the shard's event-loop thread when the CQE
// arrives — no locking needed.

struct IoTask {
    TaskTier tier{TaskTier::READ};

    // ── I/O parameters ────────────────────────────────────────────────────────
    enum class OpCode : uint8_t { PREAD, PWRITE, FSYNC, NOP } op{OpCode::NOP};

    int      fd{-1};            // file descriptor
    void*    buf{nullptr};      // I/O buffer (must be 4096-byte aligned for O_DIRECT)
    uint32_t len{0};            // byte count
    uint64_t file_offset{0};    // byte offset in the file

    // When true the SQE has IOSQE_IO_LINK set, chaining it to the next SQE
    // in the same batch.  The last SQE in a write chain must have link=false
    // so io_uring knows where the chain ends.
    bool     link_to_next{false};

    // When true, use registered fixed-buffer I/O (IORING_OP_READ_FIXED /
    // IORING_OP_WRITE_FIXED).  buf must point into one of the registered
    // buffers returned by get_fixed_buf().
    bool     use_fixed_buf{false};
    unsigned fixed_buf_idx{0};       // index into the registered iovec array

    // ── Completion callback ────────────────────────────────────────────────────
    // res > 0  → bytes transferred
    // res == 0 → EOF / NOP complete
    // res < 0  → -errno
    std::function<void(int32_t res)> on_complete;

    // Filled by the scheduler; used to match SQE → CQE.
    uint64_t user_data{0};

};

// ── Scheduler Configuration ───────────────────────────────────────────────────

struct SchedulerConfig {
    unsigned io_uring_depth{256};        // SQ/CQ ring depth
    unsigned max_submit_batch{64};       // max SQEs submitted per loop iteration
    unsigned cq_poll_budget{128};        // max CQEs drained per iteration
    bool     sq_poll{false};             // enable IORING_SETUP_SQPOLL (kernel SQ thread)
    uint32_t sq_poll_idle_ms{2000};      // SQPOLL idle timeout before sleeping (ms)
    int      pinned_cpu{-1};             // CPU core to pin scheduler thread to (-1 = none)

    // ── Write-batch group commit ──────────────────────────────────────────────
    //
    // Tier 2 (write) SQEs are held in a staging queue until one of two
    // conditions is met, then submitted together in a single io_uring_submit:
    //
    //   1. write_batch_limit  writes have accumulated  (count threshold)
    //   2. write_batch_window_us microseconds have elapsed since the first
    //      write in the current batch arrived  (time threshold)
    //
    // Group-committing writes reduces io_uring_submit syscall frequency
    // (one submit per batch instead of one per write) and amortises the
    // io_uring submission overhead across all writers that arrived during
    // the window.  Analogous to WAL group commit in the Go layer.
    unsigned write_batch_limit{32};       // max writes per batch (count trigger)
    uint32_t write_batch_window_us{1000}; // time trigger in microseconds (1 ms)
};

// ── Priority Scheduler ────────────────────────────────────────────────────────
//
// Owns an io_uring ring and a three-tier priority queue of IoTasks.
// The event loop runs on a dedicated thread (start() / stop()).
//
// Submission rules:
//   1. Drain Tier 1 (Reads) completely before touching Tier 2 or 3.
//   2. Drain Tier 2 (Writes) in batches.  When multiple writes share a batch,
//      all but the last are submitted with IOSQE_IO_LINK so that io_uring
//      guarantees write ordering without blocking the CPU.
//   3. Only submit Tier 3 (Defrag) when Tiers 1 and 2 are empty.
//
// io_uring SQE flags used:
//   IOSQE_IO_LINK  — chains SQEs so that a later SQE doesn't start until
//                    the preceding one completes.  Used to enforce write
//                    ordering within a batch without an explicit fsync.
//   IOSQE_ASYNC    — forces async execution even for operations that
//                    would normally complete synchronously (e.g., read on
//                    a hot page).  Applied to Tier 3 work to avoid stalling
//                    the ring on potentially slow compaction reads.
//
// Thread safety:
//   enqueue() is the only function safe to call from the shard thread while
//   the scheduler is running.  It uses a lock-free MPSC queue internally.
//   All other methods must be called before start() or after stop().

class PriorityScheduler {
public:
    explicit PriorityScheduler(int segment_fd, SchedulerConfig cfg = {});
    ~PriorityScheduler();

    PriorityScheduler(const PriorityScheduler&)            = delete;
    PriorityScheduler& operator=(const PriorityScheduler&) = delete;

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    void start(); // spawn the event-loop thread
    void stop();  // signal stop, drain ring, join thread

    // ── Task submission ───────────────────────────────────────────────────────

    // Enqueue a task.  Thread-safe: may be called from any thread.
    // The task is placed into the appropriate tier queue.
    // Returns a user_data token that identifies the pending operation.
    uint64_t enqueue(IoTask task);

    // ── Convenience builders ───────────────────────────────────────────────────
    //
    // High-level helpers that construct IoTask and call enqueue().

    // Submit an async read (Tier 1).
    uint64_t read_async(int fd,
                        void* buf, uint32_t len,
                        uint64_t file_offset,
                        std::function<void(int32_t)> on_complete);

    // Submit an async write (Tier 2).
    // If `link` is true the SQE is linked to the next enqueued write SQE,
    // enforcing ordering without a full fsync.
    uint64_t write_async(int fd,
                         const void* buf, uint32_t len,
                         uint64_t file_offset,
                         bool link_to_next,
                         std::function<void(int32_t)> on_complete);

    // Submit a defragmentation read (Tier 3).
    uint64_t defrag_read_async(int fd,
                               void* buf, uint32_t len,
                               uint64_t file_offset,
                               std::function<void(int32_t)> on_complete);

    // Submit a defragmentation write (Tier 3).
    uint64_t defrag_write_async(int fd,
                                const void* buf, uint32_t len,
                                uint64_t file_offset,
                                std::function<void(int32_t)> on_complete);

    // ── Fixed buffer management ────────────────────────────────────────────────
    //
    // Call register_fixed_buffers() once after construction, before start().
    // Each buffer is kFixedBufSize bytes, page-aligned.
    // After registration, pass use_fixed_buf=true and fixed_buf_idx=N to
    // read_async / write_async to use IORING_OP_READ_FIXED / WRITE_FIXED.
    //
    // Returns true on success; false if the kernel rejected registration
    // (older kernel, permission issue, or already registered).
    bool register_fixed_buffers(unsigned buf_count, std::size_t buf_size);

    // Returns a pointer to the start of registered buffer buf_idx.
    // Returns nullptr if fixed buffers are not registered or idx is out of range.
    [[nodiscard]] void* get_fixed_buf(unsigned buf_idx) const noexcept;

    // Size of each fixed buffer in bytes (0 if not registered).
    [[nodiscard]] std::size_t fixed_buf_size() const noexcept {
        return fixed_buf_size_;
    }

    // Number of registered fixed buffers (0 if not registered).
    [[nodiscard]] unsigned fixed_buf_count() const noexcept {
        return static_cast<unsigned>(fixed_iovecs_.size());
    }

    uint64_t read_fixed_async(int fd,
                               void* buf, uint32_t len,
                               uint64_t file_offset,
                               unsigned buf_idx,
                               std::function<void(int32_t)> on_complete);

    uint64_t write_fixed_async(int fd,
                                const void* buf, uint32_t len,
                                uint64_t file_offset,
                                unsigned buf_idx,
                                bool link_to_next,
                                std::function<void(int32_t)> on_complete);

    // ── Kernel GC bandwidth enforcement ──────────────────────────────────────
    //
    // KernelGCThrottle moves bandwidth accounting for Tier 3 (defrag) I/O into
    // the kernel via cgroup v2 blkio, replacing the Go-side token bucket in
    // defrag.go.  Call init_gc_throttle() once before start(); call
    // set_gc_write_limit() to adjust the cap at runtime as gcRatio changes.
    // Falls back silently if cgroup v2 is not available.
    bool init_gc_throttle(KernelGCThrottleConfig cfg = {});
    bool set_gc_write_limit(uint64_t write_bps);
    bool gc_throttle_active() const noexcept;

    // Enroll the calling thread (defrag OS thread) into the GC cgroup.
    // Must be called from within a goroutine that has called LockOSThread.
    bool enroll_defrag_thread();

    // ── Stats ──────────────────────────────────────────────────────────────────

    // Number of Tier 2 writes currently staged in write_queue_ or in-flight.
    // Producers increment this in enqueue(); the scheduler decrements it when
    // each write SQE is submitted.  Useful for back-pressure monitoring.
    [[nodiscard]] int pending_writes() const noexcept {
        return write_pending_count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t submitted()     const noexcept {
        return submitted_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t completed()     const noexcept {
        return completed_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t tier1_ops()     const noexcept {
        return tier1_ops_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t tier2_ops()     const noexcept {
        return tier2_ops_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t tier3_ops()     const noexcept {
        return tier3_ops_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t io_errors()     const noexcept {
        return io_errors_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] std::size_t pending()    const noexcept;

private:
    // ── Internal event loop ───────────────────────────────────────────────────

    void run_loop();

    // Drain the external MPSC inbox into the per-tier local queues.
    void drain_inbox();

    // Submit up to cfg_.max_submit_batch SQEs, respecting tier ordering.
    // Returns the number of SQEs actually submitted.
    unsigned submit_batch();

    // Fill an SQE from a task descriptor.
    void fill_sqe(io_uring_sqe* sqe, const IoTask& task) const noexcept;

    // Drain available CQEs; fire on_complete callbacks.
    // Returns the number of CQEs processed.
    unsigned poll_completions();

    // Assign a unique user_data token and register the callback.
    uint64_t register_task(IoTask& task);

    // Retrieve and erase a pending callback by user_data.
    std::function<void(int32_t)> pop_callback(uint64_t user_data);

    // Pin calling thread to cfg_.pinned_cpu (Linux only; no-op on others).
    void pin_thread() const noexcept;

    // ── Lock-free MPSC inbox ─────────────────────────────────────────────────
    // Producers (shard threads) push into `inbox_` under a spinlock.
    // The scheduler loop drains it without holding any external lock.
    mutable std::atomic_flag inbox_lock_ = ATOMIC_FLAG_INIT;
    std::vector<IoTask>      inbox_;

    // ── Per-tier queues (scheduler-thread only, no lock needed) ───────────────
    //
    // Three separate FIFO queues replace the earlier single priority_queue.
    // Separation gives explicit control over when each tier is submitted:
    //   read_queue_   — drained first (strict priority, no batching)
    //   write_queue_  — held until batch_limit or batch_window expires
    //   defrag_queue_ — submitted only when both higher tiers are empty
    std::deque<IoTask> read_queue_;
    std::deque<IoTask> write_queue_;
    std::deque<IoTask> defrag_queue_;

    // ── Write-batch group commit state (scheduler-thread only) ─────────────────
    //
    // write_pending_count_ is also written by producer threads (in enqueue)
    // to let callers poll how many writes are outstanding without locking.
    std::atomic<int>                           write_pending_count_{0};
    std::chrono::steady_clock::time_point      write_batch_start_{};
    bool                                       write_batch_timing_{false};

    // ── Pending completion map ─────────────────────────────────────────────────
    // user_data → on_complete callback.
    // The scheduler thread is the sole reader/writer so no lock is needed.
    std::vector<std::pair<uint64_t, std::function<void(int32_t)>>> pending_cbs_;

    // ── io_uring ring ─────────────────────────────────────────────────────────
    io_uring*   ring_{nullptr};

    // ── Config & state ────────────────────────────────────────────────────────
    int              segment_fd_;
    SchedulerConfig  cfg_;
    std::atomic<bool> running_{false};
    std::thread      thread_;
    uint64_t         next_user_data_{1}; // monotonically increasing token

    // ── Fixed buffer state ────────────────────────────────────────────────────
    std::vector<struct iovec> fixed_iovecs_;   // registered with io_uring
    std::vector<void*>        fixed_buf_ptrs_; // raw pointers for get_fixed_buf
    std::size_t               fixed_buf_size_{0};

    // ── Kernel GC throttle (optional, Linux cgroup v2 blkio) ─────────────────
#ifdef __linux__
    std::unique_ptr<KernelGCThrottle> gc_throttle_;
#endif

    // ── Counters ──────────────────────────────────────────────────────────────
    std::atomic<uint64_t> submitted_{0};
    std::atomic<uint64_t> completed_{0};
    std::atomic<uint64_t> tier1_ops_{0};
    std::atomic<uint64_t> tier2_ops_{0};
    std::atomic<uint64_t> tier3_ops_{0};
    std::atomic<uint64_t> io_errors_{0};
};

} // namespace veltrix
