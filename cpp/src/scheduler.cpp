#include "scheduler.hpp"
#include "ebpf_gc_throttle.h"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <thread>

// liburing
#include <liburing.h>

#if defined(__linux__)
#  include <sched.h>  // sched_setaffinity

// Linux I/O priority constants (from <linux/ioprio.h>).
// Defined here to avoid pulling in kernel headers that may not be installed.
//   class 1 = RT  (real-time)   — lowest latency, preempts all other I/O
//   class 2 = BE  (best-effort) — normal weighted I/O
//   class 3 = IDLE               — runs only when no other I/O is pending
static constexpr uint16_t kIoPrioRead   = (1u << 13) | 0u; // RT  class, level 0
static constexpr uint16_t kIoPrioWrite  = (2u << 13) | 4u; // BE  class, level 4
static constexpr uint16_t kIoPrioDefrag = (3u << 13) | 0u; // IDLE class, level 0
#endif

namespace veltrix {

// ─────────────────────────────────────────────────────────────────────────────
// Construction / Destruction
// ─────────────────────────────────────────────────────────────────────────────

PriorityScheduler::PriorityScheduler(int segment_fd, SchedulerConfig cfg)
    : segment_fd_(segment_fd)
    , cfg_(cfg)
{
    ring_ = new io_uring{};

    io_uring_params params{};
    if (cfg_.sq_poll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = cfg_.sq_poll_idle_ms;
    }

    const int ret = io_uring_queue_init_params(
        cfg_.io_uring_depth, ring_, &params);
    if (ret < 0) {
        delete ring_;
        ring_ = nullptr;
        throw std::runtime_error("io_uring_queue_init_params failed: " +
                                 std::string(std::strerror(-ret)));
    }

    inbox_.reserve(256);
    pending_cbs_.reserve(cfg_.io_uring_depth * 2);
}

PriorityScheduler::~PriorityScheduler() {
    if (running_.load(std::memory_order_acquire)) stop();
    if (ring_) {
        // Unregister fixed buffers and free their memory.
        if (!fixed_iovecs_.empty()) {
            ::io_uring_unregister_buffers(ring_);
            for (void* p : fixed_buf_ptrs_) ::free(p);
            fixed_iovecs_.clear();
            fixed_buf_ptrs_.clear();
        }
        io_uring_queue_exit(ring_);
        delete ring_;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Lifecycle
// ─────────────────────────────────────────────────────────────────────────────

void PriorityScheduler::start() {
    if (running_.exchange(true, std::memory_order_acq_rel))
        return; // already running
    thread_ = std::thread([this] { run_loop(); });
}

void PriorityScheduler::stop() {
    running_.store(false, std::memory_order_release);
    if (thread_.joinable()) thread_.join();
}

// ─────────────────────────────────────────────────────────────────────────────
// register_fixed_buffers
// ─────────────────────────────────────────────────────────────────────────────

bool PriorityScheduler::register_fixed_buffers(unsigned buf_count,
                                                std::size_t buf_size) {
    if (buf_count == 0 || buf_size == 0) return false;
    if (!fixed_iovecs_.empty()) return false; // already registered

    // Round buf_size up to 4 KB page boundary for O_DIRECT compatibility.
    buf_size = (buf_size + 4095) & ~static_cast<std::size_t>(4095);

    fixed_iovecs_.resize(buf_count);
    fixed_buf_ptrs_.resize(buf_count);

    for (unsigned i = 0; i < buf_count; ++i) {
        void* ptr = nullptr;
        if (::posix_memalign(&ptr, 4096, buf_size) != 0) {
            // Allocation failed — unwind what we already allocated.
            for (unsigned j = 0; j < i; ++j) ::free(fixed_buf_ptrs_[j]);
            fixed_iovecs_.clear();
            fixed_buf_ptrs_.clear();
            fixed_buf_size_ = 0;
            return false;
        }
        ::memset(ptr, 0, buf_size);
        fixed_buf_ptrs_[i] = ptr;
        fixed_iovecs_[i].iov_base = ptr;
        fixed_iovecs_[i].iov_len  = buf_size;
    }

    const int ret = ::io_uring_register_buffers(
        ring_,
        fixed_iovecs_.data(),
        static_cast<unsigned>(fixed_iovecs_.size()));

    if (ret < 0) {
        // Kernel rejected registration (old kernel, RLIMIT_MEMLOCK, etc.).
        for (void* p : fixed_buf_ptrs_) ::free(p);
        fixed_iovecs_.clear();
        fixed_buf_ptrs_.clear();
        fixed_buf_size_ = 0;
        return false;
    }

    fixed_buf_size_ = buf_size;
    return true;
}

void* PriorityScheduler::get_fixed_buf(unsigned buf_idx) const noexcept {
    if (buf_idx >= fixed_buf_ptrs_.size()) return nullptr;
    return fixed_buf_ptrs_[buf_idx];
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixed-buffer convenience builders
// ─────────────────────────────────────────────────────────────────────────────

uint64_t PriorityScheduler::read_fixed_async(int fd,
                                               void* buf, uint32_t len,
                                               uint64_t file_offset,
                                               unsigned buf_idx,
                                               std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier          = TaskTier::READ;
    t.op            = IoTask::OpCode::PREAD;
    t.fd            = fd;
    t.buf           = buf;
    t.len           = len;
    t.file_offset   = file_offset;
    t.use_fixed_buf = true;
    t.fixed_buf_idx = buf_idx;
    t.on_complete   = std::move(cb);
    return enqueue(std::move(t));
}

uint64_t PriorityScheduler::write_fixed_async(int fd,
                                                const void* buf, uint32_t len,
                                                uint64_t file_offset,
                                                unsigned buf_idx,
                                                bool link_to_next,
                                                std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier          = TaskTier::WRITE;
    t.op            = IoTask::OpCode::PWRITE;
    t.fd            = fd;
    t.buf           = const_cast<void*>(buf);
    t.len           = len;
    t.file_offset   = file_offset;
    t.use_fixed_buf = true;
    t.fixed_buf_idx = buf_idx;
    t.link_to_next  = link_to_next;
    t.on_complete   = std::move(cb);
    return enqueue(std::move(t));
}

// ─────────────────────────────────────────────────────────────────────────────
// Thread pinning
// ─────────────────────────────────────────────────────────────────────────────

void PriorityScheduler::pin_thread() const noexcept {
#if defined(__linux__)
    if (cfg_.pinned_cpu < 0) return;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cfg_.pinned_cpu, &cpuset);
    ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// enqueue  (producer-side, thread-safe)
// ─────────────────────────────────────────────────────────────────────────────

uint64_t PriorityScheduler::enqueue(IoTask task) {
    // Increment write_pending_count_ before taking the inbox lock so that
    // callers can read pending_writes() without acquiring any lock.
    if (task.tier == TaskTier::WRITE) {
        write_pending_count_.fetch_add(1, std::memory_order_release);
    }

    while (inbox_lock_.test_and_set(std::memory_order_acquire))
        ; // spin (extremely short critical section — just a push)

    const uint64_t token = next_user_data_++;
    task.user_data = token;
    inbox_.push_back(std::move(task));

    inbox_lock_.clear(std::memory_order_release);
    return token;
}

// ─────────────────────────────────────────────────────────────────────────────
// Convenience builders
// ─────────────────────────────────────────────────────────────────────────────

uint64_t PriorityScheduler::read_async(int fd,
                                        void* buf, uint32_t len,
                                        uint64_t file_offset,
                                        std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier        = TaskTier::READ;
    t.op          = IoTask::OpCode::PREAD;
    t.fd          = fd;
    t.buf         = buf;
    t.len         = len;
    t.file_offset = file_offset;
    t.on_complete = std::move(cb);
    return enqueue(std::move(t));
}

uint64_t PriorityScheduler::write_async(int fd,
                                         const void* buf, uint32_t len,
                                         uint64_t file_offset,
                                         bool link_to_next,
                                         std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier         = TaskTier::WRITE;
    t.op           = IoTask::OpCode::PWRITE;
    t.fd           = fd;
    t.buf          = const_cast<void*>(buf); // io_uring pwrite takes void*
    t.len          = len;
    t.file_offset  = file_offset;
    t.link_to_next = link_to_next;
    t.on_complete  = std::move(cb);
    return enqueue(std::move(t));
}

uint64_t PriorityScheduler::defrag_read_async(int fd,
                                               void* buf, uint32_t len,
                                               uint64_t file_offset,
                                               std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier        = TaskTier::DEFRAG;
    t.op          = IoTask::OpCode::PREAD;
    t.fd          = fd;
    t.buf         = buf;
    t.len         = len;
    t.file_offset = file_offset;
    t.on_complete = std::move(cb);
    return enqueue(std::move(t));
}

uint64_t PriorityScheduler::defrag_write_async(int fd,
                                                const void* buf, uint32_t len,
                                                uint64_t file_offset,
                                                std::function<void(int32_t)> cb) {
    IoTask t;
    t.tier        = TaskTier::DEFRAG;
    t.op          = IoTask::OpCode::PWRITE;
    t.fd          = fd;
    t.buf         = const_cast<void*>(buf);
    t.len         = len;
    t.file_offset = file_offset;
    t.on_complete = std::move(cb);
    return enqueue(std::move(t));
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

void PriorityScheduler::drain_inbox() {
    // Swap the shared inbox with a local buffer to minimise lock hold time.
    std::vector<IoTask> local;
    while (inbox_lock_.test_and_set(std::memory_order_acquire))
        ;
    std::swap(local, inbox_);
    inbox_lock_.clear(std::memory_order_release);

    for (auto& task : local) {
        // Register the completion callback before routing to a tier queue.
        pending_cbs_.emplace_back(task.user_data, std::move(task.on_complete));
        task.on_complete = nullptr;

        switch (task.tier) {
        case TaskTier::READ:
            read_queue_.push_back(std::move(task));
            break;
        case TaskTier::WRITE:
            write_queue_.push_back(std::move(task));
            // Start the batch timer on the first write in a new batch.
            if (!write_batch_timing_) {
                write_batch_start_  = std::chrono::steady_clock::now();
                write_batch_timing_ = true;
            }
            break;
        case TaskTier::DEFRAG:
            defrag_queue_.push_back(std::move(task));
            break;
        }
    }
}

uint64_t PriorityScheduler::register_task(IoTask& task) {
    pending_cbs_.emplace_back(task.user_data, std::move(task.on_complete));
    task.on_complete = nullptr;
    return task.user_data;
}

std::function<void(int32_t)> PriorityScheduler::pop_callback(uint64_t user_data) {
    for (auto it = pending_cbs_.begin(); it != pending_cbs_.end(); ++it) {
        if (it->first == user_data) {
            auto cb = std::move(it->second);
            pending_cbs_.erase(it);
            return cb;
        }
    }
    return {};
}

void PriorityScheduler::fill_sqe(io_uring_sqe* sqe,
                                  const IoTask& task) const noexcept {
    switch (task.op) {
    case IoTask::OpCode::PREAD:
        if (task.use_fixed_buf) {
            io_uring_prep_read_fixed(sqe,
                                     task.fd,
                                     task.buf,
                                     task.len,
                                     static_cast<__u64>(task.file_offset),
                                     static_cast<int>(task.fixed_buf_idx));
        } else {
            io_uring_prep_read(sqe,
                               task.fd,
                               task.buf,
                               task.len,
                               static_cast<__u64>(task.file_offset));
        }
        break;
    case IoTask::OpCode::PWRITE:
        if (task.use_fixed_buf) {
            io_uring_prep_write_fixed(sqe,
                                      task.fd,
                                      task.buf,
                                      task.len,
                                      static_cast<__u64>(task.file_offset),
                                      static_cast<int>(task.fixed_buf_idx));
        } else {
            io_uring_prep_write(sqe,
                                task.fd,
                                task.buf,
                                task.len,
                                static_cast<__u64>(task.file_offset));
        }
        break;
    case IoTask::OpCode::FSYNC:
        io_uring_prep_fsync(sqe, task.fd, 0);
        break;
    case IoTask::OpCode::NOP:
        io_uring_prep_nop(sqe);
        break;
    }

    sqe->user_data = task.user_data;

    // ── SQE flags ──────────────────────────────────────────────────────────────
    //
    // IOSQE_IO_LINK: chain this SQE to the next in the submission batch so
    //   the NVMe controller sees write ordering without a blocking fsync SQE.
    //
    // IOSQE_ASYNC: force kernel-side async dispatch for Tier 3 compaction I/O
    //   so it never stalls the ring thread when a cache-hot page short-circuits.
    if (task.link_to_next)
        sqe->flags |= IOSQE_IO_LINK;

    if (task.tier == TaskTier::DEFRAG)
        sqe->flags |= IOSQE_ASYNC;

    // ── Per-SQE I/O priority ───────────────────────────────────────────────────
    //
    // sqe->ioprio carries the Linux I/O priority class + level into the kernel's
    // block-device scheduler (BFQ, mq-deadline, etc.).  This ensures that even
    // when the NVMe hardware queue is saturated, the kernel itself prefers
    // client reads (RT class) over writes (BE class 4) over defrag (IDLE).
    //
    // Without ioprio, the kernel treats all SQEs equally regardless of our
    // application-level tier ordering — writes submitted moments before a read
    // will block the read at the hardware queue level.
#if defined(__linux__)
    switch (task.tier) {
    case TaskTier::READ:   sqe->ioprio = kIoPrioRead;   break;
    case TaskTier::WRITE:  sqe->ioprio = kIoPrioWrite;  break;
    case TaskTier::DEFRAG: sqe->ioprio = kIoPrioDefrag; break;
    }
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// submit_batch  — three-phase submission with write group commit
//
// Phase 1 — Reads (Tier 1):
//   Drain read_queue_ completely before touching any other tier.
//   Reads are latency-sensitive; they must never wait behind a write batch.
//
// Phase 2 — Writes (Tier 2) — group commit:
//   Writes are *staged* in write_queue_ until one of:
//     (a) write_queue_.size() >= cfg_.write_batch_limit  (count threshold)
//     (b) elapsed time since first write >= cfg_.write_batch_window_us
//   Then all queued writes are submitted in a single io_uring_submit call,
//   with consecutive PWRITE SQEs linked via IOSQE_IO_LINK for NVMe ordering.
//   This matches the Go WAL group-commit pattern: N writers → 1 syscall.
//
// Phase 3 — Defrag (Tier 3):
//   Only submitted when both higher tiers are empty AND no ops are in-flight.
// ─────────────────────────────────────────────────────────────────────────────

unsigned PriorityScheduler::submit_batch() {
    using Clock = std::chrono::steady_clock;
    unsigned submitted = 0;
    const auto now = Clock::now();

    // ── Phase 1: Reads ────────────────────────────────────────────────────────
    while (submitted < cfg_.max_submit_batch && !read_queue_.empty()) {
        io_uring_sqe* sqe = io_uring_get_sqe(ring_);
        if (!sqe) break; // SQ full — poll CQEs first

        IoTask task = std::move(read_queue_.front());
        read_queue_.pop_front();

        fill_sqe(sqe, task);
        ++submitted;
        submitted_.fetch_add(1, std::memory_order_relaxed);
        tier1_ops_.fetch_add(1, std::memory_order_relaxed);
    }

    // ── Phase 2: Writes — group commit ────────────────────────────────────────
    if (!write_queue_.empty()) {
        const auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now - write_batch_start_).count();

        const bool window_expired = write_batch_timing_ &&
                                    (elapsed_us >= static_cast<long>(cfg_.write_batch_window_us));
        const bool batch_full     = write_queue_.size() >= cfg_.write_batch_limit;

        if (batch_full || window_expired) {
            // Flush: submit all staged writes as a linked SQE chain.
            while (submitted < cfg_.max_submit_batch && !write_queue_.empty()) {
                io_uring_sqe* sqe = io_uring_get_sqe(ring_);
                if (!sqe) break;

                IoTask task = std::move(write_queue_.front());
                write_queue_.pop_front();

                // Auto-link consecutive PWRITE SQEs so the NVMe controller
                // processes them in order without an explicit fdatasync SQE.
                if (!task.link_to_next &&
                    !write_queue_.empty() &&
                    write_queue_.front().op == IoTask::OpCode::PWRITE)
                {
                    task.link_to_next = true;
                }

                fill_sqe(sqe, task);
                ++submitted;
                submitted_.fetch_add(1, std::memory_order_relaxed);
                tier2_ops_.fetch_add(1, std::memory_order_relaxed);
                write_pending_count_.fetch_sub(1, std::memory_order_relaxed);
            }

            // Reset batch timer.  If writes remain (SQ was full), slide the
            // window forward so the next iteration flushes promptly.
            if (write_queue_.empty()) {
                write_batch_timing_ = false;
            } else {
                write_batch_start_ = now;
            }
        }
        // else: batch not ready yet — keep staging, loop again quickly
    }

    // ── Phase 3: Defrag ───────────────────────────────────────────────────────
    if (read_queue_.empty() && write_queue_.empty() && !defrag_queue_.empty()) {
        const uint64_t in_flight = submitted_.load(std::memory_order_relaxed) -
                                   completed_.load(std::memory_order_relaxed);
        if (in_flight == 0) {
            while (submitted < cfg_.max_submit_batch && !defrag_queue_.empty()) {
                io_uring_sqe* sqe = io_uring_get_sqe(ring_);
                if (!sqe) break;

                IoTask task = std::move(defrag_queue_.front());
                defrag_queue_.pop_front();

                fill_sqe(sqe, task);
                ++submitted;
                submitted_.fetch_add(1, std::memory_order_relaxed);
                tier3_ops_.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    // ── Single io_uring_submit for ALL phases ─────────────────────────────────
    if (submitted > 0) {
        const int ret = io_uring_submit(ring_);
        if (ret < 0) {
            submitted_.fetch_sub(submitted, std::memory_order_relaxed);
            io_errors_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    return submitted;
}

// ─────────────────────────────────────────────────────────────────────────────
// poll_completions  — drain CQEs and fire callbacks
// ─────────────────────────────────────────────────────────────────────────────

unsigned PriorityScheduler::poll_completions() {
    unsigned processed = 0;
    io_uring_cqe* cqe  = nullptr;

    while (processed < cfg_.cq_poll_budget) {
        // Non-blocking peek: returns -EAGAIN if CQ is empty.
        const int ret = io_uring_peek_cqe(ring_, &cqe);
        if (ret == -EAGAIN || !cqe) break;

        const uint64_t user_data = cqe->user_data;
        const int32_t  res       = cqe->res;
        io_uring_cqe_seen(ring_, cqe);

        completed_.fetch_add(1, std::memory_order_relaxed);
        ++processed;

        if (res < 0)
            io_errors_.fetch_add(1, std::memory_order_relaxed);

        // Fire the registered callback (on THIS thread — no extra locking).
        auto cb = pop_callback(user_data);
        if (cb) cb(res);
    }

    return processed;
}

// ─────────────────────────────────────────────────────────────────────────────
// run_loop  — the main scheduler event loop
//
// Loop iteration:
//   1. drain_inbox()      — move new tasks from shared inbox → tier queues
//   2. submit_batch()     — fill SQEs from highest-priority tier down
//   3. poll_completions() — drain CQEs, fire callbacks
//
// Backpressure:
//   If the SQ was full (submit_batch returned 0 but queue is non-empty),
//   we do a blocking wait on the next CQE before re-trying.  This prevents
//   a busy-spin that wastes a CPU core.
//
// Idle behaviour:
//   When both inbox and tier queues are empty AND no CQEs are pending, the
//   loop calls io_uring_wait_cqe_timeout() with a short timeout (1 ms) to
//   sleep without losing responsiveness.
// ─────────────────────────────────────────────────────────────────────────────

void PriorityScheduler::run_loop() {
    pin_thread();

    auto any_queued = [this]() noexcept {
        return !read_queue_.empty() || !write_queue_.empty() || !defrag_queue_.empty();
    };

    while (running_.load(std::memory_order_acquire) ||
           any_queued() ||
           submitted_.load(std::memory_order_relaxed) >
               completed_.load(std::memory_order_relaxed))
    {
        drain_inbox();

        const unsigned new_sqes = submit_batch();
        const unsigned new_cqes = poll_completions();

        // SQ full: we had work but couldn't fit it — block on next CQE.
        const bool sq_full_stall = (new_sqes == 0 && any_queued());

        // Truly idle: no queued work and nothing in flight.
        // Note: pending writes in write_queue_ (not yet flushed due to batch
        // window) count as queued, so we loop quickly while they accumulate.
        const bool completely_idle =
            (!any_queued() &&
             submitted_.load(std::memory_order_relaxed) ==
                 completed_.load(std::memory_order_relaxed));

        if (sq_full_stall) {
            // SQ is full: block until at least one CQE arrives to free a slot.
            io_uring_cqe* cqe = nullptr;
            const int ret = io_uring_wait_cqe(ring_, &cqe);
            if (ret == 0 && cqe) {
                const uint64_t user_data = cqe->user_data;
                const int32_t  res       = cqe->res;
                io_uring_cqe_seen(ring_, cqe);
                completed_.fetch_add(1, std::memory_order_relaxed);
                if (res < 0)
                    io_errors_.fetch_add(1, std::memory_order_relaxed);
                auto cb = pop_callback(user_data);
                if (cb) cb(res);
            }
        } else if (completely_idle && new_cqes == 0) {
            // Nothing in flight, nothing pending: sleep up to 1 ms.
            struct __kernel_timespec ts{ .tv_sec = 0, .tv_nsec = 1'000'000 };
            io_uring_cqe* cqe = nullptr;
            io_uring_wait_cqe_timeout(ring_, &cqe, &ts);
            if (cqe) {
                // A CQE arrived during the sleep (e.g., a late Tier 3 completion).
                const uint64_t user_data = cqe->user_data;
                const int32_t  res       = cqe->res;
                io_uring_cqe_seen(ring_, cqe);
                completed_.fetch_add(1, std::memory_order_relaxed);
                if (res < 0)
                    io_errors_.fetch_add(1, std::memory_order_relaxed);
                auto cb = pop_callback(user_data);
                if (cb) cb(res);
            }
        }
        // If new work was submitted or completed in this iteration, loop
        // immediately without sleeping — hot path stays tight.
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Kernel GC throttle wrappers
// ─────────────────────────────────────────────────────────────────────────────

bool PriorityScheduler::init_gc_throttle(KernelGCThrottleConfig cfg) {
#ifdef __linux__
    gc_throttle_ = std::make_unique<KernelGCThrottle>(std::move(cfg));
    return gc_throttle_->init();
#else
    (void)cfg;
    return false;
#endif
}

bool PriorityScheduler::set_gc_write_limit(uint64_t write_bps) {
#ifdef __linux__
    if (!gc_throttle_ || !gc_throttle_->initialized()) return false;
    return gc_throttle_->set_write_limit(write_bps);
#else
    (void)write_bps;
    return false;
#endif
}

bool PriorityScheduler::gc_throttle_active() const noexcept {
#ifdef __linux__
    return gc_throttle_ && gc_throttle_->initialized();
#else
    return false;
#endif
}

bool PriorityScheduler::enroll_defrag_thread() {
#ifdef __linux__
    if (!gc_throttle_ || !gc_throttle_->initialized()) return false;
    return gc_throttle_->enroll_current_thread();
#else
    return false;
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// pending()  — count of tasks not yet completed
// ─────────────────────────────────────────────────────────────────────────────

std::size_t PriorityScheduler::pending() const noexcept {
    // In-flight (submitted but no CQE yet) plus staged-but-not-yet-submitted.
    const uint64_t s = submitted_.load(std::memory_order_relaxed);
    const uint64_t c = completed_.load(std::memory_order_relaxed);
    const std::size_t in_flight = (s > c) ? static_cast<std::size_t>(s - c) : 0u;
    return in_flight +
           read_queue_.size()   +
           write_queue_.size()  +
           defrag_queue_.size();
}

} // namespace veltrix
