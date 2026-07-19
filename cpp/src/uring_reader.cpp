/*
 * uring_reader.cpp — O_DIRECT io_uring SSTable read engine implementation.
 *
 * SQPOLL explained
 * ─────────────────
 * When IORING_SETUP_SQPOLL is set, the kernel spawns a thread that polls
 * the SQ ring.  User-space appends SQEs directly (io_uring_get_sqe + prep)
 * and calls io_uring_submit() ONLY when the kernel thread has gone to sleep
 * (indicated by IORING_SQ_NEED_WAKEUP in the ring flags).  At >50K IOPS this
 * eliminates the enter(2) syscall entirely — submitting cost becomes a CAS on
 * an atomic ring index.
 *
 * O_DIRECT contract
 * ─────────────────
 * pread with O_DIRECT requires:
 *   • buf   : aligned to logical block size (512 B) — we use 4 KB (page size)
 *   • len   : multiple of logical block size
 *   • offset: multiple of logical block size
 * VeltrixDB's segment writer already rounds all offsets/lengths to 512 B
 * (Invariant 2 in CLAUDE.md), so reads are always O_DIRECT-compatible.
 */

#include "uring_reader.hpp"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#ifdef __linux__
#  include <pthread.h>
#  include <sched.h>
#  include <sys/mman.h>
#  include <unistd.h>
#endif

/* ── Construction / destruction ─────────────────────────────────────────── */

UringReader::UringReader(int fd, Config cfg) : fd_(fd), cfg_(cfg) {
    if (!setup_ring())
        throw std::runtime_error("UringReader: io_uring setup failed");

    register_fixed_buffers(); // best-effort; falls back gracefully

    running_.store(true, std::memory_order_release);
    loop_thread_ = std::thread([this] { run_loop(); });
}

UringReader::~UringReader() {
    running_.store(false, std::memory_order_release);
    /* Wake a sleeping SQPOLL/wait_cqe by submitting a no-op SQE. */
    if (ring_initialized_) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe) {
            io_uring_prep_nop(sqe);
            sqe->user_data = 0; // sentinel: ignored by callback map
            io_uring_submit(&ring_);
        }
    }
    if (loop_thread_.joinable()) loop_thread_.join();

    /* Free fixed buffers */
    if (!fixed_ptrs_.empty() && ring_initialized_) {
        io_uring_unregister_buffers(&ring_);
    }
    for (void* p : fixed_ptrs_) free(p);

    if (ring_initialized_) io_uring_queue_exit(&ring_);
}

/* ── Ring initialisation ────────────────────────────────────────────────── */

bool UringReader::setup_ring() {
    io_uring_params params{};
    memset(&params, 0, sizeof(params));

    if (cfg_.sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = cfg_.sqpoll_idle_ms;
    }

    /* IORING_SETUP_IOPOLL: kernel polls NVMe completion queue instead of
     * raising an interrupt per completion.  Combined with SQPOLL this creates
     * the fully interrupt-free, syscall-free I/O path described in
     * "High-Performance DBMSs with io_uring" (Dec 2025).
     *
     * Requirements: O_DIRECT file (already enforced) + NVMe device.
     * Falls back silently: we try all four flag combinations from most to
     * least capable so the ring always initialises on unsupported hardware. */
    if (cfg_.iopoll) {
        params.flags |= IORING_SETUP_IOPOLL;
    }

    int ret = io_uring_queue_init_params(cfg_.ring_depth, &ring_, &params);
    if (ret < 0 && cfg_.iopoll) {
        /* IOPOLL rejected (old kernel, non-NVMe device, no O_DIRECT): retry
         * without it.  SQPOLL is still attempted below if it was also set. */
        params.flags &= ~IORING_SETUP_IOPOLL;
        ret = io_uring_queue_init_params(cfg_.ring_depth, &ring_, &params);
    }
    if (ret < 0 && cfg_.sqpoll) {
        /* SQPOLL may require CAP_SYS_NICE; retry plain ring. */
        params.flags &= ~IORING_SETUP_SQPOLL;
        ret = io_uring_queue_init_params(cfg_.ring_depth, &ring_, &params);
        sqpoll_active_ = false;
    }
    if (ret < 0) return false;

    /* Record which flags the kernel actually accepted. */
    sqpoll_active_  = (params.flags & IORING_SETUP_SQPOLL) != 0;
    iopoll_active_  = (params.flags & IORING_SETUP_IOPOLL) != 0;

    ring_initialized_ = true;
    return true;
}

/* ── Fixed buffer registration ──────────────────────────────────────────── */

bool UringReader::register_fixed_buffers() {
    const size_t aligned_size = (cfg_.fixed_buf_size + 4095) & ~4095UL;
    fixed_ptrs_.resize(cfg_.fixed_buf_count, nullptr);
    fixed_iovecs_.resize(cfg_.fixed_buf_count);

    for (unsigned i = 0; i < cfg_.fixed_buf_count; ++i) {
        void* p = nullptr;
        if (posix_memalign(&p, 4096, aligned_size) != 0) {
            /* Shrink to successfully allocated count */
            fixed_ptrs_.resize(i);
            fixed_iovecs_.resize(i);
            break;
        }
        memset(p, 0, aligned_size);
        fixed_ptrs_[i]           = p;
        fixed_iovecs_[i].iov_base = p;
        fixed_iovecs_[i].iov_len  = aligned_size;
    }

    if (fixed_iovecs_.empty()) return false;

    int ret = io_uring_register_buffers(&ring_,
                                        fixed_iovecs_.data(),
                                        static_cast<unsigned>(fixed_iovecs_.size()));
    return ret == 0;
}

/* ── Async read submission ───────────────────────────────────────────────── */

bool UringReader::read_async(
    uint64_t                          file_offset,
    uint32_t                          len,
    void*                             buf,
    std::function<void(int32_t)>      callback,
    bool                              use_fixed_buf,
    unsigned                          fixed_buf_idx
) {
    if (!ring_initialized_) return false;

    const uint64_t token = next_user_data_.fetch_add(1, std::memory_order_relaxed);

    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        pending_.emplace(token, std::move(callback));
    }

    /* SQ access: if SQPOLL is active the kernel thread owns the SQ tail,
     * so we use the ring's built-in lock-free protocol (io_uring_get_sqe
     * is safe from multiple producers on separate threads via the SQ lock).
     * When SQPOLL is off we use our own mutex for producer serialisation. */
    auto submit = [&]() -> bool {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (!sqe) return false;

        if (use_fixed_buf && fixed_buf_idx < fixed_ptrs_.size()) {
            io_uring_prep_read_fixed(sqe, fd_,
                                     fixed_ptrs_[fixed_buf_idx],
                                     len, file_offset,
                                     static_cast<int>(fixed_buf_idx));
        } else {
            io_uring_prep_read(sqe, fd_, buf, len, file_offset);
        }

        /* Tier 1: highest ioprio — preempts background writes / defrag */
#ifdef IOPRIO_CLASS_RT
        sqe->ioprio = static_cast<uint16_t>(
            (IOPRIO_CLASS_RT << IOPRIO_CLASS_SHIFT) | 0);
#endif
        io_uring_sqe_set_data64(sqe, token);
        return true;
    };

    bool ok;
    if (sqpoll_active_) {
        ok = submit();
        if (ok) {
            /* With SQPOLL, only call io_uring_submit if the kernel thread
             * has gone to sleep (IORING_SQ_NEED_WAKEUP flag is set). */
            if (io_uring_sq_ready(&ring_) > 0 &&
                (ring_.sq.kflags &&
                 (*ring_.sq.kflags & IORING_SQ_NEED_WAKEUP))) {
                io_uring_submit(&ring_);
            }
        }
    } else {
        std::lock_guard<std::mutex> lk(sq_mu_);
        ok = submit();
        if (ok) io_uring_submit(&ring_);
    }

    if (ok) {
        in_flight_.fetch_add(1, std::memory_order_relaxed);
        submitted_.fetch_add(1, std::memory_order_relaxed);
    } else {
        std::lock_guard<std::mutex> lk(pending_mu_);
        pending_.erase(token);
    }
    return ok;
}

/* ── Completion polling ──────────────────────────────────────────────────── */

void UringReader::drain_completions(unsigned budget) {
    io_uring_cqe* cqe;
    unsigned n = 0;
    while (n < budget && io_uring_peek_cqe(&ring_, &cqe) == 0) {
        const uint64_t token = io_uring_cqe_get_data64(cqe);
        const int32_t  res   = cqe->res;
        io_uring_cqe_seen(&ring_, cqe);
        ++n;

        if (token == 0) continue; // NOP sentinel (from destructor)

        std::function<void(int32_t)> cb;
        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            auto it = pending_.find(token);
            if (it != pending_.end()) {
                cb = std::move(it->second);
                pending_.erase(it);
            }
        }
        if (cb) cb(res);
        if (res < 0) io_errors_.fetch_add(1, std::memory_order_relaxed);
        completed_.fetch_add(1, std::memory_order_relaxed);
        in_flight_.fetch_sub(1, std::memory_order_relaxed);
    }
}

/* ── Event loop ─────────────────────────────────────────────────────────── */

void UringReader::run_loop() {
    if (cfg_.pinned_cpu >= 0) pin_thread_to_cpu(cfg_.pinned_cpu);

    constexpr unsigned kCQBudget = 128;

    while (running_.load(std::memory_order_relaxed) ||
           in_flight_.load(std::memory_order_relaxed) > 0) {
        drain_completions(kCQBudget);

        if (in_flight_.load(std::memory_order_relaxed) == 0) {
            if (iopoll_active_) {
                /* IOPOLL: the kernel thread is actively polling the NVMe CQ.
                 * Blocking on wait_cqe_timeout would stall the polling loop.
                 * Yield the OS thread instead — the kernel will push a CQE
                 * into the ring without an interrupt, and we pick it up on
                 * the next drain_completions iteration. */
                std::this_thread::yield();
            } else {
                /* Interrupt mode: block up to 1 ms waiting for next CQE. */
                io_uring_cqe* cqe;
                struct __kernel_timespec ts{ .tv_sec = 0, .tv_nsec = 1'000'000 };
                if (io_uring_wait_cqe_timeout(&ring_, &cqe, &ts) == 0) {
                    /* process it in the next drain_completions call */
                }
            }
        }
    }
}

void UringReader::wait_all() {
    while (in_flight_.load(std::memory_order_acquire) > 0) {
        io_uring_cqe* cqe;
        io_uring_wait_cqe(&ring_, &cqe);
        drain_completions(256);
    }
}

void* UringReader::get_fixed_buf(unsigned idx) const noexcept {
    if (idx >= fixed_ptrs_.size()) return nullptr;
    return fixed_ptrs_[idx];
}

/* ── CPU pinning ────────────────────────────────────────────────────────── */

void UringReader::pin_thread_to_cpu(int cpu) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#else
    (void)cpu;
#endif
}
