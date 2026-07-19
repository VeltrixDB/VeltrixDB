#pragma once
/*
 * uring_reader.hpp — O_DIRECT / io_uring SSTable read engine.
 *
 * Design goals
 * ────────────
 * • Zero-syscall submission via IORING_SETUP_SQPOLL: the kernel keeps a
 *   dedicated thread polling the submission queue.  Go threads call
 *   read_async() which appends to the SQ without a syscall; the kernel thread
 *   picks up SQEs automatically.
 *
 * • O_DIRECT bypass: all reads use O_DIRECT so data flows NVMe → CPU cache
 *   without touching the Linux page cache.  VeltrixDB maintains its own 256GB
 *   LIRS cache; double-caching wastes memory bandwidth and TLB entries.
 *
 * • Fixed registered buffers: io_uring_register_buffers() pre-maps I/O
 *   buffers with the kernel, eliminating per-read page-pin overhead.  The
 *   buffer pool is managed internally; callers receive pointers on completion.
 *
 * • Three completion tiers (matching PriorityScheduler): READ(0) > WRITE(1) > DEFRAG(2).
 *   UringReader is dedicated to Tier 0 (latency-sensitive SSTable lookups).
 */

#include <liburing.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

class UringReader {
public:
    struct Config {
        unsigned ring_depth;      // SQ/CQ depth; must be power of 2
        bool     sqpoll;          // IORING_SETUP_SQPOLL (zero-syscall submit)
        uint32_t sqpoll_idle_ms;  // ms before SQPOLL thread sleeps
        bool     iopoll;          // IORING_SETUP_IOPOLL (zero-interrupt completions, NVMe only)
        unsigned fixed_buf_count; // pre-registered I/O buffers
        size_t   fixed_buf_size;  // 512 KB each; must be 4 KB aligned
        int      pinned_cpu;      // CPU to pin the completion thread (-1 = none)

        Config()
            : ring_depth(256)
            , sqpoll(true)
            , sqpoll_idle_ms(1000)
            , iopoll(true)
            , fixed_buf_count(64)
            , fixed_buf_size(512 << 10)
            , pinned_cpu(-1)
        {}
    };

    /* Create a reader for the already-opened O_DIRECT file descriptor `fd`.
     * The caller retains ownership of fd; it must outlive the UringReader. */
    explicit UringReader(int fd, Config cfg = Config());
    ~UringReader();

    UringReader(const UringReader&)            = delete;
    UringReader& operator=(const UringReader&) = delete;

    /* Submit a read request.  Returns immediately (non-blocking when the SQ
     * has space).  `callback` is invoked from the completion thread with the
     * number of bytes read (>= 0) or a negative errno.
     *
     * `buf` must be:
     *   - 4 KB aligned (required by O_DIRECT)
     *   - at least `len` bytes in size
     *   - live until `callback` fires
     *
     * If use_fixed_buf is true, buf is interpreted as a fixed-buffer index
     * (0..fixed_buf_count-1); the pre-registered buffer at that index is used
     * instead of a caller-supplied pointer.  Call get_fixed_buf(idx) to
     * obtain the pointer.
     */
    bool read_async(
        uint64_t                          file_offset,
        uint32_t                          len,
        void*                             buf,
        std::function<void(int32_t)>      callback,
        bool                              use_fixed_buf = false,
        unsigned                          fixed_buf_idx = 0
    );

    /* Block until all in-flight reads complete.  Safe to call from any thread. */
    void wait_all();

    /* Return a pointer to pre-registered fixed buffer `idx`.
     * Returns nullptr if idx is out of range or buffers not registered. */
    void* get_fixed_buf(unsigned idx) const noexcept;

    /* Diagnostics */
    uint64_t submitted()     const noexcept { return submitted_.load(std::memory_order_relaxed); }
    uint64_t completed()     const noexcept { return completed_.load(std::memory_order_relaxed); }
    uint64_t io_errors()     const noexcept { return io_errors_.load(std::memory_order_relaxed); }
    bool     sqpoll_active() const noexcept { return sqpoll_active_; }
    bool     iopoll_active() const noexcept { return iopoll_active_; }

private:
    void run_loop();
    bool setup_ring();
    bool register_fixed_buffers();
    void drain_completions(unsigned budget);
    void pin_thread_to_cpu(int cpu);

    int    fd_;
    Config cfg_;

    io_uring  ring_{};
    bool      ring_initialized_{false};
    bool      sqpoll_active_{false};
    bool      iopoll_active_{false};

    /* Fixed buffer registry */
    std::vector<void*>  fixed_ptrs_;
    std::vector<iovec>  fixed_iovecs_;

    /* Completion thread */
    std::thread           loop_thread_;
    std::atomic<bool>     running_{false};
    std::atomic<uint64_t> in_flight_{0};

    /* Pending callback map: user_data → callback */
    std::unordered_map<uint64_t, std::function<void(int32_t)>> pending_;
    std::mutex                                                   pending_mu_;
    std::atomic<uint64_t>                                        next_user_data_{1};

    /* Counters */
    std::atomic<uint64_t> submitted_{0};
    std::atomic<uint64_t> completed_{0};
    std::atomic<uint64_t> io_errors_{0};

    /* SQ lock — needed only when SQPOLL is OFF (kernel thread owns SQ otherwise) */
    std::mutex sq_mu_;
};
