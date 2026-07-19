#pragma once
#include "shard.hpp"
#include <coroutine>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

// liburing forward declaration (included in .cpp only).
struct io_uring;
struct io_uring_sqe;
struct io_uring_cqe;

namespace veltrix {

// ── C++20 Coroutine Write Path ────────────────────────────────────────────────
//
// The async write path uses C++20 coroutines to express asynchronous io_uring
// submissions as if they were synchronous code — without callbacks or state
// machines.
//
// Execution model:
//   1. The shard's event loop calls async_write(...).resume() to start.
//   2. The coroutine packs the record, submits an io_uring SQE, then
//      co_awaits IoUringWriteAwaitable — this suspends the coroutine and
//      returns control to the event loop.
//   3. When the event loop's CQE poller calls cqe_poller_tick(), it finds
//      the CQE, stores the result in the awaitable, and resumes the coroutine.
//   4. The coroutine reads the result, updates the IndexEntry's disk offset,
//      and returns.

// ── IoUringWriteAwaitable ─────────────────────────────────────────────────────
//
// An awaitable that bridges co_await to io_uring.  When await_suspend fires,
// it submits the SQE and encodes the coroutine handle into io_uring user_data
// so the CQE poller can resume it.

struct IoUringWriteAwaitable {
    io_uring*        ring;
    int              fd;
    const void*      buf;
    std::size_t      len;
    uint64_t         file_offset;
    int              result{0};  // set by the CQE poller before resuming

    // Never ready immediately; always suspend.
    [[nodiscard]] bool await_ready() const noexcept { return false; }

    // Submit the SQE and encode the coroutine handle in user_data.
    void await_suspend(std::coroutine_handle<> handle) noexcept;

    // Return the io_uring completion result (bytes written, or -errno).
    [[nodiscard]] int await_resume() const noexcept { return result; }
};

// ── AsyncWriteTask ────────────────────────────────────────────────────────────
//
// A minimal "lazy" coroutine task type.  The coroutine does not start until
// the caller calls .resume() for the first time (initial_suspend returns
// std::suspend_always).
//
// Lifecycle:
//   auto task = async_write(shard, key, value, ttl);
//   task.resume();          // starts coroutine; runs until first co_await
//   // ... event loop ticks ...
//   // CQE arrives; poller resumes the handle stored in user_data.
//   // Coroutine runs to completion.
//   assert(task.done());

class [[nodiscard]] AsyncWriteTask {
public:
    struct promise_type {
        AsyncWriteTask get_return_object() noexcept {
            return AsyncWriteTask{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend()   noexcept { return {}; }
        void return_void()    noexcept {}
        void unhandled_exception() noexcept { std::terminate(); }
    };

    explicit AsyncWriteTask(std::coroutine_handle<promise_type> h) noexcept
        : handle_(h) {}

    AsyncWriteTask(AsyncWriteTask&& other) noexcept
        : handle_(std::exchange(other.handle_, {})) {}

    AsyncWriteTask(const AsyncWriteTask&)            = delete;
    AsyncWriteTask& operator=(const AsyncWriteTask&) = delete;

    ~AsyncWriteTask() {
        if (handle_) handle_.destroy();
    }

    void resume() {
        if (handle_ && !handle_.done()) handle_.resume();
    }

    [[nodiscard]] bool done()  const noexcept { return !handle_ || handle_.done(); }
    [[nodiscard]] bool valid() const noexcept { return static_cast<bool>(handle_); }

private:
    std::coroutine_handle<promise_type> handle_;
};

// ── async_write ───────────────────────────────────────────────────────────────
//
// Coroutine: pack + submit + co_await CQE + update IndexEntry.
//
// Usage:
//   auto task = async_write(shard, "user:123", value_bytes, /*ttl=*/3600);
//   task.resume();   // start coroutine, submits io_uring SQE
//   // ... in event loop ...
//   cqe_poller_tick(ring);  // on CQE: resumes coroutine, updates IndexEntry
AsyncWriteTask async_write(
    Shard&                   shard,
    std::string_view         key,
    std::span<const uint8_t> value,
    uint16_t                 ttl_seconds = 0);

// ── CQE poller ────────────────────────────────────────────────────────────────
//
// Call from the shard's event loop on every iteration.  For each available
// CQE:
//   1. Extract result (bytes written or -errno).
//   2. Load the coroutine_handle from user_data.
//   3. Write result into the awaitable and resume the coroutine.
//
// Processes up to max_batch CQEs per tick to bound latency.
void cqe_poller_tick(io_uring* ring, unsigned max_batch = 64);

} // namespace veltrix
