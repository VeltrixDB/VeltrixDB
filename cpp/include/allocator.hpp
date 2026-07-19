#pragma once
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <vector>
#include <stdexcept>

#if defined(__linux__)
#  include <sys/mman.h>   // mmap, munmap, MAP_HUGETLB, MAP_HUGE_2MB
#  include <cstring>      // memset (already included via other headers)
#endif

namespace veltrix {

// ── Segmented Memory Pool ─────────────────────────────────────────────────────
//
// A slab-style allocator that replaces per-allocation malloc/free.
//
// Motivation:
//   malloc() acquires a global lock and touches multiple cache lines.  In a
//   write-intensive database each new IndexEntry or cache node allocation
//   becomes a hidden serialisation point.  A per-shard pool eliminates both
//   the lock and the cache pollution.
//
// Design:
//   Memory is reserved in large, posix_memalign'd Segments (default 1 MB).
//   Sub-allocations use a bump pointer that moves forward in O(1).  When a
//   Segment fills up a new one is appended; the old Segment is kept alive
//   until release_all() is called (bulk deallocation).
//
//   O_DIRECT I/O requires 512-byte (or 4096-byte) aligned buffers.  The
//   default alignment of 4096 makes every allocation usable as a DMA buffer
//   directly — no extra copy needed before io_uring submission.
//
// Thread safety: NONE.  One SegmentedPool per shard; only the owning thread
//   ever calls allocate().

class SegmentedPool {
public:
    static constexpr std::size_t kDefaultSegmentBytes = 1u << 20; // 1 MB
    static constexpr std::size_t kDefaultAlignment    = 4096;     // page-aligned

    explicit SegmentedPool(
        std::size_t segment_bytes = kDefaultSegmentBytes,
        std::size_t alignment     = kDefaultAlignment)
        : segment_bytes_(segment_bytes)
        , alignment_(alignment)
    {
        grow();
    }

    ~SegmentedPool() {
        release_all();
    }

    SegmentedPool(const SegmentedPool&)            = delete;
    SegmentedPool& operator=(const SegmentedPool&) = delete;

    // Enable 2 MB hugepage allocation for future segments.
    // Call before the pool is heavily used (existing segments are not converted).
    // Requires Linux kernel >= 3.8 and /proc/sys/vm/nr_hugepages > 0.
    // Falls back silently to posix_memalign if hugepages are unavailable.
    void enable_hugepages(bool enable = true) noexcept {
        use_hugepages_ = enable;
    }

    [[nodiscard]] bool hugepages_enabled() const noexcept {
        return use_hugepages_;
    }

    // ── Allocation ─────────────────────────────────────────────────────────────

    // Allocate sz bytes, rounded up to alignment_.  Complexity: O(1) amortised.
    // Throws std::bad_alloc if a new segment cannot be obtained from the OS.
    [[nodiscard]] void* allocate(std::size_t sz) {
        sz = align_up(sz, alignment_);
        if (cursor_ + sz > segment_bytes_) {
            grow();
        }
        void* ptr = static_cast<char*>(current_) + cursor_;
        cursor_ += sz;
        ++live_allocs_;
        return ptr;
    }

    // Construct a T in the pool.  The destructor is NOT called on reset/release;
    // use this only for trivially destructible types, or call the destructor
    // explicitly before reset().
    template <typename T, typename... Args>
    [[nodiscard]] T* construct(Args&&... args) {
        static_assert(alignof(T) <= kDefaultAlignment,
                      "T alignment exceeds pool alignment; use a larger alignment");
        void* mem = allocate(sizeof(T));
        return ::new (mem) T(std::forward<Args>(args)...);
    }

    // Allocate a page-aligned, zero-filled buffer suitable for O_DIRECT I/O.
    [[nodiscard]] void* alloc_io_buffer(std::size_t sz) {
        sz = align_up(sz, kDefaultAlignment);
        void* p = allocate(sz);
        std::memset(p, 0, sz);
        return p;
    }

    // ── Deallocation ───────────────────────────────────────────────────────────

    // Reset the CURRENT segment (O(1) bulk deallocation).  Prior segments are
    // kept so that pointers into them remain valid.  Call release_all() to
    // fully reclaim memory.
    void reset() noexcept {
        cursor_      = 0;
        live_allocs_ = 0;
    }

    // Release all OS memory.  All pointers previously returned by allocate()
    // become dangling.  The pool is empty but reusable.
    void release_all() noexcept {
        for (std::size_t i = 0; i < segments_.size(); ++i) {
            if (seg_is_mmap_[i]) {
                ::munmap(segments_[i], segment_bytes_);
            } else {
                ::free(segments_[i]);
            }
        }
        segments_.clear();
        seg_is_mmap_.clear();
        current_     = nullptr;
        cursor_      = 0;
        live_allocs_ = 0;
        try { grow(); } catch (...) {}
    }

    // ── Stats ──────────────────────────────────────────────────────────────────

    [[nodiscard]] std::size_t live_allocs()     const noexcept { return live_allocs_; }
    [[nodiscard]] std::size_t bytes_used()      const noexcept {
        // Full segments + cursor into current segment.
        return (segments_.size() > 1 ? (segments_.size() - 1) * segment_bytes_ : 0) + cursor_;
    }
    [[nodiscard]] std::size_t bytes_reserved()  const noexcept {
        return segments_.size() * segment_bytes_;
    }

private:
    void grow() {
        void* raw = nullptr;
        bool  is_mmap = false;

#if defined(__linux__)
        // Try 2 MB transparent hugepages first.
        // Falls back silently if the hugepage pool is exhausted or the kernel
        // is older than 3.8 (MAP_HUGE_2MB was added in 3.8).
        if (use_hugepages_) {
            // Align segment_bytes_ to 2 MB for hugepage mapping.
            const std::size_t hugepage_sz = 2u << 20; // 2 MB
            const std::size_t alloc_sz =
                (segment_bytes_ + hugepage_sz - 1) & ~(hugepage_sz - 1);

            raw = ::mmap(nullptr, alloc_sz,
                         PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB |
                         (21 << MAP_HUGE_SHIFT), // MAP_HUGE_2MB = 21 << MAP_HUGE_SHIFT
                         -1, 0);
            if (raw != MAP_FAILED) {
                // Update segment_bytes_ to actual allocated size so cursor
                // arithmetic stays correct.
                segment_bytes_ = alloc_sz;
                is_mmap = true;
            } else {
                raw = nullptr; // fall through to posix_memalign
            }
        }
#endif

        if (raw == nullptr) {
            if (::posix_memalign(&raw, alignment_, segment_bytes_) != 0) {
                throw std::bad_alloc{};
            }
            is_mmap = false;
        }

        segments_.push_back(raw);
        seg_is_mmap_.push_back(is_mmap);
        current_ = raw;
        cursor_  = 0;
    }

    static constexpr std::size_t align_up(std::size_t n, std::size_t a) noexcept {
        return (n + a - 1) & ~(a - 1);
    }

    std::size_t         segment_bytes_;
    std::size_t         alignment_;
    void*               current_{nullptr};
    std::size_t         cursor_{0};
    std::size_t         live_allocs_{0};
    std::vector<void*>  segments_;
    std::vector<bool>   seg_is_mmap_;  // true → munmap; false → free
    bool                use_hugepages_{false};
};

// ── RAII I/O buffer ───────────────────────────────────────────────────────────
//
// A page-aligned, fixed-size buffer backed by the pool.  Returned by
// SegmentedPool::alloc_io_buffer() when wrapped in this helper.  Suitable for
// passing directly to io_uring_prep_write / io_uring_prep_read.

struct alignas(4096) IoBuffer {
    static constexpr std::size_t kSize = 4096; // one page, also one NVMe sector * 8

    std::byte data[kSize]{};

    [[nodiscard]] void*       ptr()  noexcept { return data; }
    [[nodiscard]] const void* ptr()  const noexcept { return data; }
    [[nodiscard]] std::size_t size() const noexcept { return kSize; }
};

static_assert(sizeof(IoBuffer)  == 4096, "IoBuffer must be exactly 4 KB");
static_assert(alignof(IoBuffer) == 4096, "IoBuffer must be 4 KB aligned");

} // namespace veltrix
