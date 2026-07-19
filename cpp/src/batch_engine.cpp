/*
 * batch_engine.cpp — Vectorized batch engine with shard-parallel thread pool.
 *
 * Entry point for all Go→C++ batch operations.  Compiled as C++17.
 * On Linux this file is included via storage/cgo_batch_impl_linux.cpp so that
 * `go build` (with CGO_ENABLED=1) compiles it automatically alongside the Go
 * storage package without requiring a separate CMake step.
 */

#include "batch_engine.hpp"
#include "numa_topology.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

/* ── Compile-time layout assertions ──────────────────────────────────────── */
static_assert(sizeof(SliceView)     == 16, "SliceView must be 16 bytes");
static_assert(sizeof(BatchPutEntry) == 40, "BatchPutEntry must be 40 bytes");
static_assert(offsetof(BatchPutEntry, key)         ==  0, "key offset");
static_assert(offsetof(BatchPutEntry, value)       == 16, "value offset");
static_assert(offsetof(BatchPutEntry, ttl_seconds) == 32, "ttl offset");
static_assert(offsetof(BatchPutEntry, shard_hint)  == 36, "shard_hint offset");

/* ── FNV-1a 64-bit (must match storage/shard.go fnv64a exactly) ─────────── */
static constexpr uint64_t kFNVOffset = 14695981039346656037ULL;
static constexpr uint64_t kFNVPrime  = 1099511628211ULL;
// kNumShards must match storage/shard.go numShards (8192, power-of-two).
// Bit mask: shard = h & 0x1FFF.
static constexpr size_t   kNumShards = 8192;

static inline uint16_t shard_of(const void* data, size_t len) noexcept {
    uint64_t h = kFNVOffset;
    const auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        h ^= p[i];
        h *= kFNVPrime;
    }
    return static_cast<uint16_t>(h & (kNumShards - 1));
}

/* ── ThreadPool ─────────────────────────────────────────────────────────── */
/*
 * Fixed-size pool of OS threads with optional NUMA CPU pinning.
 *
 * When `numa_cpus` is non-empty, each thread is pinned to the given CPU set
 * using pthread_setaffinity_np.  All threads share the same affinity mask
 * (they compete for the CPUs within the target NUMA node) to avoid
 * cross-NUMA memory latency on L3 / DRAM accesses made during shard work.
 */
class ThreadPool {
public:
    explicit ThreadPool(int n, const std::vector<int>& numa_cpus = {}) {
        assert(n > 0);
        workers_.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) {
            workers_.emplace_back([this, numa_cpus] {
                /* Pin BEFORE executing any work so the first task also runs
                 * on the correct NUMA node. */
                if (!numa_cpus.empty()) pin_thread_to_cpus(numa_cpus);
                loop();
            });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) t.join();
    }

    /* Submit a task.  Returns immediately; task runs on next idle worker. */
    void submit(std::function<void()> fn) {
        {
            std::unique_lock<std::mutex> lk(mu_);
            queue_.push_back(std::move(fn));
        }
        cv_.notify_one();
    }

    /* Block until queue is empty and all workers have returned. */
    void wait_idle() {
        std::unique_lock<std::mutex> lk(mu_);
        idle_cv_.wait(lk, [this] {
            return queue_.empty() && active_ == 0;
        });
    }

    int size() const noexcept { return static_cast<int>(workers_.size()); }

private:
    void loop() {
        for (;;) {
            std::function<void()> fn;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return !queue_.empty() || stop_; });
                if (stop_ && queue_.empty()) return;
                fn = std::move(queue_.front());
                queue_.pop_front();
                ++active_;
            }
            fn();
            {
                std::unique_lock<std::mutex> lk(mu_);
                --active_;
            }
            idle_cv_.notify_all();
        }
    }

    std::vector<std::thread>             workers_;
    std::deque<std::function<void()>>    queue_;
    std::mutex                           mu_;
    std::condition_variable              cv_;
    std::condition_variable              idle_cv_;
    int                                  active_{0};
    bool                                 stop_{false};
};

/* ── VeltrixBatchEngine ─────────────────────────────────────────────────── */
struct VeltrixBatchEngine {
    ThreadPool            pool;
    std::atomic<uint64_t> puts_total{0};
    std::atomic<uint64_t> gets_total{0};

    /* Simple constructor (no NUMA pinning) */
    explicit VeltrixBatchEngine(int n) : pool(n) {}

    /* NUMA-aware constructor */
    VeltrixBatchEngine(int n, const std::vector<int>& numa_cpus)
        : pool(n, numa_cpus) {}
};

/* ── extern "C" implementations ─────────────────────────────────────────── */
extern "C" {

VeltrixBatchEngine* veltrix_batch_engine_create(int num_threads) {
    if (num_threads <= 0) num_threads = 1;
    try {
        return new VeltrixBatchEngine(num_threads);
    } catch (...) {
        return nullptr;
    }
}

VeltrixBatchEngine* veltrix_batch_engine_create_ex(BatchEngineConfig cfg) {
    int n = cfg.num_threads > 0 ? cfg.num_threads : 1;
    try {
        if (!cfg.numa_aware) return new VeltrixBatchEngine(n);

        NumaTopology topo = discover_topology();
        if (topo.nodes.empty()) return new VeltrixBatchEngine(n);

        int node_id = cfg.numa_node;
        if (node_id < 0) node_id = nvme_preferred_node(0, topo);
        if (node_id < 0 || node_id >= static_cast<int>(topo.nodes.size()))
            node_id = 0;

        const std::vector<int>& cpus = topo.nodes[static_cast<size_t>(node_id)].cpus;
        if (cpus.empty()) return new VeltrixBatchEngine(n);

        return new VeltrixBatchEngine(n, cpus);
    } catch (...) {
        return nullptr;
    }
}

void veltrix_batch_engine_destroy(VeltrixBatchEngine* engine) {
    delete engine;
}

int veltrix_batch_put(
    VeltrixBatchEngine*  engine,
    const BatchPutEntry* entries,
    size_t               count
) {
    if (!engine || !entries || count == 0) return 0;

    /*
     * Phase 1 — Group entry indices by shard.
     *
     * Use a fixed stack-allocated array of 256 per-shard index lists.
     * For typical batch sizes (≤ 10 000 entries) the inner vectors rarely
     * spill to heap; for large batches the heap allocation is amortized.
     */
    struct ShardGroup { std::vector<size_t> idxs; };
    ShardGroup groups[kNumShards];

    for (size_t i = 0; i < count; ++i) {
        const BatchPutEntry& e = entries[i];
        uint16_t shard = e.shard_hint != 0
            ? e.shard_hint
            : shard_of(e.key.ptr, e.key.len);
        groups[shard].idxs.push_back(i);
    }

    /*
     * Phase 2 — Dispatch non-empty shard groups to the thread pool.
     *
     * Each lambda captures entries and its own shard group by reference.
     * Both remain alive until wait_idle() returns, so no dangling references.
     *
     * The atomic counter `processed` aggregates per-shard results without
     * a mutex (relaxed ordering is fine — we read it only after wait_idle).
     */
    std::atomic<size_t> processed{0};

    for (size_t s = 0; s < kNumShards; ++s) {
        if (groups[s].idxs.empty()) continue;

        engine->pool.submit([&entries, &groups, s, &processed] {
            size_t local = 0;
            for (size_t idx : groups[s].idxs) {
                const BatchPutEntry& e = entries[idx];
                /*
                 * Per-entry work performed by the C++ thread pool:
                 *   • Validate key/value pointers and lengths.
                 *   • In a full production deployment this would also:
                 *       – update the C++ ART index (LeafMetadata)
                 *       – queue io_uring PWRITE via PriorityScheduler
                 *   Those paths are wired in when libveltrixdb_engine is linked.
                 * The Go layer handles WAL fdatasync independently.
                 */
                if (e.key.ptr != nullptr && e.key.len > 0 &&
                    e.value.ptr != nullptr) {
                    ++local;
                }
            }
            processed.fetch_add(local, std::memory_order_relaxed);
        });
    }

    /* Block until all shard workers have finished. */
    engine->pool.wait_idle();

    const size_t n = processed.load(std::memory_order_relaxed);
    engine->puts_total.fetch_add(n, std::memory_order_relaxed);
    return static_cast<int>(n);
}

int veltrix_batch_get(
    VeltrixBatchEngine* engine,
    const SliceView*    keys,
    size_t              key_count,
    SliceView*          results
) {
    if (!engine || !keys || !results || key_count == 0) return 0;

    /* Zero-initialise: miss == { nullptr, 0 }. */
    std::memset(results, 0, key_count * sizeof(SliceView));

    engine->gets_total.fetch_add(key_count, std::memory_order_relaxed);
    /*
     * Full implementation: group keys by shard, dispatch to thread pool,
     * consult ART index per shard, return pointers to engine-owned buffers.
     * Currently returns 0 hits — reads go through the Go engine path.
     */
    return 0;
}

uint64_t veltrix_batch_engine_puts_total(const VeltrixBatchEngine* e) {
    return e ? e->puts_total.load(std::memory_order_relaxed) : 0;
}

uint64_t veltrix_batch_engine_gets_total(const VeltrixBatchEngine* e) {
    return e ? e->gets_total.load(std::memory_order_relaxed) : 0;
}

int veltrix_batch_engine_thread_count(const VeltrixBatchEngine* e) {
    return e ? e->pool.size() : 0;
}

}  /* extern "C" */
