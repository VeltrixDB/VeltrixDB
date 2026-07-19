#pragma once
/*
 * ebpf_gc_throttle.h — Kernel-level GC I/O bandwidth enforcement.
 *
 * Motivation (RESYSTANCE, Oct 2025)
 * ──────────────────────────────────
 * The paper moves compaction I/O enforcement into kernel space via eBPF,
 * eliminating the userspace token-bucket polling that adds syscall overhead
 * on every GC byte written.  VeltrixDB's equivalent uses Linux cgroup v2
 * blkio throttle — the kernel I/O scheduler enforces bandwidth limits at the
 * hardware queue level without any userspace intervention.
 *
 * How it works
 * ─────────────
 * 1. KernelGCThrottle creates a dedicated cgroup under
 *    /sys/fs/cgroup/veltrix.slice/gc/ (or a user-specified mount).
 * 2. The defrag thread(s) TID is written to cgroup.threads so only GC I/O
 *    is throttled — reads and writes on request-handler threads are unaffected.
 * 3. Bandwidth limits are written to io.max as "<major>:<minor> wbps=N rbps=N".
 *    The kernel blkio controller enforces these without any further polling.
 * 4. set_limit() updates io.max at runtime — called by the scheduler when
 *    gcRatio crosses critical/emergency thresholds (matching defrag.go logic).
 *
 * Fallback
 * ─────────
 * If cgroup v2 is unavailable (kernel < 5.0, cgroup v1 only, or insufficient
 * permissions), init() returns false and the caller falls back to the Go-side
 * token bucket in defrag.go.  The scheduler checks initialized() before use.
 *
 * Linux only — guarded by #ifdef __linux__.
 */

#ifdef __linux__

#include <cstdint>
#include <string>

namespace veltrix {

// ── KernelGCThrottleConfig ────────────────────────────────────────────────────

struct KernelGCThrottleConfig {
    // Parent cgroup path (cgroup v2 unified hierarchy mount).
    // The GC cgroup is created as <cgroup_parent>/gc/.
    std::string cgroup_parent{"/sys/fs/cgroup/veltrix.slice"};

    // Block device major:minor pairs, one per disk.  Populated automatically
    // from the NVMe device backing each VLog when not set explicitly.
    // Format: "259:0" (the number reported by /proc/diskstats for the device).
    // Up to kMaxDisks entries.
    static constexpr int kMaxDisks = 8;
    std::string disk_devnos[kMaxDisks]; // empty string → skip that disk slot

    // Initial bandwidth cap in bytes/sec (0 = unlimited).
    uint64_t initial_write_bps{static_cast<uint64_t>(60) << 20}; // 60 MB/s
    uint64_t initial_read_bps{0};                                  // GC reads: unlimited
};

// ── KernelGCThrottle ──────────────────────────────────────────────────────────

class KernelGCThrottle {
public:
    explicit KernelGCThrottle(KernelGCThrottleConfig cfg = {});
    ~KernelGCThrottle();

    KernelGCThrottle(const KernelGCThrottle&)            = delete;
    KernelGCThrottle& operator=(const KernelGCThrottle&) = delete;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    // Create the cgroup and write initial io.max limits.
    // Returns true on success, false on any failure (no cgroup v2, missing
    // permissions, etc.).  Safe to call from any thread before GC starts.
    bool init();

    // Remove the cgroup and restore any modified kernel state.
    void destroy();

    // ── Thread registration ───────────────────────────────────────────────────

    // Move the calling thread into the GC cgroup so its I/O is throttled.
    // Must be called from within the defrag goroutine (after LockOSThread).
    // Returns false if the cgroup was not successfully initialised.
    bool enroll_current_thread();

    // ── Bandwidth control ─────────────────────────────────────────────────────

    // Update the write bandwidth cap for all registered disks.
    // write_bps == 0 means unlimited (emergency GC bypass).
    // Writes to cgroup io.max — the kernel enforces this without any further
    // userspace action.  Thread-safe.
    bool set_write_limit(uint64_t write_bps);

    // ── State ─────────────────────────────────────────────────────────────────

    bool initialized() const noexcept { return initialized_; }

    // Path to the managed GC cgroup (empty until init() succeeds).
    const std::string& cgroup_path() const noexcept { return gc_cgroup_path_; }

private:
    bool write_io_max(uint64_t write_bps, uint64_t read_bps) const;
    bool write_file(const std::string& path, const std::string& content) const;

    KernelGCThrottleConfig cfg_;
    std::string            gc_cgroup_path_;
    bool                   initialized_{false};
    bool                   cgroup_created_{false};
};

} // namespace veltrix

#endif // __linux__
