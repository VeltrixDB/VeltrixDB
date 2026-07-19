/*
 * ebpf_gc_throttle.cpp — cgroup v2 blkio-based GC bandwidth enforcement.
 *
 * See ebpf_gc_throttle.h for architecture and motivation.
 */

#ifdef __linux__

#include "ebpf_gc_throttle.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace veltrix {

// ── Construction / Destruction ────────────────────────────────────────────────

KernelGCThrottle::KernelGCThrottle(KernelGCThrottleConfig cfg)
    : cfg_(std::move(cfg))
{}

KernelGCThrottle::~KernelGCThrottle() {
    destroy();
}

// ── init ──────────────────────────────────────────────────────────────────────

bool KernelGCThrottle::init() {
    if (initialized_) return true;

    // Verify cgroup v2 is mounted at the parent path.
    const std::string cgroup_type_path = cfg_.cgroup_parent + "/cgroup.controllers";
    if (::access(cgroup_type_path.c_str(), R_OK) != 0) {
        // cgroup v2 not mounted or parent does not exist.
        return false;
    }

    gc_cgroup_path_ = cfg_.cgroup_parent + "/gc";

    // Create the GC sub-cgroup.  EEXIST is acceptable (leftover from a prior
    // run with the same process, e.g., after a crash).
    if (::mkdir(gc_cgroup_path_.c_str(), 0755) != 0 && errno != EEXIST) {
        return false;
    }
    cgroup_created_ = true;

    // Enable the io controller in the parent cgroup so the child inherits it.
    // Write "+io" to cgroup.subtree_control — a no-op if already enabled.
    write_file(cfg_.cgroup_parent + "/cgroup.subtree_control", "+io");

    // Write initial bandwidth limits.
    if (!write_io_max(cfg_.initial_write_bps, cfg_.initial_read_bps)) {
        // io.max write failed — cgroup exists but io controller not available.
        // This is non-fatal: we still get the cgroup isolation benefit; limits
        // will apply once the operator enables the io controller.
    }

    initialized_ = true;
    return true;
}

// ── destroy ───────────────────────────────────────────────────────────────────

void KernelGCThrottle::destroy() {
    if (!cgroup_created_ || gc_cgroup_path_.empty()) return;

    // Remove bandwidth limits before deleting the cgroup.
    write_io_max(0, 0); // 0 = unlimited (removes the throttle entry)

    // A cgroup can only be removed when it has no member threads.
    // Threads enrolled via enroll_current_thread() exit their goroutines
    // before the engine shuts down, so by the time destroy() is called the
    // cgroup should be empty.  rmdir() is best-effort; failure is ignored.
    ::rmdir(gc_cgroup_path_.c_str());
    cgroup_created_ = false;
    initialized_    = false;
}

// ── enroll_current_thread ─────────────────────────────────────────────────────

bool KernelGCThrottle::enroll_current_thread() {
    if (!initialized_) return false;

    // Write TID (not PID) to cgroup.threads so only this OS thread is moved.
    const pid_t tid = static_cast<pid_t>(::syscall(SYS_gettid));
    return write_file(gc_cgroup_path_ + "/cgroup.threads", std::to_string(tid));
}

// ── set_write_limit ───────────────────────────────────────────────────────────

bool KernelGCThrottle::set_write_limit(uint64_t write_bps) {
    if (!initialized_) return false;
    return write_io_max(write_bps, cfg_.initial_read_bps);
}

// ── write_io_max (private) ────────────────────────────────────────────────────

bool KernelGCThrottle::write_io_max(uint64_t write_bps,
                                     uint64_t read_bps) const {
    if (gc_cgroup_path_.empty()) return false;

    const std::string io_max_path = gc_cgroup_path_ + "/io.max";
    bool any_ok = false;

    for (int i = 0; i < KernelGCThrottleConfig::kMaxDisks; ++i) {
        if (cfg_.disk_devnos[i].empty()) continue;

        // io.max format: "<major>:<minor> wbps=N rbps=N"
        // "max" means unlimited; 0 is not valid — use "max" for that case.
        std::ostringstream line;
        line << cfg_.disk_devnos[i] << " wbps=";
        if (write_bps == 0) line << "max"; else line << write_bps;
        line << " rbps=";
        if (read_bps == 0) line << "max"; else line << read_bps;

        if (write_file(io_max_path, line.str())) any_ok = true;
    }

    return any_ok;
}

// ── write_file (private) ──────────────────────────────────────────────────────

bool KernelGCThrottle::write_file(const std::string& path,
                                   const std::string& content) const {
    std::ofstream f(path);
    if (!f.is_open()) return false;
    f << content << '\n';
    return f.good();
}

} // namespace veltrix

#endif // __linux__
