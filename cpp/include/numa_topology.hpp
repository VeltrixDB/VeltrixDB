#pragma once
/*
 * numa_topology.hpp — NUMA node detection and thread pinning.
 *
 * Why NUMA matters for VeltrixDB
 * ───────────────────────────────
 * A 64-core / 480 GB machine typically has 2–4 NUMA nodes.  A thread on
 * NUMA node 0 accessing memory allocated on NUMA node 1 pays ~90 ns
 * cross-NUMA latency (vs ~30 ns local).  At 2M RPS with 47% cache misses,
 * cross-NUMA metadata fetches alone add ~85 µs of aggregate latency per
 * second per core — enough to blow the <5 ms P99 budget.
 *
 * NVMe interrupt affinity
 * ────────────────────────
 * The kernel assigns NVMe interrupt vectors to CPUs at driver init time.
 * Reading /sys/class/nvme/nvme<N>/device/local_cpulist tells us which CPUs
 * own the NVMe IRQs.  Pinning our I/O threads to those CPUs eliminates
 * cross-NUMA interrupt wakeups.
 *
 * This module reads /sys without requiring libnuma (which may not be
 * installed on all GKE node images).
 */

#include <cstdint>
#include <string>
#include <vector>

/* One NUMA node description */
struct NumaNode {
    int              id{-1};
    std::vector<int> cpus;       // logical CPU IDs in this node
    size_t           memory_mb{0}; // approximate free memory in MB
};

/* Discovered system NUMA topology */
struct NumaTopology {
    std::vector<NumaNode> nodes;   // one entry per NUMA node
    int                   num_cpus{0};
};

/* ── Discovery ──────────────────────────────────────────────────────────── */

/* discover_topology() reads /sys/devices/system/node/ to enumerate nodes
 * and their CPU lists.  Returns a topology with one "node 0" containing all
 * CPUs if sysfs is unavailable (e.g., on macOS). */
NumaTopology discover_topology();

/* nvme_preferred_node() reads /sys/class/nvme/nvme<idx>/device/local_cpulist
 * and returns the NUMA node that owns the most IRQ CPUs for that NVMe device.
 * Returns -1 if the device does not exist or the file cannot be read. */
int nvme_preferred_node(int nvme_index, const NumaTopology& topo);

/* ── Thread pinning ─────────────────────────────────────────────────────── */

/* pin_thread_to_node() calls pthread_setaffinity_np to restrict the calling
 * thread's CPU affinity to the CPUs of `node`.  Returns true on success. */
bool pin_thread_to_node(const NumaNode& node);

/* pin_thread_to_cpus() pins the calling thread to a specific CPU set. */
bool pin_thread_to_cpus(const std::vector<int>& cpus);
