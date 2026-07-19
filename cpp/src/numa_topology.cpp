/*
 * numa_topology.cpp — NUMA detection via sysfs + pthread thread pinning.
 *
 * All sysfs reads are best-effort: failures return safe defaults so that
 * NUMA-unaware machines (macOS dev, single-node VMs) still work correctly.
 */

#include "numa_topology.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>

#ifdef __linux__
#  include <dirent.h>
#  include <pthread.h>
#  include <sched.h>
#  include <unistd.h>
#endif

/* ── Helpers ────────────────────────────────────────────────────────────── */

/* Parse a Linux CPU list string like "0-3,8,12-15" into a vector of CPU IDs. */
static std::vector<int> parse_cpu_list(const std::string& s) {
    std::vector<int> cpus;
    std::istringstream ss(s);
    std::string token;
    while (std::getline(ss, token, ',')) {
        const size_t dash = token.find('-');
        if (dash == std::string::npos) {
            cpus.push_back(std::stoi(token));
        } else {
            const int lo = std::stoi(token.substr(0, dash));
            const int hi = std::stoi(token.substr(dash + 1));
            for (int i = lo; i <= hi; ++i) cpus.push_back(i);
        }
    }
    std::sort(cpus.begin(), cpus.end());
    return cpus;
}

static std::string read_file(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) return {};
    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
    /* Trim trailing whitespace / newline */
    while (!content.empty() && (content.back() == '\n' || content.back() == ' '))
        content.pop_back();
    return content;
}

/* ── discover_topology ──────────────────────────────────────────────────── */

NumaTopology discover_topology() {
    NumaTopology topo;

#ifdef __linux__
    const std::string node_base = "/sys/devices/system/node/";
    DIR* dir = opendir(node_base.c_str());
    if (!dir) {
        /* sysfs unavailable — return single-node topology with all CPUs */
        NumaNode n0;
        n0.id = 0;
        const int ncpus = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
        topo.num_cpus   = ncpus;
        for (int i = 0; i < ncpus; ++i) n0.cpus.push_back(i);
        topo.nodes.push_back(std::move(n0));
        return topo;
    }

    dirent* ent;
    while ((ent = readdir(dir)) != nullptr) {
        if (strncmp(ent->d_name, "node", 4) != 0) continue;
        const std::string num_str = ent->d_name + 4;
        if (num_str.empty() || !std::isdigit(num_str[0])) continue;

        NumaNode node;
        node.id = std::stoi(num_str);

        /* Read CPU list for this node */
        const std::string cpulist_path = node_base + ent->d_name + "/cpulist";
        const std::string cpulist      = read_file(cpulist_path);
        if (!cpulist.empty()) node.cpus = parse_cpu_list(cpulist);

        /* Read approximate free memory */
        const std::string meminfo_path = node_base + ent->d_name + "/meminfo";
        std::ifstream mf(meminfo_path);
        if (mf.is_open()) {
            std::string line;
            while (std::getline(mf, line)) {
                if (line.find("MemFree") != std::string::npos) {
                    std::istringstream ls(line);
                    std::string k;
                    ls >> k >> node.memory_mb; // reads kB
                    node.memory_mb /= 1024;    // convert to MB
                    break;
                }
            }
        }

        topo.nodes.push_back(std::move(node));
    }
    closedir(dir);

    std::sort(topo.nodes.begin(), topo.nodes.end(),
              [](const NumaNode& a, const NumaNode& b) { return a.id < b.id; });
    topo.num_cpus = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));

#else
    /* Non-Linux (macOS dev): single node, all CPUs */
    NumaNode n0;
    n0.id = 0;
    topo.num_cpus = 4; // reasonable default for dev
    for (int i = 0; i < topo.num_cpus; ++i) n0.cpus.push_back(i);
    topo.nodes.push_back(std::move(n0));
#endif

    return topo;
}

/* ── nvme_preferred_node ────────────────────────────────────────────────── */

int nvme_preferred_node(int nvme_index, const NumaTopology& topo) {
#ifdef __linux__
    /* /sys/class/nvme/nvme<N>/device/local_cpulist */
    char path[256];
    snprintf(path, sizeof(path),
             "/sys/class/nvme/nvme%d/device/local_cpulist", nvme_index);
    const std::string cpulist = read_file(path);
    if (cpulist.empty()) return -1;

    const std::vector<int> nvme_cpus = parse_cpu_list(cpulist);
    if (nvme_cpus.empty()) return -1;

    /* Find which NUMA node has the most CPUs from nvme_cpus */
    int best_node = -1;
    size_t best_count = 0;
    for (const NumaNode& n : topo.nodes) {
        size_t count = 0;
        for (int cpu : nvme_cpus) {
            if (std::find(n.cpus.begin(), n.cpus.end(), cpu) != n.cpus.end())
                ++count;
        }
        if (count > best_count) {
            best_count = count;
            best_node  = n.id;
        }
    }
    return best_node;
#else
    (void)nvme_index;
    (void)topo;
    return 0; // single node on non-Linux
#endif
}

/* ── Thread pinning ─────────────────────────────────────────────────────── */

bool pin_thread_to_cpus(const std::vector<int>& cpus) {
#ifdef __linux__
    if (cpus.empty()) return false;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int c : cpus) {
        if (c >= 0 && c < CPU_SETSIZE)
            CPU_SET(c, &cpuset);
    }
    return pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == 0;
#else
    (void)cpus;
    return false; // no-op on macOS
#endif
}

bool pin_thread_to_node(const NumaNode& node) {
    return pin_thread_to_cpus(node.cpus);
}
