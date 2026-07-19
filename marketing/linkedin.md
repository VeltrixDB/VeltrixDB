# LinkedIn Post

---

After 18 months of building, we're open-sourcing VeltrixDB today.

It's a distributed key-value database designed from scratch for NVMe SSDs. Here's the problem we were trying to solve:

At 1 billion keys with 128-byte values, Redis requires ~250 GB of RAM. On GCP, that's roughly $3,000–5,000/month. The same dataset on NVMe costs ~$300–500/month. RAM is 15–20× more expensive per GB than SSD — and most KV workloads don't need full in-memory speed for every key, just for the hot ones.

Existing disk-backed databases (RocksDB, LevelDB) have a different problem: LSM tree compaction rewrites your data 10–30× over its lifetime. Every byte you write eventually touches disk 10–30 times. That's SSD wear, it's write latency spikes, and it's unpredictable P99 under sustained write pressure.

VeltrixDB uses a research technique called WiscKey (FAST '16, by Lu et al. at University of Wisconsin) to get around this. The core idea: separate keys from values at the storage layer. Keys and metadata live in a DRAM index. Values are written once to an append-only Value Log on NVMe and never rewritten. Compaction only reclaims dead space — it doesn't move live data. Write amplification drops to ~1.0×.

Benchmark results on a 3-node GKE cluster (8 NVMe disks per node, 1 billion keys, 30 minutes sustained):

→ 7.2M reads/second  
→ 1.8M writes/second  
→ P99 latency: 510 µs (blended)  
→ Zero GC emergency events  
→ ~160 GB storage for 1B × 128B values

We've also built the operational layer we wish existed: a Kubernetes Operator with auto-resharding and self-healing, a Helm chart with 22 Prometheus alerting rules, a cloud-agnostic NVMe provisioner for GKE/EKS/AKS, and client SDKs for Go, Java, Python, Node.js, Rust, and C++.

What's NOT ready: Redis protocol compatibility (RESP is on the roadmap), a managed cloud offering, and Raft log snapshots for large clusters. We'd rather ship an honest v1 than oversell it.

Apache 2.0. The code is at github.com/VeltrixDB/veltrixdb.

If you're paying Redis bills at scale, or running into write amplification issues on RocksDB, I'd genuinely like to hear about your workload. DMs are open.

#databases #infrastructure #opensource #distributedsystems #kubernetes

---

**Post variations:**

**Shorter version (for lower engagement threshold):**

We open-sourced VeltrixDB today — a distributed KV database for NVMe SSDs.

The short version: Redis at 1B keys costs ~$4K/month in RAM. The same workload on VeltrixDB costs ~$400/month on NVMe. Write amplification is ~1.0× (vs 10-30× for LSM trees). P99 latency stays flat under sustained write pressure.

3-node benchmark: 7.2M reads/s, P99 510 µs, zero GC emergencies in 30 minutes.

Apache 2.0 → github.com/VeltrixDB/veltrixdb

What gaps exist: no Redis protocol compatibility yet, no managed cloud, no Raft snapshots.

Happy to go deeper on the architecture if anyone's curious. 

#databases #opensource #infrastructure
