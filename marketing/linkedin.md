# LinkedIn Post

---

After 18 months of building, we're open-sourcing VeltrixDB today.

It's a distributed key-value database designed from scratch for NVMe SSDs. Here's the problem we were trying to solve:

An in-memory store keeps every value in RAM, and RAM costs far more per GB than NVMe SSD — while most KV workloads only need in-memory speed for the hot keys. VeltrixDB keeps values on NVMe and only the index in RAM (~142 B/key). With 1 KB values that is roughly 4–7× less RAM; with very small values the index is about as big as the data, so the gain grows with value size.

Existing disk-backed databases (RocksDB, LevelDB) have a different problem: LSM tree compaction rewrites your data 10–30× over its lifetime. Every byte you write eventually touches disk 10–30 times. That's SSD wear, it's write latency spikes, and it's unpredictable P99 under sustained write pressure.

VeltrixDB uses a research technique called WiscKey (FAST '16, by Lu et al. at University of Wisconsin) to get around this. The core idea: separate keys from values at the storage layer. Keys and metadata live in a DRAM index. Values are written once to an append-only Value Log on NVMe and never rewritten. Compaction only reclaims dead space — it doesn't move live data. Write amplification drops to ~1.0×.

Benchmark results (YCSB 0.17.0, single AWS EC2 node with 4 NVMe disks, 100M keys, 200 threads):

→ 427,697 reads/second (461 µs average latency)  
→ 18,064 durable writes/second (fsync on every write)  
→ Zero errors and zero value-log GC emergency runs across 100M operations  
→ ~160 GB storage for 1B × 128B values

We've also built the operational layer we wish existed: a Kubernetes Operator with auto-resharding and self-healing, a Helm chart with Prometheus alerting rules, a cloud-agnostic NVMe provisioner for GKE/EKS/AKS, and client SDKs for Go, Java, Python, Node.js, Rust, and C++.

What's NOT ready: Redis protocol compatibility (RESP is on the roadmap) and a managed cloud offering. We'd rather ship an honest v1 than oversell it.

Apache 2.0. The code is at github.com/VeltrixDB/veltrixdb.

If you're paying Redis bills at scale, or running into write amplification issues on RocksDB, I'd genuinely like to hear about your workload. DMs are open.

#databases #infrastructure #opensource #distributedsystems #kubernetes

---

**Post variations:**

**Shorter version (for lower engagement threshold):**

We open-sourced VeltrixDB today — a distributed KV database for NVMe SSDs.

The short version: Redis at 1B keys costs ~$4K/month in RAM. The same workload on VeltrixDB costs ~$400/month on NVMe. Write amplification is ~1.0× (vs 10-30× for LSM trees). No compaction rewrites, so no compaction-driven P99 spikes.

Single-node YCSB: 427K reads/s, 18K fsync'd writes/s, zero errors across 100M ops.

Apache 2.0 → github.com/VeltrixDB/veltrixdb

What gaps exist: no Redis protocol compatibility yet, no managed cloud.

Happy to go deeper on the architecture if anyone's curious. 

#databases #opensource #infrastructure
