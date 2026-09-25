# Show HN: VeltrixDB – an NVMe key-value store at 1/10th the RAM cost of Redis

We built VeltrixDB because we kept running into the same problem: key-value workloads that fit fine on NVMe but forced us to pay for RAM anyway. Redis is excellent software, but at 1 billion keys × 128-byte values you're looking at ~250 GB of RAM ($3,000–5,000/month) versus ~160 GB of NVMe ($300–500/month). That's a 10× cost difference for the same data.

**What it is**

VeltrixDB is an open-source, distributed key-value database built specifically for NVMe SSDs. It uses WiscKey-style KV-separation (values append-once to NVMe, never rewritten by compaction), an 8192-shard index for parallel per-disk I/O, a LIRS scan-resistant cache, and a group-commit WAL. The result is write amplification around 1.0× — compared to 2–5× for Redis persistence modes and 10–30× for RocksDB/LSM trees.

**Benchmark numbers**

YCSB 0.17.0 on a single AWS EC2 node (4× NVMe), 100M keys, 200 threads:

- Reads: 427,697/s (461 µs average latency)
- Durable writes (fsync every write): 18,064/s
- Errors across 100M ops: 0
- Value-log GC emergency runs: 0
- Write amplification: ~1.0×

An earlier internal 3-node GKE run reached 7.2M reads/s / 1.8M writes/s, but it has not been reproduced with a published harness, so treat it as unverified.

"GC" in the list above is the value-log garbage collector: zero emergency runs means space reclamation kept up with the write rate. On the Go side, cgo builds keep the key index off the Go heap; with 5M keys a full Go GC takes 0.27 ms instead of 21 ms, at ~19 ns extra per cache-miss lookup.

**How it deploys**

There's a Kubernetes Operator with a CRD, a Helm chart, and Prometheus alerts baked in. Replication uses Raft. Storage is encrypted with AES-256-GCM. RBAC and audit logging are included. CDC is supported for streaming changes downstream.

Client SDKs: Go, Java, Python, Node.js, Rust, C++.

**Honest gaps**

We don't have RESP protocol support yet, so you can't point an existing Redis client at it without code changes. There's no managed cloud offering — you run it yourself. These are on the roadmap but not done.

**Why open source**

We're releasing under Apache 2.0 because we want the infrastructure community to be able to audit, extend, and run it without licensing risk. The benchmarking code is also in the repo so you can reproduce results on your own hardware.

**What we're looking for**

Feedback on the architecture decisions — specifically the WiscKey KV-separation tradeoffs and the 8192-shard design. If you've run into NVMe-backed KV workloads and hit walls with existing solutions, we'd love to hear what broke down.

GitHub: https://github.com/VeltrixDB/veltrixdb

The benchmark repo with methodology details: https://github.com/VeltrixDB/veltrixdb-benchmark
