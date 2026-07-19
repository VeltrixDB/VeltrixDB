# Show HN: VeltrixDB – 7.2M reads/s on NVMe at 1/10th the RAM cost of Redis

We built VeltrixDB because we kept running into the same problem: key-value workloads that fit fine on NVMe but forced us to pay for RAM anyway. Redis is excellent software, but at 1 billion keys × 128-byte values you're looking at ~250 GB of RAM ($3,000–5,000/month) versus ~160 GB of NVMe ($300–500/month). That's a 10× cost difference for the same data.

**What it is**

VeltrixDB is an open-source, distributed key-value database built specifically for NVMe SSDs. It uses WiscKey-style KV-separation (values append-once to NVMe, never rewritten by compaction), a 1024-shard architecture for parallel per-disk I/O, a LIRS scan-resistant cache, and a group-commit WAL. The result is write amplification around 1.0× — compared to 2–5× for Redis persistence modes and 10–30× for RocksDB/LSM trees.

**Benchmark numbers**

On a 3-node GKE cluster at 1 billion keys, 128-byte values:

- Reads: 7.2M/s
- Writes: 1.8M/s
- Blended P50 (80% read, 20% write): ~220 ns
- Blended P99: ~510 µs
- GC emergency events in 30-minute sustained run: 0
- Write amplification: ~1.0×

The GC number matters because we've seen other Go-based databases degrade badly under sustained load when GC pauses spike. The 30-minute benchmark at 1B keys with zero GC emergency events was a deliberate stress test of that specifically.

**How it deploys**

There's a Kubernetes Operator with a CRD, a Helm chart, and 22 Prometheus alerts baked in. Replication uses Raft. Storage is encrypted with AES-256-GCM. RBAC and audit logging are included. CDC is supported for streaming changes downstream.

Client SDKs: Go, Java, Python, Node.js, Rust, C++.

**Honest gaps**

We don't have RESP protocol support yet, so you can't point an existing Redis client at it without code changes. There's no managed cloud offering — you run it yourself. Native range scans aren't supported. Raft snapshots aren't implemented, which limits how far behind a replica can fall before it needs a full resync. These are on the roadmap but not done.

**Why open source**

We're releasing under Apache 2.0 because we want the infrastructure community to be able to audit, extend, and run it without licensing risk. The benchmarking code is also in the repo so you can reproduce results on your own hardware.

**What we're looking for**

Feedback on the architecture decisions — specifically the WiscKey KV-separation tradeoffs and the 1024-shard design. If you've run into NVMe-backed KV workloads and hit walls with existing solutions, we'd love to hear what broke down.

GitHub: https://github.com/VeltrixDB/veltrixdb

The benchmark repo with methodology details: https://github.com/VeltrixDB/veltrixdb-benchmark
