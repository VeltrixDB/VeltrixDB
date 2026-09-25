# Product Hunt Launch Materials

## Tagline

NVMe key-value store — 10× cheaper storage than Redis at scale

---

## Description

VeltrixDB is an open-source distributed key-value database designed for NVMe SSDs. On a single AWS EC2 node (4× NVMe, YCSB 0.17.0, 100M keys, 200 threads) it served 427,697 reads/s at 461 µs average latency and 18,064 durable writes/s with an fsync on every write.

The core insight is that RAM is expensive and NVMe is not. At 1 billion keys × 128-byte values, Redis requires ~250 GB of RAM — roughly $3,000–5,000/month depending on your cloud. VeltrixDB stores the same dataset in ~160 GB of NVMe for $300–500/month. Same data, 10× lower storage cost.

The architecture that makes this work: WiscKey KV-separation, where values are appended once to NVMe and never rewritten by compaction. This produces write amplification of ~1.0× — versus 2–5× for Redis persistence and 10–30× for RocksDB/LSM-based systems. An 8192-shard index enables parallel per-disk I/O. A LIRS scan-resistant cache and group-commit WAL handle the rest.

Across those 100M operations there were zero errors and zero value-log GC emergency runs: space reclamation kept up with the write rate instead of falling behind. On cgo builds the key index lives off the Go heap; with 5M keys a full Go GC takes 0.27 ms instead of 21 ms.

Production deployment is first-class: Kubernetes Operator with a CRD, Helm chart, Prometheus alerts pre-configured, Raft replication, AES-256-GCM encryption, RBAC, audit logging, and CDC support. Six client SDKs cover Go, Java, Python, Node.js, Rust, and C++.

What it doesn't do yet: no RESP protocol (can't drop-in replace Redis without code changes) and no managed cloud offering. These are real limitations worth knowing before you evaluate it.

Apache 2.0. Benchmarking methodology and reproduction scripts are in the public repo.

If you're running key-value workloads where the bottleneck is RAM cost rather than raw compute, VeltrixDB is worth a look.

---

## First Comment (Maker Comment)

I want to be upfront about where we are and why we built this.

The cost problem is real. We watched teams pay $4,000/month for Redis cluster RAM on workloads that accessed maybe 5% of their keyspace hot. The data didn't need to live in RAM — it needed to live somewhere fast. Modern NVMe delivers sub-millisecond latency at a fraction of the cost, but existing KV databases weren't designed to take full advantage of it.

The benchmark numbers are reproducible. We've published the full methodology and scripts at github.com/VeltrixDB/veltrixdb-benchmark because extraordinary claims need to be verifiable. Run it on your own hardware and tell us where it breaks.

The honest gaps: RESP protocol compatibility is the most-asked-for missing feature — it would allow existing Redis clients to connect without changes. It's not there yet, and neither is a managed cloud offering. We're not hiding these; they're on the public roadmap.

We're releasing Apache 2.0 because this infrastructure should be auditable and extensible without licensing constraints.

Happy to answer specific questions about the architecture decisions, benchmark methodology, or roadmap priorities. What would make you actually consider running this in production?
