# Product Hunt Launch Materials

## Tagline

NVMe key-value at 7.2M reads/s — 10× cheaper than Redis at scale

---

## Description

VeltrixDB is an open-source distributed key-value database designed for NVMe SSDs. It delivers 7.2M reads/s and 1.8M writes/s on a 3-node GKE cluster at 1 billion keys, with P50 latency around 220 ns and P99 around 510 µs.

The core insight is that RAM is expensive and NVMe is not. At 1 billion keys × 128-byte values, Redis requires ~250 GB of RAM — roughly $3,000–5,000/month depending on your cloud. VeltrixDB stores the same dataset in ~160 GB of NVMe for $300–500/month. Same data, 10× lower storage cost.

The architecture that makes this work: WiscKey KV-separation, where values are appended once to NVMe and never rewritten by compaction. This produces write amplification of ~1.0× — versus 2–5× for Redis persistence and 10–30× for RocksDB/LSM-based systems. A 1024-shard design enables parallel per-disk I/O. A LIRS scan-resistant cache and group-commit WAL handle the rest.

In a 30-minute sustained benchmark at 1 billion keys, VeltrixDB produced zero GC emergency events — critical for production systems where GC pauses are a common source of latency spikes and cascading failures.

Production deployment is first-class: Kubernetes Operator with a CRD, Helm chart, 22 Prometheus alerts pre-configured, Raft replication, AES-256-GCM encryption, RBAC, audit logging, and CDC support. Six client SDKs cover Go, Java, Python, Node.js, Rust, and C++.

What it doesn't do yet: no RESP protocol (can't drop-in replace Redis without code changes), no managed cloud offering, no native range scans, no Raft snapshots. These are real limitations worth knowing before you evaluate it.

Apache 2.0. Benchmarking methodology and reproduction scripts are in the public repo.

If you're running key-value workloads where the bottleneck is RAM cost rather than raw compute, VeltrixDB is worth a look.

---

## First Comment (Maker Comment)

I want to be upfront about where we are and why we built this.

The cost problem is real. We watched teams pay $4,000/month for Redis cluster RAM on workloads that accessed maybe 5% of their keyspace hot. The data didn't need to live in RAM — it needed to live somewhere fast. Modern NVMe delivers sub-millisecond latency at a fraction of the cost, but existing KV databases weren't designed to take full advantage of it.

The benchmark numbers are reproducible. We've published the full methodology and scripts at github.com/VeltrixDB/veltrixdb-benchmark because extraordinary claims need to be verifiable. Run it on your own hardware and tell us where it breaks.

The honest gaps: RESP protocol compatibility is the most-asked-for missing feature — it would allow existing Redis clients to connect without changes. It's not there yet. Raft snapshots are also missing, which matters for replica recovery after extended downtime. We're not hiding these; they're on the public roadmap.

We're releasing Apache 2.0 because this infrastructure should be auditable and extensible without licensing constraints.

Happy to answer specific questions about the architecture decisions, benchmark methodology, or roadmap priorities. What would make you actually consider running this in production?
