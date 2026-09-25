# VeltrixDB Launch Thread

---

**Tweet 1 — Hook** [271 chars]
We benchmarked an open-source key-value database with YCSB on one NVMe node, 100M keys.

427K reads/s. 18K writes/s with an fsync on every write. Zero errors across 100M ops.

Introducing VeltrixDB — built for NVMe, not RAM.

🧵

---

**Tweet 2 — The cost story** [278 chars]
1B keys × 128B values on Redis: ~250 GB RAM → $3,000–5,000/month.

Same dataset on VeltrixDB: ~160 GB NVMe → $300–500/month.

That's not a rounding error. It's a 10× reduction in storage cost for identical data — because NVMe is cheap and RAM is not.

---

**Tweet 3 — Write amplification** [263 chars]
Most people don't think about write amplification until their SSDs are dead.

Redis persistence: 2–5×
RocksDB/LSM trees: 10–30×
VeltrixDB: ~1.0×

When you write 1 byte, we write ~1 byte to NVMe. That's it. Your drives last longer. Your compaction overhead disappears.

---

**Tweet 4 — WiscKey architecture** [269 chars]
How do you get 1.0× write amplification?

WiscKey KV-separation: keys go in the index, values append once to NVMe and are never rewritten by compaction.

No rewrite cycles. No compaction churning through your data. Values land on disk once and stay exactly where they landed.

---

**Tweet 5 — Benchmarks in depth** [276 chars]
Setup: YCSB 0.17.0, one AWS EC2 node (4× NVMe), 100M keys, 200 threads.

Reads: 427,697/s
Avg read latency: 461 µs
Durable writes: 18,064/s (fsync each)
Errors: 0
Value-log GC emergency runs: 0

Full methodology + reproduction scripts: github.com/VeltrixDB/veltrixdb-benchmark

---

**Tweet 6 — Architecture details** [248 chars]
Under the hood:

• 8192-shard index → parallel per-disk I/O
• LIRS cache → scan-resistant, doesn't evict hot keys on a full table scan
• Group-commit WAL → batches writes to cut fsync overhead
• Raft replication → consistent distributed state

Each piece exists to extract NVMe performance, not paper over it.

---

**Tweet 7 — Kubernetes story** [252 chars]
Production-grade from day one:

• Kubernetes Operator with a CRD
• Helm chart
• Prometheus alerts pre-configured
• AES-256-GCM encryption
• RBAC + audit logging
• CDC for streaming changes downstream

You shouldn't have to wire up your own monitoring every time you deploy a database.

---

**Tweet 8 — Client SDKs** [185 chars]
Six official client SDKs:

Go · Java · Python · Node.js · Rust · C++

Pick your language. The wire protocol is consistent across all of them. No third-party wrappers, no abandoned community ports.

---

**Tweet 9 — One honest limitation** [247 chars]
The thing we don't have yet: RESP protocol support.

That means you can't point an existing Redis client at VeltrixDB without code changes. It's the most-requested missing feature and it's on the roadmap.

Also missing: a managed cloud offering.

Honest gaps > hidden surprises.

---

**Tweet 10 — How to get started** [258 chars]
Three ways to run it:

1. Helm: helm install veltrixdb veltrixdb/veltrixdb
2. Operator: apply the CRD, configure your VeltrixDB custom resource
3. Local: grab the binary and point it at an NVMe mount

Full quickstart docs at github.com/VeltrixDB/veltrixdb

Takes about 10 minutes to have a cluster up.

---

**Tweet 11 — Call to star** [182 chars]
If you're running key-value workloads where RAM cost is the constraint — or if you just want to follow along as we ship RESP support — a GitHub star helps more than you'd think.

⭐ github.com/VeltrixDB/veltrixdb

Apache 2.0. Always.

---

**Tweet 12 — Engagement question** [218 chars]
Question for engineers who've run Redis or RocksDB at scale:

What actually broke down first — RAM cost, write amplification, operational complexity, or something else entirely?

Trying to understand what problems matter most before we set the next roadmap priorities.
