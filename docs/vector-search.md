# Vector, full-text and hybrid search

VeltrixDB can serve nearest-neighbour vector search, BM25 full-text search
and hybrid (vector + text) search from the same database that holds the
records they describe. This page covers how it works, how to use it, how
much RAM it needs, and what it measured. Every number below states where it
was measured; none of them is a comparison with another database run on the
same machine (see [Benchmarks against other databases](#benchmarks-against-other-databases)).

- [Model: one id, one record](#model-one-id-one-record)
- [Commands](#commands)
- [Memory layouts: float32, int8, PQ, disk graph](#memory-layouts)
- [Filters](#filters)
- [Full-text (BM25) and hybrid search](#full-text-bm25-and-hybrid-search)
- [Clusters](#clusters)
- [Durability and restarts](#durability-and-restarts)
- [Measured performance](#measured-performance)
- [Tuning](#tuning)
- [Limitations](#limitations)
- [How it is tested](#how-it-is-tested)

## Model: one id, one record

A vector, a text document and a KV record that share an id describe one
thing:

```text
PUT     doc-42 {"lang":"en","tier":"gold"}          ← record (filters read this)
VSET    doc-42 NS docs 0.12 -0.03 ...               ← vector in namespace "docs"
TSET    doc-42 NS docs TEXT how to rotate keys ...  ← text document in "docs"
HSEARCH 10 NS docs FILTER lang = en VEC 0.1 ... QUERY rotate keys
```

Vectors persist as ordinary keys `@vec/<ns>/<id>` (float32, L2-normalized),
documents as `@txt/<ns>/<id>`, namespace settings as `@vecns/<ns>` (JSON),
secondary-index definitions as `@idxdef/<name>` and index entries as
`@idx/<name>/<value>/<id>`. The search indexes in RAM are derived from those
keys and rebuilt from them at startup. Similarity is cosine.

Deleting the record (`DEL doc-42`, a `TXN` delete, or the same delete
applied by raft / replication on another node) also deletes `@vec/<ns>/doc-42`
and `@txt/<ns>/doc-42` in every namespace, so the record stops appearing in
`VSEARCH`, `TSEARCH` and `HSEARCH`. A vector or document whose id never had a
record (pure vector search) is left alone: only `VDEL` / `TDEL` remove it, and
a `DEL` of an id with no record does nothing to it. A vector written after
its record was deleted is a new, record-less vector. TTL expiry of a record
does not delete its vectors or documents.

## Commands

Text protocol (the binary protocol and the Go client have the same
operations; see [README — Wire protocol](../README.md#wire-protocol)):

| Command | Does |
|--|--|
| `VCREATE ns dim [QUANT none\|int8\|pq] [PQM m] [PQTRAIN n] [GRAPH memory\|disk]` | create or reconfigure a namespace (optional: the first `VSET` creates a float32 one) |
| `VSET id [NS ns] f1 f2 ...` | upsert a vector |
| `VDEL id [NS ns]` | delete a vector (`DEL id` of its record also does, in every namespace) |
| `VSEARCH k [NS ns] [EF n] [FILTER field op value] f1 f2 ...` | top-k by cosine; `k = 0` returns every match (exact scan) |
| `TSET id [NS ns] TEXT free text` / `TDEL id [NS ns]` | upsert / delete a text document (text = rest of the line, at most 1 MiB) |
| `TSEARCH k [NS ns] [FILTER field op value] QUERY free text` | top-k by BM25; `k = 0` returns every match |
| `HSEARCH k [NS ns] [EF n] [ALPHA a] [CAND n] [FILTER ...] [VEC f1 ...] [QUERY text]` | hybrid, fused by reciprocal rank; `k > 0`; needs `VEC`, `QUERY` or both |
| `IDXCREATE name field` / `IDXDROP name` / `IDXQUERY name value [LIMIT n]` | secondary index on a record field (used by `=` filters) |

Search commands answer `id score` lines then `END`, or one `ERR ...` line.
The namespace defaults to `default`; a namespace name must be non-empty and
contain no `/`, and the dimension must be 1–4096. The search option clauses may come
in any order, each at most once; the floats (VSEARCH) or `QUERY` text come
last. Vectors must be non-zero and finite. The text protocol is
whitespace-split and line-based, so filter values cannot contain spaces and
`TSET` text cannot contain newlines (the binary protocol carries both).

Go client (`client.BinaryConn`, and `client.TCPConn` for the text protocol):
`VCreateWithOptions` (and `VCreate` with only a quantization), `VSetNS`,
`VDel`, `VSearchWithOptions`, `TSet`, `TDel`, `TSearch`, `HSearch`,
`IdxCreate`, `IdxDrop`, `IdxQuery`; `VSet` / `VSearch` use namespace
`default`. Binary opcodes: `0x08` VSETNS, `0x1E` VSEARCHX, `0x1F` VDEL,
`0x26` VSET and `0x27` VSEARCH (namespace `default`), and `0x1C` SEARCH with
sub-ops 1–5 (VCREATE, TSET, TDEL, TSEARCH, HSEARCH).

## Memory layouts

The full float32 vectors are always persisted in the VLog on NVMe. What the
RAM holds is a per-namespace choice:

| Layout | RAM holds | Search |
|--|--|--|
| float32 (default) | 4 × dim bytes + graph | exact scores |
| `QUANT int8` | dim + 4 bytes (codes + scale) + graph | graph walk on int8 (beam ≥ 64); top 4 × k re-ranked against the float32 vectors |
| `QUANT pq [PQM m]` | m bytes (default m = dim/8, range 1–dim) + graph, plus one codebook per namespace | ADC lookup tables; max(4 × k, ef) candidates — the whole beam — re-ranked against the float32 vectors |
| `... GRAPH disk` | as above, minus layer-0 edges | layer-0 edges (32 per node, ~97 % of edge memory) in 132-byte records in a memory-mapped scratch file the kernel can page to NVMe |

The re-rank reads the float32 vectors back with one parallel `MultiGet`
(LIRS-cached); a candidate deleted meanwhile is dropped. The disk graph's
file is created under `<first data dir>/vector-scratch` and unlinked at once
(it is never read after a restart). If it cannot be created, or on a
platform without mmap, the setting is accepted and the graph stays on the
heap.

Measured RAM, 768-dim, 10,000 synthetic clustered vectors, Go heap per
vector (`TestVectorMemoryTable`, macOS, 2026-09-30):

| Layout | Heap / vector | Mapped file / vector | vs float32 |
|--|--|--|--|
| float32 | 3,383 B | — | 1× |
| int8 | 1,078 B | — | 3.1× less |
| pq, m = 96 | 493 B | — | 6.9× less |
| pq, m = 96 + disk graph | 378 B | 216 B | 8.9× less heap |

A PQ namespace stores float32 until it holds `PQTRAIN` vectors (default
10,000, minimum 256), then trains its codebook (k-means, 256 centroids per
subspace, 12 Lloyd iterations, at most 10,000 training vectors) in the
background and re-encodes; searches keep working throughout. Running
`VCREATE` on an existing namespace with a different quantization, `PQM` or
graph placement re-encodes it from the persisted vectors (changing only
`PQTRAIN` does not; changing the dimension is refused).

Sizing rule of thumb for N vectors of dimension d (heap): float32 ≈
N × (4d + ~310 B), int8 ≈ N × (d + ~310 B), pq ≈ N × (m + ~400 B),
pq + disk ≈ N × (m + ~280 B), plus for a disk graph 132 B per vector in the
mapped file (up to 2× that while the file grows by doubling — 216 B was
measured). The constant is graph edges and Go per-node overhead, derived from
the 768-dim measurement above; treat it as an estimate for other dimensions.

## Filters

`FILTER field op value` (ops `= != > < >= <= contains`) is evaluated on the
KV record whose key is the vector / document id (a JSON object or `k=v`
pairs separated by `,`, `;` or space; a record without the field never
matches). The ordering ops compare numerically when both sides parse as
numbers, else lexicographically. An `=` filter on a field with an
`IDXCREATE` index is served from the index: up to 2,048 matching ids are
all scored without a graph walk (exact for float32; int8 / pq score the
codes and re-rank as above), larger sets restrict the graph walk to members; every
candidate is still checked against its live record. Other filters are
checked during the walk, one read per candidate that would enter the
results, with the walk capped at 10,000 + 20 × ef visited nodes so a filter
matching almost nothing cannot scan the whole graph. A BM25 filter is
checked best-first on the scored documents until k match.

## Full-text (BM25) and hybrid search

- **Tokenizer:** lowercase runs of Unicode letters, digits and combining
  marks (so Devanagari, Tamil and other Indic words stay whole), no stemming,
  no stop words. Scripts without spaces (CJK, Thai) index whole runs.
- **Scoring:** Okapi BM25, k1 = 1.2, b = 0.75, idf = ln(1 + (N − df + 0.5) /
  (df + 0.5)). A query with no tokens is refused (`query has no searchable
  terms`).
- **Hybrid:** vector and text searches each return `CAND` candidates
  (default max(50, 4k), capped at 10,000); the lists are fused by weighted
  Reciprocal Rank Fusion, `score = α/(60 + rank_vec) + (1 − α)/(60 +
  rank_text)` with 1-based ranks, α = `ALPHA` in [0, 1] (default 0.5). The
  fused score is what results carry. Ranks, not raw scores, are fused, so
  cosine and BM25 never need a common scale. Either half may be omitted (or
  missing: no vector namespace, no text tokens); it is an error only when
  neither can run.

## Clusters

- Search writes (`VSET`, `TSET`, `VCREATE`, `IDXCREATE`, …) go through the
  normal raft / replicated write path.
- Searches, `QUERY` and `IDXQUERY` pass the same read barrier as GET
  (`coordinator.readBarrier`): in raft mode a freshly elected leader waits
  for its term's no-op to apply, and with `--linearizable-reads` followers
  answer `MOVED` and the leader runs the ReadIndex fence first.
- The ring places `@vec/<ns>/<id>`, `@txt/<ns>/<id>` and
  `@idx/<rule>/<value>/<id>` on the node that owns `<id>`, so after a
  rebalance each node holds whole records and filters locally.
  `@vecns/` and `@idxdef/` keys are copied to every node.
- Every vector / text / hybrid search, `QUERY` and `IDXQUERY` runs on all
  non-failed nodes and is merged. BM25 first sums every node's corpus
  statistics so all nodes score with the same idf.
- A peer that does not answer within `--search-timeout-ms` (2000, per search
  phase; BM25 has two) fails the request and is named in the error (`search
  incomplete: n of m peers did not answer: ...`); `--search-allow-partial`
  returns the rest instead (logged). Nodes the failure detector marked failed
  are not queried. `--search-fanout=false` keeps searches local.
- Node-to-node traffic uses the transfer listener (`POST /internal/search`).
  Sign it with `--cluster-secret-file` or env `VELTRIXDB_CLUSTER_SECRET`
  (same secret on every node, at least 16 bytes; HMAC-SHA256 over time,
  method, path and body; 5-minute clock-skew window) or use mTLS
  (`--cluster-tls-cert` / `--cluster-tls-key` / `--cluster-tls-ca` with
  `--cluster-mtls`); without either the server logs that the listener is
  unauthenticated.

## Durability and restarts

- A vector or document write is acknowledged after the same WAL + VLog
  `fdatasync` as any other write.
- The graph and the inverted index are **not** saved; at startup they are
  rebuilt from the persisted keys on all cores (GOMAXPROCS workers,
  namespace settings first). Until that finishes, vector / text / hybrid
  searches are refused with `search indexes are still rebuilding after
  restart (loaded x of y; retry, or start the server with
  --search-allow-partial)` — they would otherwise miss vectors — also when
  the rebuilding node is a peer in a fan-out, and `INFO` shows
  `search_ready=0 search_loaded=x/y` (`search_ready=1` once done).
  `--search-allow-partial` answers anyway. KV traffic, `QUERY` and
  `IDXQUERY` are not affected.
- 3 SIGKILL-during-writes cycles (8.5K, 8.4K and 11.9K acknowledged writes):
  every acknowledged vector, document and record was present after the
  rebuild, and no acknowledged delete came back (`TestSearchCrashRecovery`,
  macOS; the nightly workflow runs 10 cycles on Linux). Writes in flight at
  the kill are not checked; after each cycle the test rewrites every such id
  to a fresh, fully acknowledged version (record, text and vector), so later
  cycles check it again.
- A record delete writes the record's tombstone first, then deletes its
  vectors and documents. If the process dies in between, the startup rebuild
  finds each vector / document whose record carries a newer tombstone,
  deletes it (durably) and does not index it
  (`TestSearch_CrashBetweenRecordAndDerivedDelete`).

## Measured performance

### Real embeddings — GloVe-100

First 100,000 words of [GloVe-100 (angular)](http://ann-benchmarks.com), 100
dimensions, 500 queries, exact cosine ground truth. In-process, one query at a
time, macOS (Apple silicon, GOMAXPROCS = 18), 2026-09-30
(`TestRealEmbeddings`). Build = 18 parallel durable `PutVector` writers.

| Layout | Build | ef | recall@10 | p50 | p99 | QPS (1 thread) |
|--|--|--|--|--|--|--|
| float32 | 57 s | 64 | 0.842 | 0.25 ms | 0.41 ms | 3,933 |
| | | 128 | 0.906 | 0.42 ms | 0.63 ms | 2,351 |
| | | 256 | 0.953 | 0.77 ms | 1.12 ms | 1,307 |
| | | 512 | 0.983 | 1.43 ms | 1.95 ms | 712 |
| int8 | 50 s | 64 | 0.839 | 0.61 ms | 0.81 ms | 1,655 |
| | | 128 | 0.905 | 0.55 ms | 0.80 ms | 1,905 |
| | | 256 | 0.952 | 0.72 ms | 1.08 ms | 1,356 |
| | | 512 | 0.983 | 1.18 ms | 1.84 ms | 834 |
| pq (m = 25) | 42 s | 64 | 0.797 | 0.56 ms | 0.90 ms | 1,771 |
| | | 128 | 0.870 | 0.72 ms | 1.06 ms | 1,367 |
| | | 256 | 0.927 | 1.17 ms | 2.91 ms | 780 |
| | | 512 | 0.969 | 1.56 ms | 3.44 ms | 604 |
| pq + disk graph | 45 s | 64 | 0.788 | 0.55 ms | 0.79 ms | 1,811 |
| | | 128 | 0.875 | 0.66 ms | 0.99 ms | 1,506 |
| | | 256 | 0.931 | 1.06 ms | 2.18 ms | 907 |
| | | 512 | 0.966 | 1.60 ms | 2.97 ms | 613 |

The quantized layouts are slower per query than float32 at this size because
they re-rank from the VLog (a parallel MultiGet — before that change int8 at
ef = 64 was p50 1.9 ms / p99 13.8 ms). Their point is RAM: at 100-dim the
vectors are small and graph edges dominate; at 768-dim the vector part is
3–9× smaller (table above).

### Over the network — `vecbench`

GloVe-100, first 20,000 words, 300 queries, int8, 8 concurrent clients, server
and client on one macOS machine (`bench/compare/cmd/vecbench`):

| ef | recall@10 | QPS | p50 | p99 |
|--|--|--|--|--|
| 64 | 0.894 | 24,637 | 0.28 ms | 0.68 ms |
| 256 | 0.985 | 15,629 | 0.48 ms | 0.91 ms |

Load: 6.4 s (3,146 durable vector writes/s).

### 768-dim, recall vs RAM

Same data as the RAM table (10K synthetic clustered, 768-dim, 50 queries,
engine re-rank rules):

| Layout | recall@10 ef = 64 | query | recall@10 ef = 256 | query |
|--|--|--|--|--|
| float32 | 0.452 | 0.51 ms | 0.782 | 1.42 ms |
| int8 | 0.468 | 0.43 ms | 0.776 | 1.15 ms |
| pq, m = 96 | 0.366 | 0.24 ms | 0.738 | 0.53 ms |
| pq + disk graph | 0.356 | 0.21 ms | 0.714 | 0.47 ms |

This synthetic set is deliberately hard (little cluster structure at
768-dim); use the GloVe numbers for expectations on real data.

### CI quality gate (synthetic, every PR)

5,000 × 64-dim clustered vectors, 100 queries, recall@10
(`TestSearchQualityGate`, measured values / gates): float32 0.927 / 0.90,
float32 ef = 256 0.998 / 0.97, int8 0.923 / 0.89, pq 0.882 / 0.85,
pq + disk graph 0.881 / 0.85, indexed filter on the graph path 0.994 / 0.96,
after deleting 40 % 0.96 / 0.92.

### HNSW rework (2026-09), index only

20,000 × 128-dim uniform random vectors, macOS, `BenchmarkHNSW_*`:

| | before | after |
|--|--|--|
| search, ef = 64 | 240 µs, 853 allocs | 99 µs, 6 allocs |
| search with 50 % deleted | 6.56 ms (the beam grew by the tombstone count) | 190 µs |
| insert | 349 µs | 209 µs |

## Tuning

- **`EF`** is the recall / latency knob. On GloVe-100, ef = 128 gives ~0.90
  recall@10 and ef = 256 ~0.95.
- **int8** keeps float32 recall at ~⅓ of the RAM; **pq** trades 1.4–4.5
  points of recall@10 on GloVe-100 (ef 512 → 64) for ~⅐ of it at 768-dim.
  Raise `PQM` (more bytes per vector) for recall.
- **`GRAPH disk`** helps once edges dominate RAM (many vectors, small codes).
- **Filters:** create an `IDXCREATE` index on fields used with `=`.
- **Clusters:** a search costs one round trip to every node; with
  `--search-fanout=false` it is local only (correct only if every node holds
  every record, i.e. no rebalance has moved keys).

## Limitations

- The graph and inverted index are rebuilt at every start (not persisted);
  searches are refused until the rebuild ends. Budget for it on large
  namespaces.
- Graph nodes, ids and the upper HNSW layers stay on the Go heap even with
  `GRAPH disk`; the constant ~280–400 B per vector does not shrink with
  quantization.
- Tested up to 100K vectors (real) and 10K at 768-dim; nothing at 10M+ yet.
- No same-hardware comparison with other vector databases (below).
- Text search has no stemming, phrase queries or per-field weighting; one
  document is at most 1 MiB.
- Dimension 1–4096.
- A record that expires by TTL keeps its vectors and documents (only a
  delete removes them); use `VDEL` / `TDEL` for those, and for vectors left
  behind by deletes made before this version.

## Benchmarks against other databases

None has been run. `bench/compare/cmd/vecbench` contains only a VeltrixDB
driver: Aerospike Vector Search and ScyllaDB vector search use client APIs
that were not available to test, and VectorDBBench has no client for either.
The harness has a `vectorDB` interface for adding one; see
[bench/compare/README.md](../bench/compare/README.md).

## How it is tested

| Where | What |
|--|--|
| Every PR, `node-7-search` (`.github/workflows/ci.yml`) | quality gates above plus BM25 / RRF ranking (`TestSearchQualityGate*`); search, hook, routing and signing tests under `-race`; a 3-process cluster test, `TestSearchCluster_EndToEnd` (writes on one node searchable from the others, unsigned requests refused, a killed node fails searches until it is marked failed) |
| Nightly (`.github/workflows/nightly.yml`) | `real-embeddings`: recall gates on GloVe-100 (`TestRealEmbeddings`, `VELTRIX_ANN_DIR`); `search-soak`: a 20-minute `TestSearchSoak` (`VELTRIX_SOAK_DURATION`; oracle-checked results, self-query miss rate ≤ 2 %, RSS at the end ≤ 1.5 × RSS at 25 % of the run, exact match after restart); `crash-chaos`: 10 SIGKILL / restart cycles (`TestSearchCrashRecovery`, `VELTRIX_CHAOS_CYCLES`); `perf-tables`: the RAM and group-commit tables |

The GloVe-100 files come from `scripts/ann-dataset.py glove-100-angular
OUT_DIR --train 100000 --test 500` (needs h5py and numpy). It sends its own
User-Agent (ann-benchmarks.com answers 403 to Python-urllib's), retries a
failed download up to 4 attempts, and writes through a `.part` temp file so
a failed download never leaves a truncated file in the CI cache.

A 60-second soak on macOS: 201,573 writes and 292,079 searches, 288,025
self-queries with 0 misses, RSS flat at ~1.0 GB, and an exact oracle match
(5,000 ids, 3,993 live) before and after a clean restart.
