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
documents as `@txt/<ns>/<id>`, namespace settings as `@vecns/<ns>`. The
search indexes in RAM are derived from those keys and rebuilt from them at
startup. Similarity is cosine.

## Commands

Text protocol (the binary protocol and the Go client have the same
operations; see [README — Wire protocol](../README.md#wire-protocol)):

| Command | Does |
|--|--|
| `VCREATE ns dim [QUANT none\|int8\|pq] [PQM m] [PQTRAIN n] [GRAPH memory\|disk]` | create or reconfigure a namespace (optional: the first `VSET` creates a float32 one) |
| `VSET id [NS ns] f1 f2 ...` | upsert a vector |
| `VDEL id [NS ns]` | delete a vector |
| `VSEARCH k [NS ns] [EF n] [FILTER field op value] f1 f2 ...` | top-k by cosine; `k = 0` returns every match |
| `TSET id [NS ns] TEXT free text` / `TDEL id [NS ns]` | upsert / delete a text document |
| `TSEARCH k [NS ns] [FILTER field op value] QUERY free text` | top-k by BM25 |
| `HSEARCH k [NS ns] [EF n] [ALPHA a] [CAND n] [FILTER ...] [VEC f1 ...] QUERY text` | hybrid, fused by reciprocal rank |

Go client (`client.BinaryConn`, and `client.TCPConn` for the text protocol):
`VCreateWithOptions`, `VSetNS`, `VDel`, `VSearchWithOptions`, `TSet`, `TDel`,
`TSearch`, `HSearch`. Binary opcodes: `0x08` VSETNS, `0x1E` VSEARCHX, `0x1F`
VDEL, and `0x1C` SEARCH with sub-ops 1–5 (VCREATE, TSET, TDEL, TSEARCH,
HSEARCH).

## Memory layouts

The full float32 vectors are always persisted in the VLog on NVMe. What the
RAM holds is a per-namespace choice:

| Layout | RAM holds | Search |
|--|--|--|
| float32 (default) | 4 × dim bytes + graph | exact scores |
| `QUANT int8` | dim + 4 bytes (codes + scale) + graph | graph walk on int8; top 4 × k re-ranked against the float32 vectors |
| `QUANT pq [PQM m]` | m bytes (default m = dim/8) + graph | ADC lookup tables; the whole beam re-ranked against the float32 vectors |
| `... GRAPH disk` | as above, minus layer-0 edges | layer-0 edges (32 per node, ~97 % of edge memory) in a memory-mapped scratch file the kernel can page to NVMe |

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
subspace) in the background and re-encodes; searches keep working
throughout. Running `VCREATE` on an existing namespace with different
settings re-encodes it from the persisted vectors (changing the dimension is
refused).

Sizing rule of thumb for N vectors of dimension d (heap): float32 ≈
N × (4d + ~310 B), int8 ≈ N × (d + ~310 B), pq ≈ N × (m + ~400 B),
pq + disk ≈ N × (m + ~280 B), plus for a disk graph 132 B per vector in the
mapped file (up to 2× that while the file grows by doubling — 216 B was
measured). The constant is graph edges and Go per-node overhead, derived from
the 768-dim measurement above; treat it as an estimate for other dimensions.

## Filters

`FILTER field op value` (ops `= != > < >= <= contains`) is evaluated on the
KV record whose key is the vector / document id (a JSON object or `k=v`
pairs). An `=` filter on a field with an `IDXCREATE` index is served from the
index: up to 2,048 matching ids are scored exactly, larger sets restrict the
graph walk to members. Other filters are checked during the walk, one read
per candidate that would enter the results, with the walk capped so a filter
matching almost nothing cannot scan the whole graph.

## Full-text (BM25) and hybrid search

- **Tokenizer:** lowercase runs of Unicode letters, digits and combining
  marks (so Devanagari, Tamil and other Indic words stay whole), no stemming,
  no stop words. Scripts without spaces (CJK, Thai) index whole runs.
- **Scoring:** Okapi BM25, k1 = 1.2, b = 0.75.
- **Hybrid:** vector and text searches each return `CAND` candidates
  (default max(50, 4k)); the lists are fused by weighted Reciprocal Rank
  Fusion, `score = α/(60 + rank_vec) + (1 − α)/(60 + rank_text)`, α = `ALPHA`
  (default 0.5). Ranks, not raw scores, are fused, so cosine and BM25 never
  need a common scale. Either half may be omitted.

## Clusters

- Search writes (`VSET`, `TSET`, `VCREATE`, `IDXCREATE`, …) go through the
  normal raft / replicated write path.
- The ring places `@vec/<ns>/<id>`, `@txt/<ns>/<id>` and
  `@idx/<rule>/<value>/<id>` on the node that owns `<id>`, so after a
  rebalance each node holds whole records and filters locally.
  `@vecns/` and `@idxdef/` keys are copied to every node.
- Every vector / text / hybrid search, `QUERY` and `IDXQUERY` runs on all
  non-failed nodes and is merged. BM25 first sums every node's corpus
  statistics so all nodes score with the same idf.
- A peer that does not answer within `--search-timeout-ms` (2000) fails the
  request and is named in the error; `--search-allow-partial` returns the
  rest instead. `--search-fanout=false` keeps searches local.
- Node-to-node traffic uses the transfer listener. Sign it with
  `--cluster-secret-file` (HMAC-SHA256, 5-minute clock-skew window) or use
  mTLS; without either the server logs that the listener is unauthenticated.

## Durability and restarts

- A vector or document write is acknowledged after the same WAL + VLog
  `fdatasync` as any other write.
- The graph and the inverted index are **not** saved; at startup they are
  rebuilt from the persisted keys on all cores. Until that finishes, searches
  are refused with `search indexes are still rebuilding after restart (loaded
  x of y)` — they would otherwise miss vectors — and `INFO` shows
  `search_ready=0`. `--search-allow-partial` answers anyway. KV traffic is not
  affected.
- 3 SIGKILL-during-writes cycles (8.5K, 8.4K and 11.9K acknowledged writes):
  every acknowledged vector, document and record was present after the
  rebuild, and no acknowledged delete came back (`TestSearchCrashRecovery`,
  macOS; the nightly workflow runs 10 cycles on Linux).

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
- **int8** keeps float32 recall at ~⅓ of the RAM; **pq** trades 2–3 points
  of recall for ~⅐ of it. Raise `PQM` (more bytes per vector) for recall.
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
- Text search has no stemming, phrase queries or per-field weighting.

## Benchmarks against other databases

None has been run. `bench/compare/cmd/vecbench` contains only a VeltrixDB
driver: Aerospike Vector Search and ScyllaDB vector search use client APIs
that were not available to test, and VectorDBBench has no client for either.
The harness has a `vectorDB` interface for adding one; see
[bench/compare/README.md](../bench/compare/README.md).

## How it is tested

| Where | What |
|--|--|
| Every PR, `node-7-search` | quality gates above; search, hook, routing and signing tests under `-race`; a 3-process cluster test (writes on one node searchable from the others, unsigned requests refused, a killed node fails searches until it is marked failed) |
| Nightly | recall gates on GloVe-100; a 20-minute soak (oracle-checked results, RSS must level off, exact match after restart); 10 SIGKILL / restart cycles; the RAM and group-commit tables |

A 60-second soak on macOS: 201,573 writes and 292,079 searches, 288,025
self-queries with 0 misses, RSS flat at ~1.0 GB, and an exact oracle match
(5,000 ids, 3,993 live) before and after a clean restart.
