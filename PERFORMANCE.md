# VeltrixDB Performance Guide

---

## The Most Important Knob: Group Commit

WAL and VLog both use group commit: concurrent writes share one `fdatasync`.
Every write is acknowledged only after the sync that covers it.

**Adaptive (default, `--group-commit=adaptive`).** The flush window is an
upper bound, not a wait. A writer that is alone (one request in the batch and
an EWMA of < 1.5 requests per recent flush) is synced at once (latency ≈ one
fdatasync). When writers are concurrent, a batch stays open until no new
write has arrived for an idle gap equal to the EWMA of recent fdatasync times,
clamped to 20 µs–2 ms, or the window expires, or the batch cap
(`--wal-max-batch`, default 4096) is reached (`storage/group_commit.go`).
Measured with an emulated 300 µs device sync, 15 ms window, macOS
(`VELTRIX_GC_TABLE=1 go test ./storage -run TestGroupCommit_LatencyTable -v`):

| Writers | Fixed 15 ms window | Adaptive |
|--|--|--|
| 1 | P50 16.0 ms, 63 writes/s | P50 0.45 ms, 2,186 writes/s |
| 8 | P50 16.0 ms, 501 writes/s | P50 0.96 ms, 8,283 writes/s |
| 64 | P50 15.9 ms, P99 16.8 ms, 4,053 writes/s | P50 1.7 ms, P99 2.6 ms, 37,504 writes/s |
| 256 | P99 33.2 ms, 15,291 writes/s | P99 11.5 ms, 73,999 writes/s |

Batches are as large as in fixed mode (64 writes per sync at 64 writers), so
adaptive does not cost more IOPS under load. With a 2 ms sync (slow or network
disk) 64 writers go 3,541 → 11,691 writes/s.

**Fixed (`--group-commit=fixed`).** The pre-2026-10 behaviour: every batch
waits the whole window. Only useful if you depend on its exact batching.

**Rule: `-wal-flush-window-ms` and `-vlog-flush-window-ms` must always be equal.**
In adaptive mode the window only bounds how long a busy batch may stay open;
the default 15 ms is fine for almost everything. In fixed mode:

```
writes/s ≈ goroutines × (1000 / window_ms)
```

| Goal (fixed mode) | Window | P99 write |
|------|--------|-----------|
| Lowest latency (low concurrency) | 0 ms | ~0.2 ms |
| Low latency | 2 ms | ~2.2 ms |
| 100K+ writes/s | **5 ms** | ~5.2 ms |
| Default window | **15 ms** | ~15.2 ms |
| Maximum throughput | 20 ms | ~20.2 ms |

P99 figures in the fixed table are Linux NVMe. **Do not size against a macOS
measurement** — on Darwin the engine calls plain `fsync(2)`, which returns at
the drive cache in ~0.02 ms and does not flush it (the adaptive table above
emulates the sync cost for that reason). See ARCHITECTURE.md, "Durability".

---

## Batch Operations

Always use `MultiPut` / `MultiGet` when writing or reading multiple keys. They share one network round-trip.

| Method | Throughput | When to use |
|--------|-----------|-------------|
| Individual PUT | ≈ 1 / fdatasync per goroutine (adaptive; 2,186/s at an emulated 300 µs sync), 1000 / window_ms in fixed mode | Single key |
| MultiPut 1024 keys | ~108K/s (macOS) / ~426K/s (Linux) | Any multi-key write |

The MultiPut row is historical (before the one-`write(2)`-per-batch WAL flusher
and the ordered-index fix). Current server numbers, 8 clients × 1024-key MPUT:
**3.54M keys/s, P99 4.2 ms** over a 1M-key space on macOS (1.45M / 9.6 ms
before the ordered-index fix), and **1.77M keys/s, P99 12.2–12.7 ms** on a
4-CPU CI runner (tmpfs, client on the same host, Go front-end, C++ storage off:
`ENGINES=nocgo NETS=go ./scripts/net-bench.sh`, whose batch phase is
`loadtest --mode write --concurrency 8 --batch-size 1024`).

The binary-protocol handler coalesces pipelined PUT frames that are already buffered on one connection into a MultiPut (`tryCoalescePuts`, cmd/server/main.go) — pipelining binary clients get batch performance without code changes. Text-protocol PUTs and a client that waits for each reply are not coalesced.

---

## Disk Density

Batch writes pack multiple records per 4 KB VLog block. Density gain at common value sizes:

| Avg value | Disk per record (packed) | Gain vs legacy 4 KB |
|-----------|--------------------------|---------------------|
| 64 B | 88 B | **47×** |
| 128 B | 152 B | **27×** |
| 256 B | 280 B | **15×** |
| 512 B | 536 B | **7.6×** |
| 4 KB+ | 4096 B | 1× |

Packing is automatic for `MultiPut` and pipelined binary PUTs. A single `Put` uses the unpacked lock-free path, which pads each record to a full 4 KB block. Verify: `veltrixdb_vlog_file_bytes / veltrixdb_storage_writes_total ≈ value_size + 24`.

---

## Cache Tuning

```bash
-cache 65536      # 64 GB
--auto-tune       # cache = 85% of an 80%-of-RAM budget (≈68% of RAM); ignores -cache
--read-heavy      # 400 GB cache, LIRRatio 0.95, GC every 300 s, TTL scan every 60 s (-cache still wins)
```

Check hit rate: `veltrixdb_cache_hits_total / (hits + misses)`. Small values (≤256 B) resist eviction more — sized workloads with tiny values benefit the most from cache.

---

## Index Memory and Go GC

On cgo builds (the default, including macOS and the Docker image) the index
lives off the Go heap in per-shard C++ tables, so the GC does not scan it.
Measured with 5M keys: full GC **21 ms → 0.27 ms**, settled RSS **168 → 142
B/key**, at a cost of ~19 ns more per cache-miss lookup. The ordered index
adds ~90 B/key on top unless `--disable-ordered-index`. Compare the two
implementations with `go test ./storage -run '^$' -bench 'Index_Footprint|Index_Get' -benchtime 1x`
(the footprint benchmark uses 2M keys and excludes the ordered index).

```bash
VELTRIXDB_INDEX=map ./veltrixdb ...      # opt out: Go map index (also CGO_ENABLED=0 builds)
sysctl -p scripts/sysctl.conf            # vm.max_map_count = 262144
```

Large key counts need `vm.max_map_count ≥ 262144` (a large engine holds ~25K
mappings); below that an insert can fail with ENOMEM.

---

## C++ Storage Switches

| Env var | Default | Effect |
|---------|---------|--------|
| `VELTRIXDB_URING_BRIDGE` | `off` | `on` / `sqpoll`: submit VLog batch writes through io_uring (Linux cgo). Falls back to `pwrite` where `io_uring_setup` is blocked (Kubernetes RuntimeDefault seccomp). |
| `VELTRIXDB_DISABLE_CGO_ENGINE` | unset | `1`: no C++ batch engine, bridge off, Go map index |

The bridge is off because a VLog batch is already one `pwrite` plus one
`fdatasync`, so it saves at most one syscall per batch. On a 4-CPU CI runner
(tmpfs) the C++ storage layer with everything on (bridge with SQPOLL, batch
engine, native index) measured **1.12M keys/s** batch writes against **1.77M**
with all three off; per-part attribution is pending. Measure before enabling.

---

## Network Front-End

`--net=go` (default) serves every protocol. `--net=cpp|uring|poll` is an
opt-in, experimental C++ event-loop front-end (`--net-threads`, default one per
core) for the binary PUT GET DEL PING MPUT MGET commands, standalone mode
without `--auth-config`.

It is **not faster overall**. On the 4-CPU CI runner (tmpfs, client on the same
host, measured before disk reads were moved off the loop): read P50 154 vs
286 µs and poll read throughput 194K vs 177K ops/s, but read P99 ~3.0 vs
1.8 ms and batch writes ~12% lower; io_uring did not beat poll. Compare on your
hardware with `scripts/net-bench.sh` (see BENCHMARKING.md). The server logs a
per-stage latency breakdown of the C++ front-end at shutdown.

Do not judge network changes on macOS: loopback caps round trips at ~60K/s for
the Go and C++ front-ends alike.

---

## Profiling

```bash
./veltrixdb --pprof-addr 127.0.0.1:6060   # CPU / heap / trace profiles, separate listener, off by default
# add contention profiles for a diagnosis (both empty unless enabled; block costs CPU while on):
./veltrixdb --pprof-addr 127.0.0.1:6060 --pprof-mutex-fraction 10 --pprof-block-rate 10000
go tool pprof http://127.0.0.1:6060/debug/pprof/mutex
```

---

## Admission Control

| Read EWMA | What happens | Constant |
|-----------|-------------|----------|
| < 15 ms | Normal | — |
| ≥ 15 ms | GC bandwidth capped at 60 MB/s | `gcLatencyThresholdNs` |
| > 20 ms | GC paused + each PUT sleeps 2 ms | `admissionThrottleNs` |
| < 10 ms | Everything resumes | `admissionResumeNs` |
| No reads 4+ min | EWMA stale — GC auto-resumes | `gcEWMAStaleDuration` |

If `veltrixdb_storage_write_admission_throttles_total` is rising, your read
EWMA is above **20 ms**. Diagnose with `veltrixdb_storage_read_latency_seconds`.

The GC bandwidth cap deliberately fires **before** the full pause, so GC is
throttled progressively rather than stopped dead.

> Earlier revisions of this table said 3 / 4 / 2 ms. Those numbers never
> matched the code — the constants are in `storage/types.go` and
> `storage/defrag.go`.

---

## Prometheus Queries

```promql
rate(veltrixdb_storage_writes_total[1m])                                    # write throughput
rate(veltrixdb_storage_reads_total[1m])                                     # read throughput

rate(veltrixdb_cache_hits_total[5m]) /
  (rate(veltrixdb_cache_hits_total[5m]) + rate(veltrixdb_cache_misses_total[5m]))   # hit ratio

rate(veltrixdb_storage_writes_total[1m]) / rate(veltrixdb_storage_wal_flushes_total[1m])            # WAL amortisation

histogram_quantile(0.99, rate(veltrixdb_storage_read_latency_seconds_bucket[5m]))  # P99 read
```

---

## Vector and Text Search

Details and all measurements: [docs/vector-search.md](docs/vector-search.md).

| Knob | Effect (GloVe-100, 100K words, recall@10 / p50, one query at a time, macOS) |
|--|--|
| `VSEARCH ... EF n` | ef 64: 0.842 / 0.25 ms · ef 128: 0.906 / 0.42 ms · ef 256: 0.953 / 0.77 ms · ef 512: 0.983 / 1.43 ms (float32) |
| `VCREATE ... QUANT int8` | same recall as float32, ~⅓ of the vector RAM; re-ranks 4 × k from disk (+0.1–0.3 ms at this size) |
| `VCREATE ... QUANT pq [PQM m]` | −2 to −3 points of recall, ~⅐ of float32's RAM at 768-dim; re-ranks the whole beam from disk. More `PQM` = better recall, more RAM |
| `VCREATE ... GRAPH disk` | moves layer-0 edges (≈97 % of edge memory) to a mapped file; recall unchanged within noise |
| `IDXCREATE` on filter fields | `=` filters served from the index instead of one read per candidate |
| `--search-fanout=false` | searches stay local — only correct while every node holds every record |

RAM per 768-dim vector (heap, measured): float32 3.4 KB, int8 1.1 KB, pq
(m = 96) 0.49 KB, pq + disk graph 0.38 KB + 0.22 KB mapped.

The search indexes are rebuilt at startup and searches are refused until the
rebuild finishes (`INFO` → `search_ready`); plan restarts of large namespaces
accordingly.

---

## Common Problems

| Symptom | Check | Fix |
|---------|-------|-----|
| Write P99 high | `veltrixdb_storage_write_admission_throttles_total` rising | Read latency too high — see admission control |
| Cache hit rate low | `veltrixdb_cache_evictions_total` rising, `cache_size_bytes` ≈ `cache_max_size_bytes` | Increase `-cache` (or reclaim ~90 B/key with `--disable-ordered-index` and give it to the cache) |
| GC not running | `vlog_gc_skipped_paused_total` rising | Wait 4 min for stale EWMA to clear |
| `reads_total = 0` with traffic | Old binary | Update — the missing metrics call is fixed |
| False failure alerts (single node) | `fd.SetLocalNode` not called | Fix before `fd.Start()` in your cluster setup |
| Writes slow at low concurrency (≈ window per write) | `--group-commit=fixed` set | Use the default `adaptive` |
| `search indexes are still rebuilding after restart` | `INFO` shows `search_ready=0 search_loaded=x/y` | Wait for the rebuild, or `--search-allow-partial` if incomplete results are acceptable |
| `search incomplete: N of M peers did not answer` | the named peer is down or slow; not yet marked failed | Retry after the failure detector marks it, raise `--search-timeout-ms`, or `--search-allow-partial` |
| `[transfer] WARNING: listener ... is unauthenticated` | no `--cluster-secret-file` and no mTLS | Set the same secret file on every node |
| PQ namespace uses as much RAM as float32 | fewer than `PQTRAIN` vectors, codebook not trained yet | Expected until the threshold; lower `PQTRAIN` (≥ 256) for small namespaces |
