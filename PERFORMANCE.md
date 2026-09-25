# VeltrixDB Performance Guide

---

## The Most Important Knob: Flush Windows

WAL and VLog both use group-commit. N writes arriving within the window share one `fdatasync`.

**Rule: `-wal-flush-window-ms` and `-vlog-flush-window-ms` must always be equal.**

```
writes/s ≈ goroutines × (1000 / window_ms)
```

| Goal | Window | P99 write |
|------|--------|-----------|
| Lowest latency (low concurrency) | 0 ms | ~0.2 ms |
| Low latency | 2 ms | ~2.2 ms |
| 100K+ writes/s | **5 ms** | ~5.2 ms |
| **Default (balance)** | **15 ms** | ~15.2 ms |
| Maximum throughput | 20 ms | ~20.2 ms |

P99 figures are Linux NVMe. **Do not size against a macOS measurement** — on
Darwin the engine calls plain `fsync(2)`, which returns at the drive cache in
~0.02 ms and does not flush it. See ARCHITECTURE.md, "Durability".

```bash
./veltrixdb --wal-flush-window-ms 5 --vlog-flush-window-ms 5
```

---

## Batch Operations

Always use `MultiPut` / `MultiGet` when writing or reading multiple keys. They share one network round-trip.

| Method | Throughput | When to use |
|--------|-----------|-------------|
| Individual PUT | ~10K/s per goroutine at 10 ms | Single key |
| MultiPut 1024 keys | ~108K/s (macOS) / ~426K/s (Linux) | Any multi-key write |

The MultiPut row is historical (before the one-`write(2)`-per-batch WAL flusher
and the ordered-index fix). Current server numbers, 8 clients × 1024-key MPUT:
**3.54M keys/s, P99 4.2 ms** over a 1M-key space on macOS (1.45M / 9.6 ms
before the ordered-index fix), and **1.77M keys/s, P99 12.2–12.7 ms** on a
4-CPU CI runner (tmpfs, client on the same host, Go front-end, C++ storage off).

The TCP server automatically coalesces back-to-back PUTs from the same connection into a MultiPut — you get batch performance without client changes.

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

Packing is automatic for `MultiPut` and pipelined PUTs. Single `Put` uses the unpacked lock-free path. Verify: `veltrixdb_vlog_file_bytes / veltrixdb_storage_writes_total ≈ value_size + 24`.

---

## Cache Tuning

```bash
-cache 65536      # 64 GB
--auto-tune       # VeltrixDB picks 85% of available RAM
--read-heavy      # 400 GB preset + scan-resistant tuning
```

Check hit rate: `veltrixdb_cache_hits_total / (hits + misses)`. Small values (≤256 B) resist eviction more — sized workloads with tiny values benefit the most from cache.

---

## Index Memory and Go GC

On cgo builds (the default, including macOS and the Docker image) the index
lives off the Go heap in per-shard C++ tables, so the GC does not scan it.
Measured with 5M keys: full GC **21 ms → 0.27 ms**, settled RSS **168 → 142
B/key**, at a cost of ~19 ns more per cache-miss lookup.

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

## Common Problems

| Symptom | Check | Fix |
|---------|-------|-----|
| Write P99 high | `veltrixdb_storage_write_admission_throttles_total` rising | Read latency too high — see admission control |
| Cache hit rate low | `vlog_gc_skipped_empty_total` rising | Increase `-cache` or enable KV separation |
| GC not running | `vlog_gc_skipped_paused_total` rising | Wait 4 min for stale EWMA to clear |
| `reads_total = 0` with traffic | Old binary | Update — the missing metrics call is fixed |
| False failure alerts (single node) | `fd.SetLocalNode` not called | Fix before `fd.Start()` in your cluster setup |
