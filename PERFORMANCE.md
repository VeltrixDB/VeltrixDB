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
rate(veltrixdb_writes_total[1m])                                    # write throughput
rate(veltrixdb_reads_total[1m])                                     # read throughput

rate(veltrixdb_cache_hits_total[5m]) /
  (rate(veltrixdb_cache_hits_total[5m]) + rate(veltrixdb_cache_misses_total[5m]))   # hit ratio

rate(veltrixdb_writes_total[1m]) / rate(veltrixdb_wal_flushes_total[1m])            # WAL amortisation

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
