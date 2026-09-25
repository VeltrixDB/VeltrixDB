# Benchmarking VeltrixDB

`scripts/bench.sh` runs a 6-phase sequenced workload and reports go/no-go on two hard gates: **packing density** and **GC emergency runs**. Re-run it after any storage engine change.

---

## Run It

```bash
# macOS / Linux dev (uses /tmp, 1M keys)
./scripts/bench.sh

# Linux production-class (n2-highmem-64, 8 NVMe disks)
DATA_DIRS=/mnt/nvme0,...,/mnt/nvme7 \
RAW_VLOGS=/dev/nvme0n1,...,/dev/nvme7n1 \
CACHE_MB=409600 NUM_KEYS=10000000 \
CONCURRENCY=512 BULK_DUR=120 STRESS_DUR=300 \
  ./scripts/bench.sh
```

---

## Key Env Vars

| Var | Default | Notes |
|-----|---------|-------|
| `VALUE_SIZE` | 128 | Match your real value distribution |
| `NUM_KEYS` | 1 000 000 | Use 10M for production-scale |
| `BATCH_SIZE` | 1024 | Higher = better packing density |
| `CONCURRENCY` | 64 | Use 512 on multi-core hosts |
| `BULK_DUR` | 30 s | 120–300 s for production verification |
| `STRESS_DUR` | 60 s | 600 s to detect GC pressure |
| `WAL_WINDOW_MS` | 5 | 10 for default, 2 for low latency |
| `CACHE_MB` | 1024 | 409600 on n2-highmem-64 |
| `RAW_VLOGS` | — | Raw block-device VLog (Linux + CAP_SYS_RAWIO) |

---

## Pass Criteria

| Gate | Condition | What failure means |
|------|-----------|-------------------|
| Density | `bytes/record ≤ 1.2 × (24 + value_size)` | Packing not engaged — use `--batch-size > 1` |
| GC emergency | `vlog_gc_emergency_runs Δ == 0` | Write rate exceeds GC throughput |

---

## Reference Numbers

> **Provenance:** added at v1.1.0 (2026-07-19); the Linux table is a
> projection, not a measurement. They predate the one-`write(2)` WAL
> flusher, one `pwrite` per VLog batch, the binary WAL, the cache write-around
> for `MultiPut`, the ordered-index overwrite skip and the native index, all
> of which change write-path numbers. The macOS figures also sit on a `fsync`
> that does not flush the drive cache (see ARCHITECTURE.md, Durability).
> Re-run before quoting them.

### macOS M-series (dev)
| Metric | Value |
|--------|-------|
| MPut throughput (1024 batch) | ~360K writes/s |
| Bytes/record at 128 B | ~160 B (25× density) |
| Single-Put P99 (5 ms window) | ~8 ms |

### Linux n2-highmem-64 (8 NVMe) — projected
| Metric | Value |
|--------|-------|
| MPut throughput (1024 batch) | ~3M writes/s |
| Single-Put P99 (5 ms window) | ~5.2 ms |
| GET P99 cache-hit | ~52 µs |
| 1B keys × 128B disk total | ~149 GB (vs 4 TB unpacked) |

---

## Quick Diagnostics

```bash
# Watch bytes/record live during phase 2
watch -n 2 'W=$(curl -s :2112/metrics | awk "/^veltrixdb_storage_writes_total /{print \$2}"); \
            B=$(curl -s :2112/metrics | awk "/^veltrixdb_vlog_file_bytes /{print \$2}"); \
            python3 -c "print(f'"'"'bytes/record={$B/$W:.1f}'"'"')"'

# GC and latency at a glance
curl -s :2112/metrics | grep -E "gc_(run|emerg|skipp)|latency"
```

---

## Network front-ends and storage configurations (`net-bench.sh`)

`scripts/net-bench.sh` starts a fresh server per combination and runs batch
write (8 × 1024-key MPUT, `BATCH_DURATION`, default 10 s), read (64 binary
GET clients) and mixed (70/30) workloads, then prints one markdown table
(also to `$GITHUB_STEP_SUMMARY`). The `Net front-end` GitHub workflow runs it
on Linux, where the io_uring code exists.

```bash
# Front-ends (NET:N = N event loops for a C++ front-end)
NETS="go uring poll uring:2" ENGINES=native WINDOWS=5 ./scripts/net-bench.sh

# Storage configurations under the Go front-end
ENGINES="native bridge sqpoll map nocgo" NETS=go WINDOWS="5 1" ./scripts/net-bench.sh
```

Defaults: `NETS="go uring"`, `ENGINES=native`, `WINDOWS=5`, `DURATION=15` s
per read/mixed workload, `NET_THREADS` = CPU count. The C++ front-ends need
the binary protocol, so the script drives reads and mixed with
`loadtest --proto=binary`.

| `ENGINES` value | Configuration |
|---|---|
| `native` | native C++ index, io_uring VLog bridge off — the default build |
| `inline` | `native`, but the C++ front-end reads disk on its loop thread (`VELTRIXDB_NET_DEFER_READS=0`) |
| `bridge` / `sqpoll` | `native` + `VELTRIXDB_URING_BRIDGE=on` / `sqpoll` |
| `map` | Go map index, bridge off |
| `nocgo` | `VELTRIXDB_DISABLE_CGO_ENGINE=1`: map index, no C++ storage layer |

Things to know when reading the table:

- **Every overwrite appends to the WAL and VLog**, and nothing reclaims space
  during a run. One combination leaves ~5-10 GB, so each data dir is deleted
  when its combination finishes. On a tmpfs `DATA_ROOT` of 6 GB, keeping them
  made every later write fail with ENOSPC.
- **Each workload runs under a watchdog** (duration + `WATCHDOG_SLACK`, default
  60 s). On a hang the script dumps native thread stacks (gdb) and the Go
  goroutine dump (SIGQUIT), records a FAILED row, and moves on.
- **C++ front-ends log per-stage latency at shutdown**, and the script copies
  it into the summary. The buckets are log2, so `p99<=8ms` means the 99th
  percentile is in [4, 8) ms. `cb_enter` is the time a loop thread waits for a
  Go P before `vxnfExec` runs. When it is high, the loops are competing with Go
  for CPUs; try fewer loops (`uring:2`).
- **The client runs on the same host and the data sits on tmpfs.** Compare rows
  with each other, not with other hardware. With fdatasync on RAM, the
  io_uring bridge has no real I/O to overlap.
- **The C++ front-end is not faster overall.** On the 4-CPU CI runner
  (2026-09-25, before disk-bound reads were deferred) it cut read P50 to
  154 µs from 286 µs under `--net=go`, and poll read throughput was ~10%
  higher, but read P99 was worse (~3.0 vs 1.8 ms), batch writes ~12% lower,
  and io_uring did not beat poll. The best overall row was the Go front-end
  with no C++ storage layer. On macOS only poll with one loop runs, and
  loopback caps round trips at ~60K/s for both — measure on Linux.

## Ordered key index: measuring whether you need it

Every write maintains a skiplist mirroring all live keys so `RANGE` /
`SCANCUR` can run in O(log N + limit). Point-lookup-only workloads pay for it
and get nothing. `--disable-ordered-index` turns it off; `RANGE` and `SCANCUR`
then return `ErrOrderedIndexDisabled` and everything else is unaffected.

Measured cost on darwin/arm64, 18 cores (2026-09-21, before overwrites of
a live key stopped re-inserting into the skiplist — that change took server
batch writes 1.45M → 3.54M keys/s, so the ON column overstates today's cost
for overwrite-heavy loads):

| | ordered ON | ordered OFF | delta |
|---|---|---|---|
| `MultiPut_1024` wall (benchstat, n=6) | 3.482 ms | 3.304 ms | **−5.09%** (p=0.002) |
| `MultiPut_1024` allocations | 9416 | 7368 | **−21.75%** (p=0.002) |
| Resident RAM per live key | 90.7 B | 0 B | **−90.7 B/key** |

**The memory figure is the one that matters at scale: ~85 GB at 1 billion
keys.** Give it to `--cache` instead. A cache hit is ~90 ns against ~80 µs for
an NVMe miss, so hit-rate buys far more than any CPU micro-optimisation.

End-to-end `bench.sh` on the same machine, **single run each — indicative
only, not significant**:

| Phase | ordered ON | ordered OFF |
|-------|-----------|-------------|
| Bulk load (MPut) | 583,840 ops/s | 591,955 ops/s |
| Read-only | 53,789 ops/s | 53,979 ops/s |
| Mixed 70R/30W | 18,701 ops/s | 18,753 ops/s |
| Write stress | 575,071 ops/s | 595,563 ops/s |
| Read P99 | 750.12 µs | 717.04 µs |

Both runs passed the density and GC-emergency gates. The 1–4% deltas are
within run-to-run variance for an fsync-bound macOS run; the microbenchmark
above is the rigorous measurement. Re-measure on Linux NVMe before deciding.

To A/B it yourself:

```bash
EXTRA_SERVER_FLAGS="--disable-ordered-index" ./scripts/bench.sh
```
