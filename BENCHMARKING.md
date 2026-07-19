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
