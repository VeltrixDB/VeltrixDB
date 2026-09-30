# Same-hardware comparison: VeltrixDB vs Aerospike vs ScyllaDB

This directory is a separate Go module (it pulls in go-ycsb and the Aerospike
and Cassandra client libraries, which the database itself does not need).

| Piece | What it is |
|--|--|
| `cmd/ycsb` | go-ycsb's core workload runner with three drivers: `veltrixdb` (this module, `ycsbdriver/`), `aerospike` and `cassandra` (go-ycsb upstream; ScyllaDB speaks CQL). One runner, one measurement code path for all three. |
| `workloads/` | YCSB A, B, C, D, E, F plus `common` (1 M records × 10 fields × 100 B, whole-record writes). |
| `cmd/vecbench` | ANN benchmark on a real dataset: load, exact ground truth, beam-width sweep → recall@k, QPS, p50/p99, JSON + Markdown. VeltrixDB driver only (see below). |
| `docker-compose.yml`, `aerospike.conf` | One service per database with equal CPU / memory limits and data on `BENCH_DATA`. |
| `compare.sh` | Runs the databases one at a time: load, each workload, `results/<ts>/summary.md`. |

## What was verified, and what was not

- Verified where this was written (macOS, no Docker): the VeltrixDB YCSB
  driver against a real server for workloads A–F, `compare.sh` end to end with
  `START=0 DBS=veltrixdb`, and `vecbench` on GloVe-100.
- Only compiled, not run: the Aerospike and ScyllaDB drivers (upstream
  go-ycsb code), `docker-compose.yml` and `aerospike.conf`. Check them on the
  benchmark machine first.
- Not included: a vector driver for Aerospike Vector Search or ScyllaDB vector
  search. Their client APIs could not be exercised here, and VectorDBBench has
  no client for either. `cmd/vecbench` has a `vectorDB` interface to add one.

## Running it

```bash
# On the machine under test (Linux, NVMe), with Docker and Go ≥ 1.25:
export BENCH_DATA=/mnt/nvme0/bench BENCH_CPUS=8 BENCH_MEM=16g
./compare.sh                                   # all three, workloads a b c f
RECORDS=10000000 OPS=10000000 THREADS=128 WORKLOADS="a b c d f" ./compare.sh

# Vectors (VeltrixDB):
../../scripts/ann-dataset.py glove-100-angular /mnt/nvme0/ann --train 1000000 --test 1000
VECTORS=1 ANN_DIR=/mnt/nvme0/ann VEC_ARGS="-quant pq -graph disk" ./compare.sh
```

## Making the numbers comparable

Publish a result only with all of these stated next to it:

1. **Same machine, same disk, one database running at a time.** The client on
   a second machine is better still (`START=0` + addresses).
2. **Durability.** VeltrixDB ACKs after `fdatasync` of WAL and VLog (default).
   ScyllaDB's default `commitlog_sync: periodic` ACKs before the write is on
   disk; set `commitlog_sync: batch` for an equal comparison, or report both.
   Aerospike ACKs from its write buffer (`flush-max-ms`); `commit-to-device`
   is documented as an Enterprise feature — check your edition.
3. **Warm-up and size.** Load more data than the page cache / database cache
   holds if the point is disk performance; run each workload long enough
   (≥ 5 min) for compaction and GC to show up.
4. **Replication factor 1** for single-node numbers (as configured here).
5. **Versions** of all three (`environment.txt` records the host; add image tags).

## Workload notes

- Updates write the whole record (`writeallfields=true`) so every database does
  one write per update. With partial updates VeltrixDB would read-modify-write.
- Workload E uses VeltrixDB's `RANGE` over the ordered key index.
