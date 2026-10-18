# Quickstart: install, run, benchmark

From nothing to a running server and a benchmark table in about five
minutes. Every step is one command, and every `make` target wraps a script
you can also run by hand (`make help` lists them).

| I want to… | Command | Time | Needs |
|--|--|--|--|
| Run one node | `make run` or `docker run …` | 1 min | Go 1.21+, or Docker |
| Run a 3-node cluster on my laptop | `make cluster-up` | 1 min | Go |
| Get a quick benchmark table | `make bench-quick` | ~1 min | Go |
| Run YCSB (A/B/C/F) | `make bench-ycsb` | 2–5 min | Go 1.25+ |
| Run YCSB on a 3-node cluster | `make cluster-up && make bench-ycsb-cluster` | 3–5 min | Go 1.25+ |
| Run the gated storage benchmark | `make bench` | ~4 min | Go |
| Compare with Aerospike and ScyllaDB | `make bench-compare` | 30+ min | Linux, Docker, NVMe |
| Get real multi-host numbers | `make bench-gcp PROJECT=<sandbox>` | ~1 h | gcloud, a GCP project you may spend in |
| Deploy on Kubernetes | Helm chart or operator, [below](#5-kubernetes) | 10 min | a cluster |

---

## 1. Install

Pick one.

**Docker**: no toolchain needed.

```bash
docker run --rm -p 9000:9000 -p 2112:2112 ghcr.io/veltrixdb/veltrixdb:latest
```

The image is `linux/amd64` only. On Apple silicon Docker Desktop runs it
under emulation: it works, but it is slow, so build from source if you want
numbers on a Mac.

**From source**: macOS or Linux.

```bash
git clone https://github.com/VeltrixDB/veltrixdb.git && cd veltrixdb
make build        # → bin/veltrixdb (server), bin/veltrix (CLI), bin/loadtest
make run          # one node on 127.0.0.1:9000, data in ./dev-data
```

- **Go:** Go 1.21 or newer gets the off-heap native index. With a C++
  compiler present, Go builds with cgo by default, which is what the Docker
  image runs. 1.19/1.20 builds work too, with the Go map index.
  `CGO_ENABLED=0 make build` gives a pure-Go binary.
- **Benchmarks:** the YCSB targets build `bench/compare`, a separate module
  that needs Go 1.25+.
- **Release tarball:** `./scripts/build.sh --output ./dist` builds one,
  C++ engine included, on Linux.

**Production hosts (Linux):** install `scripts/sysctl.conf` as
`/etc/sysctl.d/99-veltrixdb.conf`. The native index needs
`vm.max_map_count ≥ 262144` at large key counts. Then give the server every
NVMe disk:

```bash
bin/veltrixdb -addr :9000 -data-dirs /mnt/nvme0,/mnt/nvme1,/mnt/nvme2,/mnt/nvme3 -cache 65536
```

## 2. Talk to it

```bash
echo -e "PUT hello world\nGET hello\nPING" | nc 127.0.0.1 9000   # → OK, world, PONG

bin/veltrix put user:1 alice
bin/veltrix get user:1
bin/veltrix status          # health, ops, cache, WAL, GC in one screen
bin/veltrix top             # live dashboard

curl -s 127.0.0.1:2112/readyz      # ready
curl -s 127.0.0.1:2112/metrics | grep veltrixdb_storage_writes_total
```

Client SDKs (Go, Java, Python, Node.js, Rust and C++) live in the
[VeltrixDB-Client](https://github.com/VeltrixDB/VeltrixDB-Client) repository.

## 3. A 3-node cluster on one machine

```bash
make cluster-up                    # raft: n1 :9000, n2 :9010, n3 :9020
make cluster-up MODE=replicated    # or primary-copy replication (quorum ACK)
make cluster-status                # nodes, pids, raft leader
make cluster-down                  # stop, keep data
make cluster-clean                 # stop, delete data
```

- **Ports:** node *i* listens on `9000 + 10·(i−1)`. Its HTTP port
  (metrics, `/healthz`, `/admin`) is that port + 4, so n1's is `:9004`.
- **Files:** data and logs go to `./.local-cluster/` (git-ignored).
  `EXTRA_FLAGS="-raft-pipeline"` passes extra flags to every node.
- **Writes in raft mode:** writes go to the leader. A follower answers
  `ERR MOVED <leader>`, which the SDKs and the YCSB driver follow.
- **Membership:** to add or remove nodes on a running cluster, see
  [docs/node-lifecycle.md](docs/node-lifecycle.md).

All three nodes share one CPU and one disk, so this setup is for checking
behaviour, not for performance numbers (see step 4).

## 4. Benchmark

### `make bench-quick`: one table in about a minute

This target:
- builds the server and `loadtest`;
- starts a throwaway server on a temporary directory;
- runs four phases;
- prints one table, then deletes everything.

```
VeltrixDB quick bench — Darwin arm64, 64 workers, 128 B values, 15s per phase
phase                             ops/s          P50          P99   errors
MPUT x256 (keys/s)              4852888     3.015 ms     9.600 ms        0
PUT (durable)                     47710     1.294 ms     2.545 ms        0
GET                               49690     1.248 ms     1.689 ms        0
mixed: reads                      34974     1.230 ms     1.412 ms        0
mixed: writes                     14958     1.336 ms     2.541 ms        0
```

(An M-series laptop with a same-host client. See "Reading the numbers"
below before quoting it.)

Knobs: `DURATION` (15 s per phase), `CONCURRENCY` (64), `VALUE_SIZE` (128),
`NUM_KEYS` (200000), `BATCH_SIZE` (256), `READ_RATIO` (0.7) and
`SERVER_FLAGS`. `ADDR=host:9000` benchmarks a server that is already
running instead of starting one:

```bash
DURATION=30 CONCURRENCY=128 VALUE_SIZE=1024 make bench-quick
ADDR=10.0.0.5:9000 ./scripts/quick-bench.sh
```

### `make bench-ycsb`: standard YCSB, no Docker

This target:
- starts a throwaway node on `:9400`;
- loads `RECORDS` records;
- runs each workload;
- writes `bench/compare/results/<timestamp>/summary.md` (throughput, avg,
  P99 and P99.9 per operation) and stops the node.

```bash
make bench-ycsb                                              # a b c f, 100K records, 32 threads
make bench-ycsb WORKLOADS="a b c d e f" RECORDS=1000000 OPS=1000000 THREADS=64
```

### `make bench-ycsb-cluster`: YCSB on the local 3-node cluster

```bash
make cluster-up                 # or MODE=replicated
make bench-ycsb-cluster
make cluster-clean
```

The summary adds a few things to the single-node results:
- each node's role and leader;
- a per-node replica check: `keys=… missing=0` on every node means RF 3
  held;
- the client routing: redirects to the raft leader, failovers and the reads
  served per node.

### `make bench`: the gated storage benchmark

`scripts/bench.sh` runs build, bulk load, cool-down, reads, mixed and write
stress, then fails (non-zero exit) on two gates: packing density and VLog GC
emergency runs. Re-run it after any storage-engine change. The variables
(`NUM_KEYS`, `DATA_DIRS`, `CACHE_MB`, …) are in
[BENCHMARKING.md](BENCHMARKING.md).

### `make bench-compare`: against Aerospike and ScyllaDB

This runs the same go-ycsb runner, the same workloads and the same
CPU/memory limits on the same machine, one database at a time:

```bash
export BENCH_DATA=/mnt/nvme0/bench BENCH_CPUS=8 BENCH_MEM=16g
make bench-compare
```

It needs Linux, Docker and an NVMe mount. Fairness notes and the 3-node
variant are in [bench/compare/README.md](bench/compare/README.md).

### `make bench-gcp`: real multi-host numbers

The script creates the hardware, runs every configuration and deletes it
again:
- **Hardware:** 3 × n2-standard-8 servers with local NVMe, plus 1 client VM,
  in a compact placement group.
- **Configurations:** raft without pipelining, raft with pipelining, and
  replicated with quorum ACK. The client gets
  `bench/compare/results/gcp-<ts>/combined.md`.
- **Cleanup:** everything is deleted at the end, even on failure or Ctrl-C.

```bash
make bench-gcp-dry-run                      # print the gcloud commands, create nothing
make bench-gcp PROJECT=my-sandbox-project   # the real run: costs money
```

Use a sandbox project you are allowed to spend in. `KEEP=1` keeps the VMs
for debugging, and `CLEANUP_ONLY=1` deletes the leftovers of an interrupted
run. Details: [bench/compare/README.md](bench/compare/README.md).

### Reading the numbers

- **macOS write numbers are optimistic.** Darwin's `fsync` returns at the
  drive cache (about 0.02 ms). Linux `fdatasync` on NVMe takes about
  0.2–0.5 ms, so durable-write numbers from a Mac are not a lower bound for
  Linux.
- **The client shares the machine.** In every local target the client
  competes with the server for CPU. Real numbers need a separate client host
  (`make bench-gcp`).
- **Local clusters share a disk.** Three nodes doing fsyncs on one SSD
  measure that SSD, which is why raft numbers on a laptop are far below
  multi-host ones.
- **Batch writes count keys.** The MPUT row is keys/s; its latency is per
  request of `BATCH_SIZE` keys.
- **Say what you ran.** Quote a number together with the hardware, value
  size, concurrency and durability setting. `summary.md` records them under
  "Configuration".

## 5. Kubernetes

**Helm** ([VeltrixDB-Helm-Chart](https://github.com/VeltrixDB/VeltrixDB-Helm-Chart)).
For production, follow its README: a local-NVMe StorageClass and GKE node
pools created with `--local-ssd-interface=NVME`. To try it on kind, minikube
or Docker Desktop:

```bash
git clone https://github.com/VeltrixDB/VeltrixDB-Helm-Chart.git && cd VeltrixDB-Helm-Chart
helm install veltrixdb . -n veltrixdb --create-namespace -f examples/values-dev.yaml
kubectl -n veltrixdb rollout status statefulset/veltrixdb
```

**Operator** ([VeltrixDB-Kubernetes-Operator](https://github.com/VeltrixDB/VeltrixDB-Kubernetes-Operator)).
To try it, run the controller from your workstation against the current
kube context:

```bash
git clone https://github.com/VeltrixDB/VeltrixDB-Kubernetes-Operator.git && cd VeltrixDB-Kubernetes-Operator
make install && make run       # CRD + controller in the foreground
kubectl create namespace veltrixdb-dev
kubectl apply -f config/samples/veltrix_v1alpha1_veltrixcluster_dev.yaml
```

**Benchmark inside the cluster.** Port-forwarding measures the tunnel, so
run the load generator in a pod:

```bash
kubectl -n veltrixdb run vxbench --rm -it --restart=Never --image=golang:1.22 -- \
  go run github.com/VeltrixDB/veltrixdb/cmd/loadtest@main \
    --addr=veltrixdb-0.veltrixdb-headless:9000 --proto=binary \
    --mode=write --batch-size=256 --concurrency=32 --duration=30 --value-size=128
```

Use `--mode=read` or `--mode=mixed` afterwards. In raft mode, point
`--addr` at the leader (`veltrix nodes`); `loadtest` does not follow `MOVED`.

## Troubleshooting

| Symptom | Fix |
|--|--|
| `bind: address already in use` | Another server is on the port: `make cluster-down`, or change `ADDR` / `BASE_PORT` / `PORT` |
| `ErrDataDirLocked` at startup | Another process owns that data dir (one engine per dir). Stop it, or use another `-data` |
| `go: … requires go >= 1.25` in a YCSB target | `bench/compare` needs Go 1.25+. The server itself builds with 1.19+ |
| A raft node answers `ERR MOVED -` | No leader yet (election running). Retry after a second; `make cluster-status` shows the leader |
| Docker on a Mac is slow | The image is amd64 only. Build from source instead |
| `ENOMEM` / panic in the native index at large key counts | `vm.max_map_count` is below 262144. Install `scripts/sysctl.conf` |

Further reading: [README.md](README.md) (features, flags, modes),
[BENCHMARKING.md](BENCHMARKING.md) (methodology, reference numbers),
[docs/](docs/README.md) (replication, monitoring, backup, node lifecycle).
