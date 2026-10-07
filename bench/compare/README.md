# Same-hardware comparison: VeltrixDB vs Aerospike vs ScyllaDB

This directory is a separate Go module (it pulls in go-ycsb and the Aerospike
and Cassandra client libraries, which the database itself does not need).

| Piece | What it is |
|--|--|
| `cmd/ycsb` | go-ycsb's core workload runner with three drivers: `veltrixdb` (this module, `ycsbdriver/`), `aerospike` and `cassandra` (go-ycsb upstream; ScyllaDB speaks CQL). One runner, one measurement code path for all three. |
| `workloads/` | YCSB A, B, C, D, E, F plus `common` (1 M records × 10 fields × 100 B, whole-record writes). |
| `cmd/vecbench` | ANN benchmark on a real dataset: load, exact ground truth, beam-width sweep → recall@k, QPS, p50/p99, JSON + Markdown. VeltrixDB driver only (see below). |
| `cmd/cqlexec` | Runs CQL with the same gocql driver go-ycsb uses (keyspace creation, version) — no `cqlsh` or Docker needed on the client. |
| `cmd/keycount` | After a 3-node VeltrixDB run: how many YCSB records each node serves, and how many it is missing (RF 3 ⇒ must be 0). |
| `docker-compose.yml`, `aerospike.conf` | One single-node service per database with equal CPU / memory limits and data on `BENCH_DATA`. |
| `docker-compose.cluster.yml`, `aerospike-cluster-{1,2,3}.conf` | Three nodes per database on one machine (fixed IPs on a compose network) — functional check of the `NODES=3` path only. |
| `compare.sh` | Runs the databases one at a time: load, each workload, `results/<ts>/summary.md` + `environment.txt`. |

## What was verified, and what was not

- Verified where this was written (macOS, no Docker): the VeltrixDB YCSB
  driver against a real server for workloads A–F, `compare.sh` end to end with
  `START=0 DBS=veltrixdb`, and `vecbench` on GloVe-100.
- 3-node VeltrixDB (2026-10-06, macOS, local processes on one laptop):
  `START=0 NODES=3 DBS=veltrixdb` against a real 3-node `--mode=raft` cluster
  and a real 3-node `--mode=replicated` cluster, the driver's MOVED handling,
  and a raft leader killed mid-run (3,000 workload-A ops, 0 errors, longest
  op 2.1 s while the election ran, every record still on both survivors —
  run with `--auto-rebalance=false`, before the auto-rebalance fix; see
  below). The driver's routing logic is also unit
  tested against fake servers (`ycsbdriver/cluster_test.go`).
- Only compiled / syntax-checked, not run: the Aerospike and ScyllaDB drivers
  (upstream go-ycsb code), `cmd/cqlexec`, both compose files (YAML parses),
  the Aerospike confs, and ScyllaDB's `--commitlog-sync` pass-through. Check
  them on the benchmark machines first.
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

`compare.sh` defaults: `DBS="veltrixdb aerospike scylla"`, `WORKLOADS="a b c f"`,
`RECORDS=1000000`, `OPS=1000000`, `THREADS=64`, `NODES=1`, `START=1` (docker
compose up / stop each database in turn). With `START=0` it uses databases
that are already running at `VELTRIX_ADDR(S)` (default `127.0.0.1:9000`),
`AEROSPIKE_HOST(S)` and `SCYLLA_HOSTS` (default `127.0.0.1`). The full list of
variables is at the top of `compare.sh`.

`scripts/ann-dataset.py` needs `h5py` and `numpy`. It downloads
`http://ann-benchmarks.com/<name>.hdf5` with its own User-Agent
(ann-benchmarks.com answers 403 to Python-urllib's default), makes up to 4
attempts with a growing back-off, and writes through a `.part` temp file so
a failed download never leaves a truncated `.hdf5` behind. It writes
`<name>.train.fvecs` and `<name>.test.fvecs`; ground truth is not copied
(the consumers compute exact neighbours).

## 3-node clusters, replication factor 3 (`NODES=3`)

`NODES=3` sets RF 3 everywhere and needs three addresses per database:

| | VeltrixDB | ScyllaDB | Aerospike CE |
|--|--|--|--|
| Replication | RF 3 is fixed in the server (`cluster/partition_map.go`) | keyspace `ycsb`, `NetworkTopologyStrategy`, `replication_factor 3` (created / ALTERed by `compare.sh`) | namespace `test`, `replication-factor 3` (conf file) |
| Write ACK | `raft`: committed on 2 of 3 raft logs, through the leader. `replicated`: local write, then `--consistency` (`quorum` = 2 of 3, default in compose / `VELTRIX_CONSISTENCY`) | QUORUM (2 of 3) — go-ycsb v1.0.3 hard-codes `gocql.Quorum` (`db/cassandra/db.go`), there is no property | COMMIT_ALL (all 3), the aerospike-client-go v1.35 default; go-ycsb exposes no property |
| Read | `raft`: local on whichever node the thread is homed on (a follower may be stale); `--linearizable-reads`: leader only. `replicated`: local | QUORUM | master copy |
| Consistency model | raft: linearizable writes | tunable (QUORUM/QUORUM ⇒ read-your-writes) | AP; `strong-consistency` (linearizable, roster) is **Enterprise only** |
| Client routing | see below | gocql round-robin over all nodes (that gocql version is not token- or shard-aware) | partition-aware: each key goes to its master (the driver takes one seed host and discovers the others) |

These are not identical guarantees — state them next to any number, as
`summary.md` does under "Configuration".

**VeltrixDB driver routing** (`veltrixdb.addr` with several addresses):
each YCSB thread has its own connection, and threads are homed round-robin
over the list (`veltrixdb.spread=true`, `VELTRIX_SPREAD`). Reads go to the
home node. Writes start at the home node; the first `MOVED <leader>` reply
from a raft follower is followed, and the leader address is then shared by
all threads, so a raft run costs about one redirect per thread rather than
one per write. In `replicated` mode nothing answers MOVED and writes stay
spread. A connection error fails the op over to the next listed node
(immediately, then with back-off), `MOVED -` (election running) waits and
retries; MOVED hops in a row are capped by `veltrixdb.retries` (3) and waits
by `veltrixdb.retrytime` (3 s). Every run's raw output ends with a
`# veltrixdb:` line — redirects, failovers, errors, last error, threads per
home node, and reads / writes answered per node (an update's GET counts as a
read) — copied into `summary.md`. With `spread=false` every thread is homed
on the first address. After the run `compare.sh` runs `cmd/keycount`: every
node must hold every record.

Why this default: in raft mode only the leader can take a write, so sending
writes to followers would just add a hop; reads are served locally by any
node, so spreading them uses all three machines (as ScyllaDB and Aerospike
do). The price is that a follower read may be stale — if you need
linearizable reads, run the servers with `--linearizable-reads`
(`VELTRIX_LINEARIZABLE_READS=true` for compose): followers then redirect
reads too, and every op ends up on the leader.

**Known VeltrixDB cluster issues found while building this** (2026-10-06;
state them with any result until fixed):

- **Auto-rebalance deleted data on a node failure — fixed (CLAUDE.md
  invariant 62, CHANGELOG [Unreleased]).** Builds before the fix, with the
  default `--auto-rebalance=true`, made each node run
  `TransferAgent.MigrateToNewOwners` when a node was marked FAILED; it sent
  every key whose ring owner is another node to that node and **deleted it
  locally** — outside the raft log / replication stream, although with RF 3
  on 3 nodes every node must keep every key. Observed: after killing the raft
  leader of a loaded 3-node cluster, the survivors held 5,435 and 7,875 of
  20,000 records, and the new leader answered `key not found` for the rest.
  Now migration never runs in `--mode=raft` (`--auto-rebalance` is a logged
  no-op there), node failure / recovery never starts a migration in either
  mode, and a membership-change migration keeps every key on its RF
  replicas. `tests/integration/rebalance_test.go` repeats the leader kill
  with the default flag. The commands and compose files below still pass
  `--auto-rebalance=false`: it changes nothing on a fixed build (the
  server's membership is static, so it never migrates) and keeps a run
  against an older build safe. A YCSB read of a missing key counts as a
  success, which is why `compare.sh` still runs `keycount`.
- **Raft writes were slow before the incremental raft log (fixed; CLAUDE.md
  invariant 63, CHANGELOG [Unreleased]).** Older builds rewrote and fsynced
  the whole retained log (up to 8192 entries of ~1.1 KB) on every flush. Now a
  flush appends only its new records to `raft_log.dat` and fsyncs once.
  Measured locally (one laptop, 3 processes, 16 threads, 20 K records, one run
  each, same machine): load 70 → 1,046 inserts/s (avg 229 → 15 ms), workload A
  87 → 1,212 ops/s, B 769 → 6,795 ops/s. Raft commit latency is still above
  `replicated --consistency=quorum`. Record which build you benchmark.
- **Raft pipelining (2026-10, CLAUDE.md invariant 63).** Followers now write +
  fsync outside the node lock and acknowledge only durable entries; the leader
  keeps up to 8 AppendEntries in flight per follower over one multiplexed
  stream and replicates before its own fsync. Same laptop, 3 local processes,
  16 threads, 20 K records / 20 K ops, medians of 2 runs: raft load 1,038 →
  560 inserts/s (p99 33 → 44 ms), A 1,058 → 1,022 ops/s (update p99 78 → 47
  ms), B 5,630 → 10,547 ops/s, C ~47.5 K both; `replicated
  --consistency=quorum` reference: load 1,590 inserts/s (p99 12 ms), A 2,940,
  B 25,700. **The laptop load figure is not a cluster result**: raft fsyncs
  with `F_FULLFSYNC` (os.File.Sync on darwin, a real drive-cache flush, ~3.6
  ms) while the storage engine uses plain `fsync(2)`, which on macOS does not
  flush the drive cache (~3 µs) — so replicated mode pays almost nothing for
  durability here, and all three raft nodes share one drive whose cache
  flushes serialise. Overlapping the nodes' fsyncs cannot help on one drive,
  and three continuously flushing syncers slow the engine's own fsyncs in the
  apply path (leader ApplyBatch 2.8 ms for 12 entries before, 7.8 ms for 5
  after). With each node's raft fsync replaced by an independent 1 ms delay +
  `fsync(2)` (a scratch build modelling three machines with their own disks;
  not shipped) the same load goes 3,015 → 5,630 inserts/s, p99 17 → 4 ms. Run
  the multi-host runbook below for real numbers.

### `START=1 NODES=3`: one machine, functional check only

`docker-compose.cluster.yml` runs three containers per database (VeltrixDB
`veltrixdb-{1,2,3}` with `--peers` and a per-run random cluster secret,
Aerospike with mesh heartbeat, ScyllaDB seeded from node 1 with nodes joining
one at a time), each limited to `BENCH_CPUS` (default 2) / `BENCH_MEM`
(default 4g). It exists to check that clusters form, RF 3 applies and the
drivers route — **not for performance**: all three nodes of a
database share one CPU, memory bus and disk, and replication is loopback.
`environment.txt` says so. Fixed IPs (172.28.0.0/24) are reachable from the
host on Linux only; with Docker Desktop run the client in a container on the
`bench` network.

```bash
NODES=3 VELTRIX_MODE=raft ./compare.sh
NODES=3 VELTRIX_MODE=replicated VELTRIX_CONSISTENCY=quorum DBS=veltrixdb ./compare.sh
```

## Multi-host runbook (the real 3-node run)

Seven machines of the same type in one zone and one placement group (AWS
cluster placement group, GCP compact placement policy): three per database
under test, run one database at a time on the same three hosts if you have
only three, and a fourth machine — same zone, same placement group, a NIC at
least as fast as the servers' — for the client. Example addresses below:
servers `10.0.0.11–13`, client `10.0.0.50`. Put data on the local NVMe of
each server, nothing else running on them.

### Automated VeltrixDB-only run on GCP (`gcp-multihost.sh`)

`gcp-multihost.sh` does the VeltrixDB part of this runbook end to end. It
cross-compiles the server and creates 3 server VMs, each with its own local
NVMe SSD, plus 1 client VM. All four go in one zone with a compact placement
policy and a firewall rule for the VeltrixDB ports. For each configuration in
`CONFIGS` (`raft-off`, `raft-on`, `replicated`) it wipes the data, restarts
the cluster and runs `compare.sh` from the client, `RUNS` times. It copies
`results/gcp-<ts>/` back, with a `combined.md` and every disk's
synchronous-write rate, then **deletes every resource it created**, also on
Ctrl-C.

```bash
PROJECT=<a project you may create VMs in> ./gcp-multihost.sh
DRY_RUN=1 PROJECT=x ./gcp-multihost.sh          # print the gcloud commands only
CLEANUP_ONLY=1 PROJECT=<same> ./gcp-multihost.sh   # if a run was killed hard
```

Defaults: `n2-standard-8`, `us-central1-a`, 1M records and 1M ops, 64
threads, workloads `a b c`, 2 runs. The script header lists every variable.
It covers VeltrixDB only. Scylla and Aerospike still follow the manual steps
below.

### Firewall (inbound on the servers; open them between servers and from the client)

| Database | Ports (TCP) |
|--|--|
| VeltrixDB | 9000 client (from client + peers), 9001 replication, 9002 raft, 9003 gossip, 9005 transfer (peers), 2112 metrics / `/admin/cluster` (from client, optional) |
| ScyllaDB | 9042 CQL and 19042 shard-aware CQL (from client), 7000 inter-node (7001 with TLS), 7199 JMX / nodetool, 10000 REST API (local), 9180 Prometheus (optional) |
| Aerospike | 3000 client (from client + peers), 3001 fabric, 3002 heartbeat (mesh), 3003 info (peers) |

### VeltrixDB

Same binary and the same secret file (≥ 16 bytes) on all three hosts. The
addresses in `--peers` are what MOVED hands to the client, so use the routable
IPs, not `0.0.0.0`. RF is 3 by construction.

```bash
head -c 32 /dev/urandom | base64 > /etc/veltrixdb/cluster.secret   # once, copy to all three
PEERS=n1@10.0.0.11:9000,n2@10.0.0.12:9000,n3@10.0.0.13:9000

# raft (host N = 1, 2, 3):
./veltrixdb --mode=raft --node-id=nN --addr=10.0.0.1N:9000 --peers=$PEERS \
  --data-dirs=/mnt/nvme0,/mnt/nvme1 --cache=65536 \
  --metrics-addr=10.0.0.1N:2112 --cluster-secret-file=/etc/veltrixdb/cluster.secret \
  --auto-rebalance=false            # only needed on builds before the fix; see "Known VeltrixDB cluster issues"
  # add --linearizable-reads for leader-only, linearizable reads
  # add --raft-pipeline=true for pipelined replication (default false); run both
  # and pass VELTRIX_RAFT_PIPELINE=true|false to compare.sh so the summary says which

# replicated: same, with
  --mode=replicated --consistency=quorum      # or strong (all 3) / eventual (async)

curl -s 10.0.0.11:2112/admin/cluster          # mode, raft leader, 3 nodes ACTIVE
```

### ScyllaDB

Install the same version on all three (`scylla_setup`, which runs
`scylla_io_setup` for the disk). In `/etc/scylla/scylla.yaml` on host N:

```yaml
cluster_name: 'bench'
listen_address: 10.0.0.1N
rpc_address: 10.0.0.1N
endpoint_snitch: GossipingPropertyFileSnitch
seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "10.0.0.11"
commitlog_sync: batch     # ACK after the commitlog is synced, like VeltrixDB's
                          # fdatasync-before-ACK (default periodic = up to 10 s
                          # of ACKed writes only in memory). Report which one.
```

Start node 1, wait for `nodetool status` to show it `UN`, then node 2, then
node 3. `compare.sh` creates the keyspace with RF 3 through `cmd/cqlexec`
(`CREATE KEYSPACE ycsb WITH replication = {'class':'NetworkTopologyStrategy',
'replication_factor':3}`, then `ALTER` in case an RF 1 keyspace is left over)
and records `release_version` and the keyspace's replication in
`environment.txt`. Pass `SCYLLA_COMMITLOG=batch|periodic` to `compare.sh` so
the summary states the setting the servers actually run (with `START=0` it
cannot check).

### Aerospike

Start from `aerospike-cluster-N.conf` on host N, replacing the 172.28.0.x
addresses: `access-address 10.0.0.1N` and one `mesh-seed-address-port
10.0.0.1x 3002` line per node. For a performance run replace the `file`
storage with the raw device (`device /dev/nvme0n1`) and drop `filesize`.
`replication-factor 3` is already set. Community Edition is AP; for
`strong-consistency true` (and `commit-to-device`) you need Enterprise.
`flush-max-ms` (default 1 here) bounds how long an ACKed write can stay in
the write buffer — `AEROSPIKE_FLUSH_MAX_MS` / `AEROSPIKE_COMMIT_TO_DEVICE=1`
make `compare.sh` render the conf it uses into `results/<ts>/aerospike-conf/`
and record the values; on your own hosts set them in the conf and pass the
same variables so the summary is right.

```bash
asadm -e info                       # cluster size 3 on every node
asinfo -v 'namespace/test' | tr ';' '\n' | grep -E 'replication-factor|strong-consistency'
```

### Client (4th machine)

```bash
git clone … && cd VeltrixDB/bench/compare        # Go ≥ 1.25
START=0 NODES=3 VELTRIX_MODE=raft \
  VELTRIX_ADDRS=10.0.0.11:9000,10.0.0.12:9000,10.0.0.13:9000 \
  SCYLLA_HOSTS=10.0.0.11,10.0.0.12,10.0.0.13 SCYLLA_COMMITLOG=batch \
  AEROSPIKE_HOSTS=10.0.0.11,10.0.0.12,10.0.0.13 AEROSPIKE_FLUSH_MAX_MS=1 \
  RECORDS=50000000 OPS=50000000 THREADS=256 WORKLOADS="a b c f" \
  DBS=veltrixdb ./compare.sh        # one database at a time on shared hosts
```

Check in `summary.md`: the Configuration block, the `# veltrixdb:` routing
line (redirects ≈ threads, `errors=0`, reads spread over three nodes) and the
replica check (missing=0 on all three).

### Calibration: reproduce a vendor number first

A harness can be slower than the database. Before trusting a same-runner
comparison, check each database with its own tool on these machines against
a number its vendor published for similar hardware:

1. **ScyllaDB — `cassandra-stress`** from the client, e.g.
   `cassandra-stress write n=50000000 cl=QUORUM -schema "replication(strategy=NetworkTopologyStrategy,replication_factor=3)" -mode native cql3 -rate threads=256 -node 10.0.0.11,10.0.0.12,10.0.0.13`,
   then `read` / `mixed`. Compare with ScyllaDB's published result for the
   same instance type (cite the source next to yours).
2. **Aerospike — ACT and `asbench`**: certify the drives with ACT
   (`act_storage`, Aerospike Certification Tool) at the load you plan, then
   `asbench -h 10.0.0.11 -n test -k 50000000 -o B1000 -w RU,50 -z 256`
   (aerospike-tools; the vendor's Java benchmark or YCSB binding are
   alternatives) against the vendor's published figures.
3. **VeltrixDB — `cmd/loadtest`** (`--proto=binary`) against BENCHMARKING.md.

If a database's own tool is far below its published figure, fix the setup
(disk, IRQs, CPU governor, NIC, placement) before comparing. Then run
`compare.sh`; report both numbers — own tool and go-ycsb — for each
database. The gap is the harness cost: go-ycsb's cassandra driver uses an old
gocql without token- or shard-aware routing, its aerospike driver an old
client, so they can trail `cassandra-stress` / `asbench`.

## Making the numbers comparable

Publish a result only with all of these stated next to it:

1. **Same machines, same disks, one database running at a time.** The client
   on a separate machine (`START=0`), in the same zone / placement group for
   clusters.
2. **Durability.** VeltrixDB ACKs after `fdatasync` of WAL and VLog (default).
   ScyllaDB's default `commitlog_sync: periodic` ACKs before the write is on
   disk; compose files here pass `--commitlog-sync batch`
   (`SCYLLA_COMMITLOG`, default `batch`) for an equal comparison — report
   which. Aerospike ACKs from its write buffer (`flush-max-ms`);
   `commit-to-device` is Enterprise.
3. **Warm-up and size.** Load more data than the page cache / database cache
   holds if the point is disk performance; run each workload long enough
   (≥ 5 min) for compaction and GC to show up.
4. **Replication factor**: 1 with `NODES=1`, 3 with `NODES=3`, and the
   consistency levels of the table above.
5. **Versions** of all three (`environment.txt` records image tags for
   `START=1`, ScyllaDB's `release_version`, Aerospike's build when `asinfo`
   is installed, and the VeltrixDB client commit).

## Workload notes

- Updates are sent with all fields (`writeallfields=true`), but go-ycsb's
  VeltrixDB and Aerospike drivers still read the record before writing it
  (two round trips per update); its cassandra driver issues one `UPDATE`.
  Update latencies are therefore not like-for-like with ScyllaDB.
- Workload E uses VeltrixDB's `RANGE` over the ordered key index.
