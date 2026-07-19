# VeltrixDB Architecture

---

## Overview

```
Client (any SDK or nc)
        │  TCP — binary or text protocol
        ▼
┌─────────────────────────────────────────┐
│  TCP Server (cmd/server)                │
│  1 goroutine/conn · pipeline coalescing │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│  Storage Engine (storage/engine.go)     │
│                                         │
│  In-Memory Index   LIRS Cache           │
│  8192 shards       hot values in RAM    │
│                                         │
│  WAL (per disk)    VLog (per disk)      │
│  group-commit      append-only values   │
└──────────────────┬──────────────────────┘
                   │ (Linux production only)
                   ▼
┌─────────────────────────────────────────┐
│  C++ Layer                              │
│  ART index · io_uring · NUMA pinning    │
└─────────────────────────────────────────┘
```

---

## Sharding

All data is split across **8192 shards**. Each shard has its own lock — reads on shard 5 never block reads on shard 42.

```
key → FNV-1a hash → bottom 13 bits (& 0x1FFF) → shard ID (0–8191)
shard ID % numDisks → which disk (WAL + VLog + compaction)
```

With 8 disks, 1024 shards land on each disk. A failure on one disk doesn't affect the other 7. (The Go `numShards` and C++ `kNumShards` constants must stay equal — see invariant 1.)

---

## Key Components

**In-Memory Index** — hash map per shard (Go) or ART tree (C++). Each entry stores disk offset, disk index, value size, flags, and a monotone version counter.

**LIRS Cache** — scan-resistant; small values (≤256 B) get priority 2 vs priority 1, making them harder to evict. Sized with `-cache <MB>`.

**WAL (Write-Ahead Log)** — group-commit: N writes share one `fdatasync`. Default 10 ms window. One WAL per disk.

**VLog (Value Log)** — append-only file per disk. Values are written here; only key metadata lives in the index (WiscKey KV separation). Lock-free concurrent appends via atomic offset reservation.

**Block Packing** — batched writes pack up to 26 records per 4 KB VLog block. For 128 B values: 4096 B → 152 B per record (27× density). Single `Put` stays on the unpacked fast path.

---

## Write Path

```
PUT "user:42" → value
  1. Hash key → shard 307 → disk 2 (307 % 8)
  2. Atomically reserve VLog offset; write value
  3. Write WAL entry (key + vlogOffset)
  4. WAL + VLog fdatasync race concurrently (wait = max of both)
  5. Update shard 307 index entry (DiskOffset, Version++)
  6. Insert into LIRS cache
  7. Return OK
```

If the process crashes after step 2 but before step 3, the value is orphaned but never visible — safe.

---

## Read Path

```
GET "user:42"
  1. Hash key → shard 307
  2. LIRS cache hit → return value (no disk I/O)
  3. Cache miss → lock shard (read), look up index entry
  4. Not in index → NOT_FOUND
  5. In index → read value from VLog at DiskOffset
  6. Insert into cache
  7. Return value
```

---

## Admission Control

Protects reads from being starved by heavy writes:

| Read EWMA | Action |
|-----------|--------|
| < 3 ms | Normal — GC runs at full speed |
| ≥ 3 ms | GC bandwidth capped at 60 MB/s |
| ≥ 4 ms | GC paused + each PUT sleeps 2 ms |
| < 2 ms | Everything resumes |
| No reads for 4 min | EWMA treated as stale — GC resumes |

---

## VLog GC Tiers

When garbage builds up, GC escalates automatically to avoid death-spirals:

| Garbage ratio | GC bandwidth | Admission pause honored? | Defrag interval |
|---------------|-------------|--------------------------|-----------------|
| < 30% | no GC | — | — |
| 30–50% | unlimited / 60 MB/s | yes | 120 s |
| 50–65% (critical) | unlimited / 200 MB/s | yes | 60 s |
| ≥ 65% (emergency) | unlimited | **no** | 30 s |

Emergency mode logs `[gc] disk=N EMERGENCY` and increments `veltrixdb_vlog_gc_emergency_runs_total`.

---

## C++ Layer (Linux Only)

| Component | What it replaces | Win |
|-----------|-----------------|-----|
| ART index | Go hash map | Faster range scans, prefix compression |
| io_uring scheduler | blocking pwrite | 3-tier I/O priority (reads always beat compaction) |
| SQPOLL VLog reader | blocking pread | Zero-syscall NVMe reads |
| NUMA thread pinning | default scheduler | Eliminates cross-socket memory latency |

The Go layer is fully functional without C++. C++ is loaded on Linux for production throughput.

---

## Cluster

```
cluster/   partition_map.go    consistent-hash ring (FNV-1a, 64 vnodes/node), epoch fencing
           failure_detection.go heartbeat SUSPECT → FAILED state machine
           gossip.go            TCP gossip listener + digest exchange

replication/ async / quorum / strong replication modes
             vector clocks, anti-entropy, tombstone watermarks

consensus/  Raft — leader election + log replication + snapshots
```

### Deployment modes (`--mode`) — what is actually wired into the serving path

As of the B2 integration, the distributed layer is wired into `cmd/server`
through a **write coordinator** (`cmd/server/coordinator.go`) that fronts the
storage engine.  Every mutating client op (PUT / DELETE / MultiPut / CAS /
INCR / DECR / SETNX / TXN, text and binary protocols) goes through it.

| `--mode` | How writes are handled | Consistency guarantee |
|----------|------------------------|-----------------------|
| `standalone` (default) | Straight to the local engine — byte-for-byte the pre-existing single-node path. No Raft, no replication, no redirects. | Single-node linearizable (one writer, one copy). |
| `raft` | Ops are gob-encoded and submitted to a Raft log (`consensus`), committed by quorum, and applied on every node via a storage-backed FSM (`cmd/server/raft_fsm.go`). Non-leaders reject writes with a `MOVED <leader-addr>` redirect. | **Linearizable writes** (single Raft group, quorum commit). Reads default to local applied state (fast, possibly stale); with `--linearizable-reads`, GET runs the **ReadIndex** fence (`consensus/read_index.go`) — quorum-confirmed, never stale, one heartbeat round-trip per read. |
| `replicated` | The write is applied to the local engine, then handed to the replication engine. The `--consistency` flag decides when the client is ACKed. | Primary-copy durability across N copies; **NOT** linearizable under concurrent writers (no single-writer ordering). Reads are local. |

**`--consistency` (replicated mode):**

- `eventual` (async) — ACK immediately after the local write; replicas catch up in the background.
- `quorum` — ACK only after a majority of replicas (counting the local copy) have applied the write.
- `strong` — ACK only after **all** replicas have applied the write.

`quorum`/`strong` surface `ErrReplicationTimeout` / `ErrQuorumNotReached` to the
client when the required copies cannot be reached (e.g. a strong write with a
majority of replicas down errors instead of silently ACKing).

### Raft FSM & snapshots

`raftFSM` implements `consensus.SnapshotStateMachine`.  `Apply` decodes a write
command and runs the engine's own Put/Delete/atomic/txn method — deterministic
because Raft delivers the identical log order to every node.  Results for ops
that return a value/status (CAS/INCR/SETNX/TXN) are returned to the submitting
client via a per-request result side-channel keyed by a node-unique request id.
`Snapshot`/`Restore` dump and reload the keyspace via the engine's paginated
`ScanCursor`.

### Cluster-aware client & topology

`client.Client` (`client/client.go`) is a real cluster client: it fetches
topology over the storage port (`TOPOLOGY` command, mirrored at
`/admin/cluster`), builds a consistent-hash ring **identical** to the server's
(`cluster.HashKey` + 64 vnodes/node), routes each key to its owner, and follows
`MOVED` redirects to the Raft leader.  `/admin/cluster` reports role, Raft
term/leader, peers, partition epoch, and per-replica replication lag.

### Distributed coverage (B3 phase)

- Namespace (NSPUT/NSDEL/NSDROP), hash-field (HSET/HDEL/HEXPIRE), vector
  (VSET), secondary-index (IDXCREATE/IDXDROP), and list/set
  (LPUSH/RPUSH/LPOP/RPOP/SADD/SREM) writes now route through the coordinator
  in ALL modes: raft replays them deterministically via dedicated FSM ops;
  replicated mode ships their composite-key KV effects as ordinary
  replication traffic (the replica's apply hook also refreshes its in-RAM
  vector index for `@vec/` keys).
- Raft reads: local by default; linearizable via `--linearizable-reads`
  (ReadIndex fence).
- **Auto-rebalance is wired** (`cmd/server/rebalancer.go`): membership
  events (join/leave/fail) trigger ring rebalance + physical key migration
  through the TransferAgent (transfer HTTP listener on clientPort+5;
  disable with `--auto-rebalance=false`).
- Cross-region replication: `cmd/repl-ship` tails `/admin/cdc` live and,
  with `--checkpoint`, replays missed writes (including deletes) through the
  durable `/admin/changes` catch-up feed on restart — zero-loss at
  last-write-wins semantics.

### Remaining gaps

- Replicated-mode secondary-index METADATA (IDXCREATE/IDXDROP) is node-local;
  raft mode replicates it.
- QUERY / RANGE / SCANCUR reads are always local in cluster modes.
- repl-ship has no back-pressure to the source and only LWW conflict
  resolution.
