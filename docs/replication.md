# Replication in VeltrixDB

VeltrixDB has two replication mechanisms. The server uses exactly one of them, chosen by `--mode` (`cmd/server/coordinator.go`):

1. **Raft Consensus** (`consensus/raft.go`, `--mode=raft`) — every mutating command goes through a replicated Raft log and is applied on every node in log order. Linearizable writes; non-leaders answer writes with a `MOVED <addr> <id>` redirect.
2. **Replication Engine** (`replication/engine.go`, `--mode=replicated`) — primary-copy replication: a node applies the write locally, then ships it to its peers; `--consistency=eventual|quorum|strong` decides when the client is acknowledged.

`--mode=standalone` (the default) uses neither. Both distributed modes need the Go network front-end (`--net=go`, the default). `cmd/server` refuses `--net=cpp|uring|poll` outside standalone (`--net=<x> serves --mode=standalone only (writes bypass Raft/replication routing)`): the C++ front-end writes straight to the local engine.

Listener ports are derived from a node's client `--addr` port P: replication P+1 (`--repl-addr`), Raft RPC P+2 (`--raft-addr`), gossip P+3 (`--gossip-addr`), partition transfer P+5 (`--transfer-addr`). Peers always use the derived offsets. `--cluster-tls-cert/-key/-ca` enable TLS on the Raft, replication and transfer links; `--cluster-mtls` additionally requires client certificates on the replication and transfer listeners.

---

## Layer 1: Raft Consensus Replication

### What Raft Does

Raft replicates every write as a log entry across all nodes in the Raft group before acknowledging it to the client. A write is **committed** once a quorum (majority) of nodes has durably persisted the entry. This guarantees:
- No committed write is ever lost, even if a minority of nodes crash.
- All nodes apply writes in the same order.
- There is always exactly one leader at any given Raft term.

### Components

| Component | Role |
|-----------|------|
| `RaftNode` | Core Raft state machine per node |
| `persistentState` | Survives crashes: `CurrentTerm`, `VotedFor`, `Log` |
| `Transport` | gob-over-TCP layer for `RequestVote`, `AppendEntries` and `InstallSnapshot` RPCs |
| `StateMachine` | Interface applied once a log entry commits — in the server, `raftFSM` (`cmd/server/raft_fsm.go`), a `SnapshotStateMachine` and `BatchStateMachine` over the `StorageEngine` |

Each log entry's command is a gob-encoded `fsmCmd`. The op codes (`fsmOp`, persisted in the log, so new ones are only ever appended): `opPut`, `opDelete`, `opMultiPut`, `opCAS`, `opIncr`, `opDecr`, `opSetNX`, `opTxn`, `opNSPut`, `opNSDelete`, `opNSDrop`, `opHSet`, `opHDel`, `opHExpire`, `opVSet`, `opIdxCreate`, `opIdxDrop`, `opLPush`, `opRPush`, `opLPop`, `opRPop`, `opSAdd`, `opSRem`, `opVDel`. Ops that return a value (CAS, INCR, TXN, pops, …) get it back through a per-request result channel keyed by a node-salted request ID. Cluster membership changes are separate `EntryConfig` entries (`AddServer` / `RemoveServer`, `consensus/membership.go`).

### The Write Path (Leader)

```
Client PUT
    │
    ▼
Submit(command)           ← only succeeds on the leader; enqueues to submitCh
    │
    ▼
flusher goroutine         ← group commit: drains up to 4096 pending Submits
    ├─ Append every LogEntry{Term, Index, Command} under one rn.mu section
    ├─ Persist log to raft_state.gob (writeStateFile: one fsync for the batch,
    │                                 outside rn.mu, before replication)
    │
    ▼
broadcastAppendEntries()  ← once per batch; one goroutine per peer, parallel
    │
    ├─ peer 2: AppendEntries RPC → ACK
    ├─ peer 3: AppendEntries RPC → ACK  ← quorum (2 of 3)
    │
    ▼
maybeAdvanceCommit()      ← advance commitIndex when quorum ACKs
    │
    ▼
applier goroutine         ← raftFSM.ApplyBatch / Apply → StorageEngine
    │                        (consecutive PUTs coalesce into one MultiPut)
    ▼
Submit() returns nil      ← client receives OK
```

**Key invariant**: `Submit()` blocks on a per-index commit waiter that the applier signals after applying that entry, with a 5-second deadline (`submit timeout: entry N not committed within 5s`); losing leadership fails it with `ErrNotLeader`. The entry is only visible to clients after the state machine has applied it on the leader. The applier wakes on commit notifications and on a 1 ms ticker.

### The Write Path (Follower)

```
HandleAppendEntries(args)
    │
    ├─ Verify args.Term >= currentTerm
    ├─ Stay/become Follower, record LeaderID
    ├─ resetElectionTimer()                ← first reset: prevent spurious election
    ├─ Consistency check: args.PrevLogIndex/Term matches local log
    ├─ Append new entries, truncate conflicts
    ├─ saveState() → raft_state.gob        ← durable persist
    ├─ Advance commitIndex if LeaderCommit > commitIndex
    ├─ notifyApplier()                     ← wake applier goroutine
    ├─ resetElectionTimer()                ← second reset: invalidates stale timer fire
    └─ reply Success=true
```

The **two-phase timer reset** around `saveState()` is critical. On slow storage (CI runners, HDDs), `saveState()` can take 200+ ms. During that time the first timer may fire and the ticker goroutine will wait on `rn.mu`. The second `resetElectionTimer()` after `saveState()` increments the **generation counter**, causing the stale timer fire to be silently discarded rather than starting a spurious election.

### Leader Election

```
Election timeout fires (400–800 ms, randomized)
    │
    ▼
startElection()
    ├─ Increment CurrentTerm
    ├─ Vote for self
    ├─ Become Candidate
    ├─ Reset election timer (new random timeout for re-election if split vote)
    └─ Send RequestVote RPCs to all peers (parallel goroutines)
            │
            ▼
        Peer grants vote if:
          0. the candidate is in the peer's current configuration
          1. args.Term >= peer.currentTerm
          2. peer hasn't voted in this term (or already voted for this candidate)
          3. Candidate log is at least as up-to-date (§5.4.1: compare last term, then length)
            │
            ▼
        Collect votes → become leader when votes ≥ len(config)/2 + 1
        (a single-node configuration wins immediately)
```

**Randomized timeouts** (400–800 ms) prevent multiple nodes from starting elections simultaneously. Even with 3 nodes starting at the same instant, the different random delays mean at most one reaches candidate before the others.

### Raft §5.4.2 — The No-Op Entry

When a new leader is elected, it cannot directly commit log entries from previous terms — doing so can violate safety (entries can be overwritten by a future leader that hasn't seen them). Instead, the new leader immediately appends a **no-op entry** in its own term:

```go
// becomeLeader() in raft.go
noop := LogEntry{Term: rn.ps.CurrentTerm, Index: rn.lastLogIndex() + 1, Command: nil}
rn.ps.Log = append(rn.ps.Log, noop)
rn.termStartIndex = noop.Index
```

Once the no-op is committed (quorum ACK), all prior-term entries before it are also implicitly committed. The applier skips nil-command entries (and config entries):

```go
if e.Type == EntryNormal && len(e.Command) > 0 {
    rn.sm.Apply(e.Command)
}
```

The new leader persists the no-op and broadcasts it immediately, so it normally commits within one round trip. Writes submitted meanwhile are accepted; they commit after the no-op.

**Reads on a new leader wait for the no-op too.** Until the no-op is applied, the new leader's state machine may not yet contain entries its predecessor committed and acknowledged. Serving a local GET in that window returned "not found" for an acknowledged write (`TestRaftClusterFailover` failed intermittently this way; nothing was lost). `RaftNode.WaitLeaderApplied` blocks reads on a leader until `lastApplied ≥` its no-op index (≤ `readIndexTimeout`, 2 s); after that it is one atomic load. Followers never wait — their local reads are stale by design; use `--linearizable-reads` for the ReadIndex fence. Every read path — GET, MGET, namespace, hash, list / set, `RANGE` / `SCANCUR`, `VER` / `GETVER`, `IDXQUERY`, `QUERY` and search, text and binary — goes through the same barrier (`coordinator.readBarrier`, invariant 58); with `--linearizable-reads` that barrier is the ReadIndex fence, so all of them redirect on followers (`cmd/server/read_barrier_test.go`). Without it, a unit test missed the acknowledged write in 20 of 20 failovers; with it, 0 of 20.

### Persistence

Raft state (`CurrentTerm`, `VotedFor`, log entries) is persisted (gob) to `<dataDir>/raft_state.gob` using a **temp-file rename** pattern for atomicity. The server's Raft `dataDir` is `<--data>/raft` (the `--data` flag, even when `--data-dirs` is set).

```
1. Write to raft_state_<random>.tmp
2. f.Sync()         ← fsync
3. os.Rename(tmp, raft_state.gob)  ← atomic on POSIX
```

If the process crashes mid-write, the old `raft_state.gob` is untouched. The new file is only visible after the rename succeeds. The whole log is rewritten on each persist, which is why snapshots matter.

Once the retained log reaches `SnapshotThreshold` entries (default 8192) and the state machine implements `SnapshotStateMachine` (the server's `raftFSM` does), the applied prefix is replaced by a snapshot in `<dataDir>/raft_snapshot.gob` (same temp-file + fsync + rename pattern). On restart the snapshot is restored before the log tail is replayed; a follower that needs compacted entries receives one single-shot `InstallSnapshot` RPC. Limits of the server snapshot: it is the whole live keyspace as key/value pairs, capped at 256 MB (a larger one is skipped with `snapshot too large ... skipping compaction`, and the log keeps growing), and TTLs are not preserved — restored keys become immortal.

Raft state is separate from the storage engine's own WAL, which each node replays on startup independently (see [node-lifecycle.md](node-lifecycle.md#crash-recovery-on-the-crashed-node)).

### Raft Election Constants

| Constant | Value | Why |
|----------|-------|-----|
| `electionTimeoutMin` | 400 ms | Must exceed `saveState()` worst-case duration (~200 ms on CI) plus margin |
| `electionTimeoutMax` | 800 ms | Spread reduces collision probability in 3-node clusters |
| `heartbeatInterval` | 50 ms | Must be << `electionTimeoutMin`; leader sends heartbeat every 50 ms |

---

## Layer 2: Replication Engine (Async/Quorum/Strong)

The `ReplicationEngine` (`replication/engine.go`) backs `--mode=replicated`. It does not use Raft: there is no leader election or single-writer ordering, and every node accepts writes. Quorum/Strong give durability across N copies before the ACK, but not linearizability under concurrent writers. Reads are always local.

### Consistency Levels

Set per server with `--consistency` (default `eventual`; the library's `DefaultReplicationConfig` defaults to `QuorumConsistency` with `AsyncReplication`, but the server overrides the level). `replFactor` is `ReplicationFactor` (3); the local write counts as one copy.

| Level | Client is ACKed after | Use Case |
|-------|----------|----------|
| `EventualConsistency` (`eventual`) | the local write — replication is async | Maximum write throughput |
| `QuorumConsistency` (`quorum`) | RF/2 + 1 copies (local + RF/2 replicas) have the write | Balanced consistency + availability |
| `StrongConsistency` (`strong`) | RF copies have the write | Survives primary failure |

Quorum/Strong return `ErrReplicationTimeout` when the copies are not reached within `ReplicationTimeout` (10 s).

### Write Replication Flow

```
coordinator.Put(key, value)            (cmd/server/coordinator.go)
    │
    ├─ StorageEngine.Put(key, value)    ← local write first
    ▼
ReplicationEngine.OnLocalWrite(op)
    │ (registered in the retention buffer, enqueued to writeQueue, cap 10000;
    │  never blocks — on a full queue the op stays retained, healthy replicas
    │  go LAG and anti-entropy is kicked at once to deliver it;
    │  counted in WriteQueueOverflows)
    ▼
backgroundReplicationWorker()
    │ batches up to BatchSize (100) entries or FlushIntervalMs (10 ms)
    ▼
replicateBatch(ops)
    ├─ goroutine → replica 1: sendReplicationRPC(ops) → TCP
    ├─ goroutine → replica 2: sendReplicationRPC(ops) → TCP
    └─ … (every replica in SYNC or LAG; FAILED / SYNC_PENDING replicas
          are caught up by the recovery worker instead)

coordinator, for quorum/strong:
    WaitForReplication(seq, target, 10000 ms)
       polls every 1 ms until 1 + #replicas that acked this exact op ≥ target
       (target = RF/2+1 for quorum, RF for strong)
```

The **retention buffer** (`pendingWrites`) holds every op until every registered replica, live or not, has acked it; acks are tracked per op and per replica. It is bounded by `MaxRetainedOps` (100 000): on overflow the oldest 10 % are dropped and every replica that had not acked them is flagged for a change-feed catch-up (see Replica Recovery). `WaitForReplication` counts the replicas that acked that op, so an ack of a later sequence number never satisfies a wait for an earlier one (concurrent writers can enqueue seq N+1 before N); once the op has left the buffer it falls back to `LastAckSeqNum`. The buffer is in memory: a primary restart forgets what its replicas had not acked.

Deletes replicate the same way as tombstone ops. `MultiPut` is replicated entry by entry. Each replica runs a `ReplicationServer` (default port client+1) that receives the batch — a `RELP` magic header plus a gob-encoded `[]*WriteOperation` — and calls `applyFn` for each operation: `StorageEngine.Delete` for a tombstone, otherwise `StorageEngine.Put`. Vector, text and index-definition writes replicate as plain keys on their reserved prefixes; the engine's search hooks update the replica's RAM indexes.

### Version Vectors

`WriteOperation` has a `VersionVector` field (a per-node logical clock map) with these helpers:

```go
// HappenedBefore: vv causally precedes other
func (vv *VersionVector) HappenedBefore(other *VersionVector) bool

// Concurrent: neither causally precedes the other (concurrent writes)
func (vv *VersionVector) Concurrent(other *VersionVector) bool
```

They are reserved, not wired in: the server does not set `VersionVector` on its writes, nothing compares them, and no conflict resolution runs. A replica simply applies each received op with `Put`/`Delete`, in arrival order, so concurrent writes to one key on different nodes can leave replicas with different values. The type and the `WriteOperation` field stay for wire-format and API stability. The metrics that implied otherwise — `veltrixdb_replication_conflict_resolutions_total` and `veltrixdb_replication_vector_clock_updates_total`, both always 0 — were removed, along with the engine's unused per-replica version-vector map.

### Anti-Entropy

The `backgroundAntiEntropyWorker` runs every 30 seconds (`AntiEntropyInterval`). For each replica in state `LAG` it re-sends every retained op that replica has not acked; success returns it to `SYNC`. Anti-entropy is not a full-state sync: it never compares keyspaces. `FAILED` and `SYNC_PENDING` replicas are handled by the recovery worker.

### Replica States

| State | Meaning |
|-------|---------|
| `SYNC` | Healthy: receives live batches |
| `LAG` | Set only by backpressure: lag above `BackpressureLagBytes` (0 = disabled, the server default). Still receives live batches; anti-entropy retries it; the next successful send returns it to `SYNC` |
| `FAILED` | Set on any failed send. Receives no live batches (the ops it misses stay in the retention buffer); re-probed by the recovery worker with backoff |
| `SYNC_PENDING` | The replica answered a probe and is being caught up; still no live batches. Ends in `SYNC` (caught up) or back in `FAILED` (a catch-up send failed) |

### Replica Recovery

`backgroundRecoveryWorker` (`replication/engine.go`, `recoverFailedReplicas` / `recoverReplica`) re-probes each `FAILED` replica once its backoff has elapsed. The first probe is `RecoveryProbeInterval` (1 s) after the failure; each failed attempt doubles the delay, up to `RecoveryMaxBackoff` (30 s); recovery resets it.

```
FAILED ──(backoff elapsed)──► Ping (empty batch, acked without applying; ReplicationClient.Ping)
   ▲                             │ ok
   │                             ▼
   │                        SYNC_PENDING
   │                             │ 1. if retained ops it never acked were dropped (buffer overflow):
   │                             │    stream the primary's change feed (StorageEngine.ChangesSince,
   │                             │    current value or tombstone of every key written since the
   │                             │    oldest dropped op − 10 s, pages of 1000, TTLs preserved)
   │                             │ 2. replay every retained op it has not acked, in SeqNum order,
   │                             │    in BatchSize chunks; repeat until none is left
   └──── any send fails ─────────┤
                                 ▼ (checked under the engine lock — later writes reach it live)
                               SYNC
```

`cmd/server` installs the change-feed source (`engineCatchUp`, `cmd/server/cluster_setup.go`). A library user without `SetCatchUpSource` gets the replay of retained ops only; if ops were dropped the replica is returned to `SYNC` anyway, a warning is logged and `CatchUpGaps` is incremented. The change-feed catch-up ships the primary's current state, not every intermediate version, and like all replication it is applied in arrival order (no conflict resolution).

Limits: retention and replica state are in memory, so a replica that was down when the primary restarted is not caught up for writes made before that restart; a replica only learns writes the primary originated (each node replicates its own writes).

Replica state is visible in `GET /admin/cluster` (`replication[].state`, `lag_ns`, `last_ack_unix_nano`, `next_probe_unix_nano` while FAILED) and in Prometheus (below).

### Lag Monitoring

The `backgroundLagMonitor` runs every second. For each replica it computes the age of the oldest write that replica has not acked (0 when caught up) — `LagNs`, shown per replica as `lag_ns` in `/admin/cluster` — and publishes:
- `veltrixdb_replication_lag_nanoseconds` — the maximum `LagNs` across replicas
- `veltrixdb_replication_lag_bytes` — sum of each replica's `LagBytes` (bytes of failed sends since its last successful one)
- `veltrixdb_replication_replicas_failed`, `veltrixdb_replication_replicas_sync_pending` — gauges of replicas in each state
- `veltrixdb_replication_recovery_probes_total`, `veltrixdb_replication_replica_recoveries_total` — reconnect probes sent, and recoveries completed (FAILED → SYNC_PENDING → SYNC)

Other replication metrics: `veltrixdb_replication_writes_total`, `veltrixdb_replication_failures_total`, `veltrixdb_replication_anti_entropy_runs_total`.

### Tombstone Coordination

The `TombstoneCoordinator` (`storage/tombstone_replicated.go`) keeps the GC from reaping a tombstone before every replica has acknowledged it. In `--mode=replicated` the lag monitor reports each replica's watermark every second (`SetWatermarkObserver` → `StorageEngine.SetReplicaWatermark`): the timestamp of the oldest write the replica has not acked, or now when it is caught up, minus a 10 s margin. The defragmenter's `reapExpiredTombstones` then applies:

```
canReapTombstone(ts, now, grace):
    age < grace                     → keep   (unchanged floor; also protects repl-ship's catch-up feed)
    age ≥ 2 × grace                 → reap   (upper bound: a replica that never catches up
                                              delays reaping by at most one more grace period)
    min replica watermark < ts      → keep   (a replica has not acknowledged the delete)
    otherwise                       → reap
```

With no replica watermark (single node, `--mode=raft`) the grace period (`GCGracePeriodSec`, 86400 s) alone applies, as before. Held tombstones are logged per pass (`[gc] tombstones reaped=… held_for_replicas=…`). Watermarks are in memory and are re-reported within a second of a restart. A replica that stays down longer than 2 × the grace period can still miss a delete whose tombstone was reaped: its change-feed catch-up no longer contains it.

---

## CDC and `repl-ship` (cross-process)

The engine's in-process CDC broker (`storage/cdc.go`) is the feed `cmd/repl-ship` ships from: it long-polls `/admin/cdc` and forwards each event over the binary protocol. Every engine write path emits one event per key, `PUT` (with the value) or `DEL`, after that key's index entry is installed: `Put`, `Delete`, `MultiPut` — which carries MPUT, server-coalesced pipelined PUTs, TXN commit, Raft `ApplyBatch` and the WriteBatcher — and the atomic ops (CAS / INCR / DECR / SETNX, emitted as `PUT` while the shard lock is held). A CAS mismatch or a SETNX on an existing key writes nothing and emits nothing. TTL expiry emits nothing.

Delivery never blocks a writer: a subscriber whose buffer is full loses that event (`cdc_dropped_total` in `/admin/stats`) and is disconnected after 3 consecutive drops; a successful send resets the count. While repl-ship is down it catches up from the durable `/admin/changes` feed on restart (last-write-wins).

## Raft vs. the Replication Engine

The two layers are alternatives, not a stack: `buildCoordinator` builds either a Raft coordinator or a replicated one, never both, and in raft mode no `ReplicationEngine` exists.

```
--mode=raft                                --mode=replicated
┌──────────────────────────────┐           ┌──────────────────────────────┐
│ Client ──► Leader            │           │ Client ──► any node          │
│   (follower: MOVED <addr>)   │           │   local Put, then            │
│ Leader ──AppendEntries──► F  │           │   OnLocalWrite ──► replicas  │
│        ──AppendEntries──► F  │           │   ACK per --consistency      │
│ commit = quorum persisted    │           │                              │
│ apply on every node (FSM)    │           │                              │
└──────────────────────────────┘           └──────────────────────────────┘
```

Raft mode gives **linearizable writes**; reads are local (possibly stale on followers) unless `--linearizable-reads`. Replicated mode gives **eventual consistency** by default, or N-copy durability before the ACK with `quorum`/`strong`.
