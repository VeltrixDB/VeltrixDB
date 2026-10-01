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

**Reads on a new leader wait for the no-op too.** Until the no-op is applied, the new leader's state machine may not yet contain entries its predecessor committed and acknowledged. Serving a local GET in that window returned "not found" for an acknowledged write (`TestRaftClusterFailover` failed intermittently this way; nothing was lost). `RaftNode.WaitLeaderApplied` blocks reads on a leader until `lastApplied ≥` its no-op index (≤ `readIndexTimeout`, 2 s); after that it is one atomic load. Followers never wait — their local reads are stale by design; use `--linearizable-reads` for the ReadIndex fence. GET and every MGET path go through the same barrier (`coordinator.readBarrier`). Without it, a unit test missed the acknowledged write in 20 of 20 failovers; with it, 0 of 20.

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
    │ (registered in pendingWrites, enqueued to writeQueue, cap 10000;
    │  never blocks — a full queue returns "write queue full")
    ▼
backgroundReplicationWorker()
    │ batches up to BatchSize (100) entries or FlushIntervalMs (10 ms)
    ▼
replicateBatch(ops)
    ├─ goroutine → replica 1: sendReplicationRPC(ops) → TCP
    ├─ goroutine → replica 2: sendReplicationRPC(ops) → TCP
    └─ … (every replica not in FAILED state)

coordinator, for quorum/strong:
    WaitForReplication(seq, target, 10000 ms)
       polls every 1 ms until 1 + #replicas with LastAckSeqNum ≥ seq ≥ target
       (target = RF/2+1 for quorum, RF for strong)
```

Deletes replicate the same way as tombstone ops. `MultiPut` is replicated entry by entry. Each replica runs a `ReplicationServer` (default port client+1) that receives the batch — a `RELP` magic header plus a gob-encoded `[]*WriteOperation` — and calls `applyFn` for each operation: `StorageEngine.Delete` for a tombstone, otherwise `StorageEngine.Put`. Vector, text and index-definition writes replicate as plain keys on their reserved prefixes; the engine's search hooks update the replica's RAM indexes.

### Version Vectors

`WriteOperation` has a `VersionVector` field (a per-node logical clock map) with these helpers:

```go
// HappenedBefore: vv causally precedes other
func (vv *VersionVector) HappenedBefore(other *VersionVector) bool

// Concurrent: neither causally precedes the other (concurrent writes)
func (vv *VersionVector) Concurrent(other *VersionVector) bool
```

They are not wired in: the server does not set `VersionVector` on its writes, nothing compares them, and no conflict resolution runs. A replica simply applies each received op with `Put`/`Delete`, in arrival order. The `ConflictResolutions` counter (`veltrixdb_replication_conflict_resolutions_total`) is never incremented.

### Anti-Entropy

The `backgroundAntiEntropyWorker` runs every 30 seconds (`AntiEntropyInterval`). For each replica in state `LAG` or `SYNC_PENDING` it re-sends every op in `pendingWrites` whose sequence number is above that replica's `LastAckSeqNum`:

```
Anti-entropy check (every 30 s)
    │
    ▼
For each replica in LAG / SYNC_PENDING:
    find ops where seqNum > replica.LastAckSeqNum
    sendToReplica(pendingOps)
    │
    ▼
Success → state SYNC, LastAckSeqNum advanced
```

`pendingWrites` only holds ops not yet acked by every replica. Anti-entropy is not a full-state sync: it never compares keyspaces, and it does not touch replicas in `FAILED` state.

### Replica States

| State | Meaning |
|-------|---------|
| `SYNC` | Last send to the replica succeeded |
| `SYNC_PENDING` | Defined, but never set by the current code |
| `LAG` | Set only by backpressure: lag above `BackpressureLagBytes` (0 = disabled, the server default). Anti-entropy retries it |
| `FAILED` | Set on any failed send. FAILED replicas are skipped by `replicateBatch` and by anti-entropy, and nothing in `replication/` moves them back to `SYNC` — in practice a replica stays failed until the primary restarts |

### Lag Monitoring

The `backgroundLagMonitor` runs every second and sums each replica's `LagBytes` (un-acked bytes since its last successful send) into `ReplicationMetrics.ReplicaLagBytes`. Exposed via Prometheus as:
- `veltrixdb_replication_lag_bytes`
- `veltrixdb_replication_lag_nanoseconds` — reads `ReplicaLagNs`, which nothing writes, so it is always 0 (per-replica `LagNs` holds the last send's duration and is visible only via `GetReplicaLag`)

Other replication metrics: `veltrixdb_replication_writes_total`, `veltrixdb_replication_failures_total`, `veltrixdb_replication_anti_entropy_runs_total`, `veltrixdb_replication_vector_clock_updates_total`.

### Tombstone Coordination

The `TombstoneCoordinator` (`storage/tombstone_replicated.go`) is meant to keep the GC from reaping tombstones before every replica has acknowledged them:

```
CanReapTombstone(tombstoneTsUs, nowUs, gracePeriodSec) → false
    if the tombstone is younger than the grace period, or
    if any replica's acked watermark < tombstoneTsUs
```

It is not wired in yet: the replication engine never calls `SetReplicaWatermark`, and the defragmenter's `reapExpiredTombstones` uses only `GCGracePeriodSec` (86400 s) and never calls `CanReapTombstone`. A replica that misses a delete for longer than the grace period is not protected.

---

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
