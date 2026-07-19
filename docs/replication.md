# Replication in VeltrixDB

VeltrixDB uses two replication mechanisms that work at different layers:

1. **Raft Consensus** (`consensus/raft.go`) — synchronous, strongly consistent replication of the write-ahead log across a Raft group. This is the primary durability guarantee.
2. **Replication Engine** (`replication/engine.go`) — asynchronous (or quorum/strong) replication of writes to replica nodes, used for read scaling, geographic distribution, and cross-cluster DR.

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
| `Transport` | TCP layer for `RequestVote` and `AppendEntries` RPCs |
| `StateMachine` | Interface applied once a log entry commits (`StorageEngine`) |

### The Write Path (Leader)

```
Client PUT
    │
    ▼
Submit(command)           ← only succeeds on the leader
    │
    ├─ Append LogEntry{Term, Index, Command} to local log
    ├─ Persist log to raft_state.gob (writeStateFile, async)
    │
    ▼
broadcastAppendEntries()  ← one goroutine per peer, parallel
    │
    ├─ peer 2: AppendEntries RPC → ACK
    ├─ peer 3: AppendEntries RPC → ACK  ← quorum (2 of 3)
    │
    ▼
maybeAdvanceCommit()      ← advance commitIndex when quorum ACKs
    │
    ▼
applier goroutine         ← sm.Apply(command) → StorageEngine.Put/Delete
    │
    ▼
Submit() returns nil      ← client receives OK
```

**Key invariant**: `Submit()` polls `lastApplied >= targetIndex` with a 5-second deadline. The entry is only visible to clients after `sm.Apply()` runs on the leader.

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
          1. args.Term >= peer.currentTerm
          2. peer hasn't voted in this term (or already voted for this candidate)
          3. Candidate log is at least as up-to-date (§5.4.1: compare last term, then length)
            │
            ▼
        Collect votes → become leader when votes > N/2
```

**Randomized timeouts** (400–800 ms) prevent multiple nodes from starting elections simultaneously. Even with 3 nodes starting at the same instant, the different random delays mean at most one reaches candidate before the others.

### Raft §5.4.2 — The No-Op Entry

When a new leader is elected, it cannot directly commit log entries from previous terms — doing so can violate safety (entries can be overwritten by a future leader that hasn't seen them). Instead, the new leader immediately appends a **no-op entry** in its own term:

```go
// becomeLeader() in raft.go
noop := LogEntry{Term: rn.ps.CurrentTerm, Index: lastIdx+1, Command: nil}
rn.ps.Log = append(rn.ps.Log, noop)
```

Once the no-op is committed (quorum ACK), all prior-term entries before it are also implicitly committed. The applier goroutine skips nil-command entries:

```go
if len(e.Command) > 0 {
    rn.sm.Apply(e.Command)
}
```

This is why the first observable effect after a leader election may be a short (≤ 50 ms) delay before writes are accepted — the no-op must commit first.

### Persistence

Raft state (`CurrentTerm`, `VotedFor`, log entries) is persisted to `<dataDir>/raft_state.gob` using a **temp-file rename** pattern for atomicity:

```
1. Write to raft_state_<random>.tmp
2. f.Sync()         ← fdatasync
3. os.Rename(tmp, raft_state.gob)  ← atomic on POSIX
```

If the process crashes mid-write, the old `raft_state.gob` is untouched. The new file is only visible after the rename succeeds.

### Raft Election Constants

| Constant | Value | Why |
|----------|-------|-----|
| `electionTimeoutMin` | 400 ms | Must exceed `saveState()` worst-case duration (~200 ms on CI) plus margin |
| `electionTimeoutMax` | 800 ms | Spread reduces collision probability in 3-node clusters |
| `heartbeatInterval` | 50 ms | Must be << `electionTimeoutMin`; leader sends heartbeat every 50 ms |

---

## Layer 2: Replication Engine (Async/Quorum/Strong)

The `ReplicationEngine` (`replication/engine.go`) handles cross-node replication independently of Raft. It is used when you need read replicas, geographic distribution, or configurable consistency levels beyond Raft's quorum.

### Consistency Levels

| Level | Behavior | Use Case |
|-------|----------|----------|
| `EventualConsistency` | Fire-and-forget — write returns immediately, replication is async | Maximum write throughput |
| `QuorumConsistency` | Wait for RF/2 + 1 replica ACKs | Balanced consistency + availability |
| `StrongConsistency` | Wait for ALL replica ACKs | No data loss even on primary failure |

Default: `QuorumConsistency` with `AsyncReplication` mode.

### Write Replication Flow

```
StorageEngine.Put(key, value)
    │
    ▼
ReplicationEngine.OnLocalWrite(op)
    │ (enqueued to writeQueue channel, cap 10000)
    ▼
backgroundReplicationWorker()
    │ batches up to BatchSize (100) entries or FlushIntervalMs (10 ms)
    ▼
replicateBatch(ops)
    ├─ goroutine → replica 1: sendReplicationRPC(ops) → TCP
    ├─ goroutine → replica 2: sendReplicationRPC(ops) → TCP
    └─ goroutine → replica 3: sendReplicationRPC(ops) → TCP
    │
    └─ Wait based on ConsistencyLevel:
       StrongConsistency  → wg.Wait() (all replicas)
       QuorumConsistency  → wait for RF/2+1 acks
       EventualConsistency → fire-and-forget
```

Each replica runs a `ReplicationServer` that receives the batch over TCP and calls `applyFn` (typically `StorageEngine.Put`) for each operation.

### Version Vectors

Every `WriteOperation` carries a `VersionVector` — a per-node logical clock map. Version vectors enable causal ordering detection:

```go
// HappenedBefore: vv causally precedes other
func (vv *VersionVector) HappenedBefore(other *VersionVector) bool

// Concurrent: neither causally precedes the other (concurrent writes)
func (vv *VersionVector) Concurrent(other *VersionVector) bool
```

When two writes to the same key are **concurrent** (neither causally precedes the other), the `ConflictResolutions` metric increments and last-write-wins semantics apply (highest `WriteTimestampUs`).

### Anti-Entropy

The `backgroundAntiEntropyWorker` runs every 30 seconds. It compares each replica's `LastAckSeqNum` against `pendingWrites`. Any operations the replica hasn't acknowledged are re-sent:

```
Anti-entropy check (every 30 s)
    │
    ▼
For each lagging replica:
    find ops where seqNum > replica.LastAckSeqNum
    sendReplicationRPC(pendingOps)
    │
    ▼
Replica catches up (state → ReplicaStateSync)
```

Anti-entropy ensures replicas that were temporarily unavailable (network partition, restart) eventually converge.

### Replica States

| State | Meaning |
|-------|---------|
| `SYNC` | Replica is up-to-date |
| `SYNC_PENDING` | Replica acknowledged partial batch |
| `LAG` | Replica is behind; anti-entropy will catch it up |
| `FAILED` | Replica unreachable; replication skipped until recovery |

### Lag Monitoring

The `backgroundLagMonitor` runs every second, aggregating `LagBytes` and `LagNs` across all replicas into `ReplicationMetrics`. Exposed via Prometheus as:
- `veltrixdb_replica_lag_bytes`
- `veltrixdb_replica_lag_ns`

### Tombstone Coordination

The `TombstoneCoordinator` (`storage/tombstone_replicated.go`) prevents the GC from reaping tombstone entries before all replicas have acknowledged them:

```
CanReapTombstone(key, writeTimestamp) → false
    if any replica's acked timestamp < writeTimestamp
```

Without this, a GC pass could delete a tombstone before a slow replica sees it — causing the replica to resurrect a logically-deleted key on next anti-entropy sync.

---

## Putting It Together: Raft + Replication Engine

In a full VeltrixDB cluster:

```
                ┌─────────────────────────────────────┐
                │          Raft Group (3 nodes)        │
                │                                      │
Client  ──────► │  Leader ──AppendEntries──► Follower  │
                │          ──AppendEntries──► Follower  │
                │                                      │
                │  (Raft commit = quorum WAL durable)  │
                └──────────────┬──────────────────────┘
                               │ after Raft commit
                               ▼
                    ReplicationEngine.OnLocalWrite()
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
               Async replica         Async replica
               (read scaling)        (DR / geo)
```

Raft provides **linearizability** within the cluster — no committed write is lost. The Replication Engine provides **eventual consistency** (or stronger) to read replicas and DR copies without adding to the write critical path.
