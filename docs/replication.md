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
| `persistentState` + `logStore` | Survives crashes: `CurrentTerm`, `VotedFor` (`raft_meta.dat`), `Log` (append-only `raft_log.dat`) — `consensus/raft_storage.go` |
| `Transport` | gob-over-TCP layer for `RequestVote`, `AppendEntries` and `InstallSnapshot` RPCs: one multiplexed stream per peer (requests in order, replies matched by ID), legacy one-RPC-per-round-trip framing against older servers — `consensus/transport.go` |
| syncer / replicators | `consensus/pipeline.go`: one goroutine per node writes + fsyncs the queued log records; one goroutine per follower (on the leader) keeps a window of AppendEntries in flight |
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
    ├─ under one rn.mu section: append every LogEntry{Term, Index, Command}
    │  to the in-memory log and encode ONLY the new records into the
    │  logStore write queue (queue order = log order = file order)
    ├─ kick the syncer (and, with --raft-pipeline, every peer's replicator)
    │  — nothing waits for disk
    │
    ├──────────────────────────────┬───────────────────────────────────────┐
    ▼                              ▼                                       ▼
syncer (leader's own copy)     replicator, peer 2                      replicator, peer 3
    ├─ one write(2) of the      ├─ 1 (default) or up to PipelineWindow  (same)
    │  queue + one fsync,       │  (8, --raft-pipeline) entry-carrying
    │  outside rn.mu            │  AppendEntries in flight, nextIndex
    ├─ durableIndex = tail      │  advanced optimistically
    │  (leader counts itself    └─ reply: matchIndex = reply.MatchIndex
    │   only up to here)            (durable on the follower)
    └─ default: wake the replicators (paced sending)
    │                              │
    └──────────────┬───────────────┘
                   ▼
maybeAdvanceCommit()      ← commitIndex = highest N of the current term held
    │                        durably by a majority (leader: durableIndex)
    ▼
applier goroutine         ← raftFSM.ApplyBatch / Apply → StorageEngine
    │                        (consecutive PUTs coalesce into one MultiPut)
    ▼
Submit() returns nil      ← client receives OK
```

Concurrent batches share fsyncs on both sides — the syncer writes and syncs whatever is queued when it runs. When the replicators send depends on `--raft-pipeline`:

**`--raft-pipeline=false` (the default): one AppendEntries in flight per follower, paced by the leader's fsync.** Appending does not wake the replicators; the syncer wakes them each time a leader fsync completes, and they send everything appended so far — including entries the leader's *next* fsync is still writing. A successful reply sends again at once only if no leader fsync is pending (otherwise that fsync's completion does it). This is the send pattern of the pre-2026-10 code (fsync, then broadcast the log tail), and it keeps each follower fsync covering a leader batch or more. Env `VELTRIX_RAFT_PIPELINE` sets the flag's default.

**`--raft-pipeline=true`: pipelined, eager sending.** Every append and every reply wakes the replicators, so a batch ships the moment it is appended — the leader's own fsync and the followers' round trip + write + fsync **overlap** — and up to `DefaultPipelineWindow` (8) AppendEntries are outstanding per follower.

**Why it is off by default.** Eager sending splits the stream into many small AppendEntries; every one costs a follower fsync. With one disk per node that is cheap and the overlap wins; with several nodes sharing one disk the extra flushes saturate it. Measured on one Mac (3 local processes, darwin `F_FULLFSYNC` ≈ 3.6 ms on a shared SSD, YCSB load, 20 K records, 16 threads): eager sending ~560–610 inserts/s (≈ 100 log fsyncs/s per node at ~7–8 ms each), paced sending ~1,050–1,100 (leader ≈ 135 fsyncs/s at ~3.8 ms, followers ≈ 78/s), which is the pre-2026-10 figure. Window 1 alone does not recover it (~575/s); the send trigger does — with paced sending even window 8 measured ~1,100. Pipelined mode did better on the read-heavy workload B (~10.5 K vs ~5.6 K ops/s on the same Mac) and in a model with independent per-node fsyncs (load 3.0 K → 5.6 K inserts/s), but it has not been measured on separate hosts. **Enable it** once a multi-host benchmark (bench/compare/README.md, "Raft pipeline") shows it ahead on your hardware — typically one NVMe per node on separate machines. Durability, commit rules and the wire protocol are identical in both modes, so nodes can be switched one at a time (a mixed cluster is fine).

**Pipelining (both modes; window 1 when off).** Each replicator keeps up to its window of entry-carrying AppendEntries outstanding per follower, each at most `maxAppendEntries` (1024) entries / `maxAppendBytes` (4 MiB). The TCP transport delivers one peer's requests in send order over a single stream, and the follower appends them in that order, so a pipeline normally never sees a gap. Replies can return out of order (a heartbeat is answered while an earlier append still waits for its fsync); they are matched to their own request:

- **Success** raises `matchIndex` to the reply's `MatchIndex` (never lowers it) and moves `nextIndex` past it.
- **Rejection or transport error** in the current pipeline epoch resets the pipeline: `epoch++`, `nextIndex` = the follower's conflict hint (or the failed batch's first index), and the replicator *probes* — one request at a time, after every older request has been answered — until a Success. Rejections from an older epoch are ignored.
- **A reply with a higher term** makes the leader step down; replies for an older term, or for a replicator that was replaced, are ignored.
- **Heartbeats** (every 50 ms) bypass the window, so a follower whose fsync is slow still hears from the leader.
- **InstallSnapshot**: a peer whose `nextIndex` is at or below the snapshot gets the snapshot only once its pipeline has drained; afterwards the pipeline restarts (probing) at `lastIncludedIndex + 1`.
- **Membership**: a replicator starts when a peer enters the configuration (append time) and stops when it leaves or the node stops leading.

A transport that does not implement `AsyncTransport` (test mocks) is driven through a per-peer FIFO that calls `SendAppendEntries` one request at a time.

**Key invariant**: `Submit()` blocks on a per-index commit waiter that the applier signals after applying that entry, with a 5-second deadline (`submit timeout: entry N not committed within 5s`); losing leadership fails it with `ErrNotLeader`. The entry is only visible to clients after the state machine has applied it on the leader. The applier wakes on commit notifications and on a 1 ms ticker.

### The Write Path (Follower)

```
HandleAppendEntriesAsync(args, respond)   ← the RPC server calls it in arrival order
    │  under rn.mu:
    ├─ Verify args.Term >= currentTerm (a higher term is persisted first)
    ├─ Stay/become Follower, record LeaderID, resetElectionTimer()
    ├─ Consistency check: args.PrevLogIndex/Term matches local log
    ├─ Append new entries, truncate conflicts (durableIndex is lowered below
    │  a truncation point); enqueue the new records; kick the syncer
    ├─ commitIndex = min(LeaderCommit, index of the last entry of this request)
    │  release rn.mu
    ├─ entries already durable (or a heartbeat, see below) → respond now
    └─ otherwise park the reply; the syncer answers it once durableIndex
       covers the request's last entry
```

A follower **acknowledges an entry only after it is durable**. The syncer writes the queue and fsyncs outside `rn.mu` — heartbeats, RequestVote and further appends are served while it runs, and several AppendEntries that arrive during one fsync share the next one. A parked reply is decided by the syncer (or by any later event that changes the outcome):

- `durableIndex ≥` the request's last index and that entry still has the term it had when accepted → `Success`, `MatchIndex` = that index;
- the term changed since → `Success=false` with the new term (the old leader steps down);
- the entry was truncated or replaced (a snapshot install discarded the log, …) → `Success=false` with a conflict hint — **never** a match for entries that are no longer in the log;
- a failed write/fsync or `Stop` → `Success=false`.

`markDurableLocked(idx, term)` re-checks that the entry at `idx` still has the term it had when the fsync began. By the Log Matching property that is enough: if a newer leader replaced entries while the fsync ran, the term at `idx` differs and the stale report is dropped; the new records wait for their own fsync.

**Heartbeats never wait for an fsync.** A leader of this version sets `AppendEntriesArgs.WantMatch`; the follower answers a heartbeat at once with `MatchIndex = min(PrevLogIndex, durableIndex)` — its log matches the leader's through `PrevLogIndex`, but it only vouches for the durable part. A leader of an older build does not set `WantMatch` and treats `Success` as "durable through `PrevLogIndex + len(Entries)`"; for it the follower answers heartbeats only once that is true (and an older follower, which never sets `HasMatch`, is credited with exactly that), so a mixed-version cluster keeps the rule.

The commit index a follower takes from the leader is capped at the last entry the request verified (Figure 2, step 5). Earlier builds capped it at the follower's whole log, which could commit a stale suffix from an older term that the request had not checked.

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

Raft state lives in the node's Raft `dataDir` — `<--data>/raft` in the server (the `--data` flag, even when `--data-dirs` is set) — in two files (`consensus/raft_storage.go`):

| File | Holds | Written |
|------|-------|---------|
| `raft_log.dat` | 8-byte header (`VXRL`, version 1), then one record per entry: `[len u32][crc32c u32][term u64, index u64, type u8, command]` | appended by the syncer: one `write(2)` of everything queued + one fsync per round (a round covers every group-commit batch or AppendEntries queued before it started) |
| `raft_meta.dat` | `CurrentTerm`, `VotedFor` (CRC-checked) | temp file → fsync → rename → directory fsync, only when term or vote changes |
| `raft_snapshot.gob` | state-machine snapshot + last included index/term + configuration | temp file → fsync → rename → directory fsync, on compaction / InstallSnapshot |

The cost of a commit is the new records plus one fsync, whatever the length of the retained log. Before 2026-10 every flush gob-encoded and fsynced the **whole** retained log into `raft_state.gob` (up to 8,192 entries — about 8 MB per flush with ~1 KB YCSB values), which held a local 3-node raft YCSB load to ~58 inserts/s; `TestRaftPersist_BytesPerFlushIndependentOfLogLength` pins the new behaviour (1,049 bytes per single-entry flush at 100 and at 8,085 entries; the old format wrote 143 KB and 8.4 MB).

What is durable before what:

- **Entries.** Log records are encoded into the write queue under `rn.mu`, so the file order is the log order; the syncer's `write(2)` and fsync run outside the lock (on APFS a `write(2)` blocks while an `F_FULLFSYNC` of the same file runs, so it must not sit under `rn.mu` either). A follower acknowledges entries only once they are durable (see the follower write path); the leader counts itself toward a commit quorum only up to `durableIndex` — the last index its own fsync covered — so entries shipped to followers before the leader's fsync finishes cannot commit on the strength of an unsynced leader copy.
- **Failed write or fsync** is sticky: the store refuses every later write until the node restarts (after a failed fsync the kernel may have dropped the dirty pages). A follower drops its non-durable suffix from memory and answers `Success=false`; a leader keeps its log (its entries may already be on followers, and an index must never be reused for a different entry of the same term) and fails the `Submit` calls it cannot vouch for with `persist log: …` — they may still commit through the followers.
- **Term and vote** are written before the node acts on them: before granting a vote, before soliciting votes, and before answering an RPC that raised its term. If the write fails the vote is not granted and the RPC is not acknowledged.
- **Truncation.** There is no truncate record: replay treats a record whose index is ≤ the last replayed index as "drop everything from this index, then append". A follower that overwrites a conflicting suffix therefore supersedes it with the first new record; a crash before that record is complete leaves the old suffix, i.e. the state before the AppendEntries, which was never acknowledged.
- **Torn tail.** Replay stops at the first record with a short read, an impossible length or a CRC mismatch; the file is truncated to the last good record and fsynced before anything is appended (otherwise later records would sit behind the garbage and be lost on the next restart).
- **Compaction.** After the snapshot file is durable, `raft_log.dat` is rewritten to the retained suffix (temp → fsync → rename → dir fsync). A crash in between leaves the longer file; its snapshot-covered prefix is dropped on load (and the file rewritten).
- On load `CurrentTerm` is raised to the last log term if it is lower (cannot happen with the write order above; it guards a damaged or hand-restored directory).

**Migration from `raft_state.gob`.** A `dataDir` that has the old whole-state `raft_state.gob` and no `raft_meta.dat` is converted on startup: `raft_log.dat` first, `raft_meta.dat` last (the commit point — a crash before it simply re-runs the migration from the old file), then `raft_state.gob` is renamed to `raft_state.gob.migrated`. The log prints `[raft] migrated …`. Nothing else is needed.

**Rollback.** A build from before this change does not read `raft_log.dat` / `raft_meta.dat`. Started on a migrated directory it finds no `raft_state.gob` and comes up at term 0 with an empty log — it can then vote twice in a term and forget entries it acknowledged, so **do not just start the old binary on the directory**. To downgrade a node: stop it, delete its whole `--data` directory (Raft state and storage engine — re-applying the log onto existing storage would re-run non-idempotent ops such as INCR), start the older build with the same `--node-id` / `--peers`, and let it catch up from the leader (InstallSnapshot or log replay). Do it one node at a time with a healthy leader and the other two nodes up. Renaming `raft_state.gob.migrated` back is only safe if the node has accepted no entry and no term change since the upgrade, which you normally cannot know.

Once the retained log reaches `SnapshotThreshold` entries (default 8192) and the state machine implements `SnapshotStateMachine` (the server's `raftFSM` does), the applied prefix is replaced by a snapshot in `<dataDir>/raft_snapshot.gob` (same temp-file + fsync + rename pattern). On restart the snapshot is restored before the log tail is replayed; a follower that needs compacted entries receives one single-shot `InstallSnapshot` RPC. Limits of the server snapshot: it is the whole live keyspace as key/value pairs, capped at 256 MB (a larger one is skipped with `snapshot too large ... skipping compaction`, and the log keeps growing), and TTLs are not preserved — restored keys become immortal.

Raft state is separate from the storage engine's own WAL, which each node replays on startup independently (see [node-lifecycle.md](node-lifecycle.md#crash-recovery-on-the-crashed-node)).

### Raft Election Constants

| Constant | Value | Why |
|----------|-------|-----|
| `electionTimeoutMin` | 400 ms | Heartbeats no longer wait behind a follower fsync; still well above the 50 ms heartbeat |
| `electionTimeoutMax` | 800 ms | Spread reduces collision probability in 3-node clusters |
| `heartbeatInterval` | 50 ms | Must be << `electionTimeoutMin`; leader sends heartbeat every 50 ms (outside the pipeline window) |
| `DefaultPipelineWindow` | 8 | Entry-carrying AppendEntries in flight per follower with `--raft-pipeline` (`Options.PipelineWindow`); 1 without it (the default) |
| `maxAppendEntries` / `maxAppendBytes` | 1024 / 4 MiB | Upper bound of one AppendEntries |
| `rpcCallTimeout` | 5 s | An unanswered request fails, its stream is closed and re-dialled |

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

### Partition migration and the two modes

Partition migration (`TransferAgent.MigrateToNewOwners`, `docs/partitioning.md`) writes and deletes keys on the local engine directly, outside both the Raft log and the replication stream, so it is restricted (invariant 62):

- **`--mode=raft`: never.** Every node applies the whole log and holds every key. `cmd/server` marks the transfer agent raft-managed: `--auto-rebalance` is a no-op (logged at startup), `MigrateToNewOwners` returns `ErrMigrationRaftMode` without touching data, and inbound `/transfer/keys` batches are refused with HTTP 403. Raft membership changes go through `RaftNode.AddServer` / `RemoveServer`; a new member is filled by `AppendEntries` / `InstallSnapshot`, not by migration. Before this rule, killing the leader of a 3-node raft cluster loaded with 20,000 keys left the survivors with 5,435 and 7,875 keys: every node had sent each key to its single ring owner and deleted it locally.
- **`--mode=replicated`: only on membership changes, and RF-aware.** A node keeps every key it is one of the RF replicas of (`GetReplicasForKey`), copies a key to replicas that newly need it, and deletes a key only when it is no longer among the key's replicas and every current replica acknowledged it. A node failure or recovery is a state change, not a membership change, and moves nothing — catching a recovered replica up is the replication engine's job (Replica Recovery above). With 3 nodes and RF=3 nothing ever moves. Note that the server's replication engine sends every write to every peer, so in `cmd/server` every replicated node holds every key regardless of RF; and the server's membership is static, so its auto-rebalancer never actually migrates.
