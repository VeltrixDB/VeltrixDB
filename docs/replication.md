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

Raft state lives in the node's Raft `dataDir` — `<--data>/raft` in the server (the `--data` flag, even when `--data-dirs` is set) — in two files (`consensus/raft_storage.go`), plus the snapshot (`consensus/snapshot_file.go`, [Snapshots](#snapshots)):

| File | Holds | Written |
|------|-------|---------|
| `raft_log.dat` | 8-byte header (`VXRL`, version 1), then one record per entry: `[len u32][crc32c u32][term u64, index u64, type u8, command]` | appended by the syncer: one `write(2)` of everything queued + one fsync per round (a round covers every group-commit batch or AppendEntries queued before it started) |
| `raft_meta.dat` | `CurrentTerm`, `VotedFor` (CRC-checked) | temp file → fsync → rename → directory fsync, only when term or vote changes |
| `raft_snapshot.dat` | streaming state-machine snapshot: header (index/term, configuration, ID, checksums), CRC'd records, trailer | streamed to a temp file → fsync → rename → directory fsync, on compaction / InstallSnapshot (`raft_snapshot.gob` from older builds is migrated on start) |

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

**Rollback.** It depends on the target release and on whether the node has a `raft_snapshot.dat` (checked against the tags; `tests/upgrade` covers v1.16):

| Target | Node without `raft_snapshot.dat` | Node with `raft_snapshot.dat` |
|---|---|---|
| **v1.15, v1.16** (read `raft_log.dat` / `raft_meta.dat`, stream transport, `WantMatch`) | starts as it is — stop cleanly, start v1.16 on the same `--data` (`TestDowngrade_RaftNoSnapshotNoWipe`). Read the marker note under Applied index → Downgrade before upgrading it again | **wipe** (below). v1.16 does not read `raft_snapshot.dat`: it loads the compacted `raft_log.dat` with no snapshot under it and refuses to start with `corrupt raft log … does not follow the snapshot (index 0)` (`TestDowngrade_Raft`). The exception is an **empty** `raft_log.dat` (every entry compacted), which v1.16 cannot tell from a fresh node — it would start with an empty log over a full engine. That shape is untested (`TestDowngrade_RaftNoWipeEmptyLog` skips when it cannot produce it); do not rely on it — wipe |
| **v1.14 and older** (no `raft_log.dat`) | **wipe** | **wipe** |

A v1.14-or-older build started on a migrated directory finds no `raft_state.gob` and comes up at term 0 with an empty log — it can then vote twice in a term and forget entries it acknowledged, so **do not just start it on the directory**. To wipe-and-resync a node: stop it, delete its whole `--data` directory (Raft state and storage engine — re-applying the log onto existing storage would re-run non-idempotent ops such as INCR), start the older build with the same `--node-id` / `--peers`, and let it catch up from the leader (InstallSnapshot or log replay). Do it one node at a time with a healthy leader and the other two nodes up, followers first, the leader last. Renaming `raft_state.gob.migrated` back is only safe if the node has accepted no entry and no term change since the upgrade, which you normally cannot know.

### Snapshots

Once the retained log reaches `--raft-snapshot-threshold` entries (default 8192; env `VELTRIX_RAFT_SNAPSHOT_THRESHOLD`; 0 = never), the node captures the state machine into `raft_snapshot.dat` and replaces the applied log prefix with it. There is **no size cap**: the snapshot is streamed to disk record by record and never held in memory. (Before 2026-10 a snapshot over 256 MB was skipped — `snapshot too large ... skipping compaction` — and the log then grew in memory and on disk without bound.) CLAUDE.md invariant 64 is the summary.

**File format.** A 4 KB header written last (magic `VXSN`, format version, payload kind, last included index/term, configuration, a random snapshot ID, body length + CRC32C, record count, header CRC), then records `[len u32][crc32c u32][payload]`, then a trailer (marker, record count, CRC). Written to `raft_snapshot_*.tmp`, fsynced, renamed, directory fsynced. Readers verify every checksum; a node verifies the whole file before restoring from it.

**What is in it.** The server's state machine (`cmd/server/raft_snapshot.go`) writes every key of the engine — including the reserved `@vec/` / `@txt/` / `@vecns/` / `@idx/` / `@idxdef/` keys, hash fields, list and set internals and namespaces — with its absolute TTL expiry (µs), followed by the secondary-index definitions.

**Capture does not stall writes.** The capture runs on its own goroutine while Apply continues. The engine notes every key written meanwhile (`storage.WriteTracker`); after the keyspace walk the capture re-reads the noted keys until few remain, then holds the apply lock only for a final drain and read of those few keys. The snapshot is labelled with the index applied at that moment, and it equals the state at that index exactly (the proof is in `raft_snapshot.go`; `TestRaftFSMSnapshot_ConsistentUnderConcurrentApply` checks it against a reference engine). Measured on a laptop (3 in-process nodes over TCP, 1,000,000 keys × 100 B): capture 1.6 s, file 125 MB, Apply fence 0.2–0.3 ms, slowest write during the capture 28 ms — v1.16 took 2.7 s and a write waited 2.76 s behind it. A follower that missed the snapshot caught up in 2.6 s (v1.16: 42 s). 300,000 keys × 1 KB (a 315 MB snapshot, past v1.16's cap): capture 1.2 s, catch-up 1.2–2.1 s (`TestRaftSnapshot_LargeSnapshot`; `TestRaftSnapshot_Bench`, env-gated, for other sizes).

**Restore makes the state machine exactly the snapshot.** Keys absent from the snapshot are deleted, keys whose value or TTL differ are rewritten, keys that already expired are not restored, and the TTLs of the others are kept to the microsecond. Derived keys are written as they are in the snapshot (no secondary-index or vector/text side effects); the search indexes are then rebuilt from the restored keys, as after a restart (searches are refused while that runs). Before 2026-10 a restore only wrote the snapshot's pairs over the follower's engine: every key deleted while the follower was behind survived on it forever, and every restored key became immortal.

**Startup.** A node skips the snapshot restore when its engine already holds the snapshot's state: its durable applied index (below) is at or past the snapshot's last included index and no restore is marked in progress. Otherwise — an interrupted install or startup restore, an engine behind the snapshot, or a data dir from an older build — it restores its engine from the snapshot (a read of every key, writes only for the keys that differ) before applying the log tail. The skip is logged (`snapshot idx=… NOT restored`) and counted (`SnapshotStatus().StartupRestoreSkips`). Measured on a single node whose snapshot holds 1,000,000 keys: see CHANGELOG `[Unreleased]` Fixed.

### Applied index: exactly-once apply across restarts

Raft persists the log, term and vote, but not which entries the state machine has applied. Before 2026-10 a restarted node therefore re-applied its whole retained log (or, with a snapshot, the tail after it) over a storage engine that already held those entries' effects: every non-idempotent command was applied twice — INCR / DECR counters doubled, list pushes and pops repeated, a version-guarded TXN failed its guard, relative TTLs restarted. In a 3-node rolling restart each node's counters ended up different.

The engine now records the applied index with its state (`storage/raft_applied.go`, CLAUDE.md invariant 65):

- After each applied run the FSM appends one **marker record** to the engine WAL (reserved key, value inline, never in the keyspace) carrying the run's last index and term. Its engine version is taken after every write of the run returned (every engine write path returns only once fdatasync'd) and before the next run starts, so in version order the writes of entries ≤ A come before marker(A), and the writes of later entries after it. The marker joins the WAL's next group commit; nothing waits for it.
- In `--mode=raft` (`StorageConfig.RaftAppliedIndex`) WAL replay stops at the newest marker: records with a higher version — the writes of entries that were being applied when the node crashed — are dropped, and raft applies those entries again from its log, once. Because the cut is by version, a partly durable entry is rolled back as a whole, whatever it wrote (list element + meta, a TXN across disks, a field-index backfill, derived search keys). Dropped records stay in `wal.log` until the next checkpoint; the next marker records their version range so a later replay keeps skipping them.
- Raft starts with `lastApplied = commitIndex =` the recorded index. The node may not yet know those entries are committed (the commit index is not persisted), and a follower's log may not even hold them (it can apply an entry the leader reported committed before its own copy is fsynced); both are safe — an entry is applied only once committed, and a re-sent copy is never applied again.
- A snapshot install or startup restore first writes a durable "restore in progress" marker, and after the restore a durable marker at the snapshot index, which clears it. A node that crashes in between restores again on its next start (invariant 64's guarantee).
- Every checkpoint (clean shutdown, `/admin/checkpoint`, backup) carries the marker at the checkpoint's version; a runtime checkpoint runs with Apply quiesced so the marker matches the state.

**Upgrade.** A data dir from an older build has no marker. With a snapshot, the node restores it (the engine becomes exactly the snapshot; a v1.16 `raft_snapshot.gob` has no TTLs, so a key whose value the engine already holds keeps its own expiry) and applies the tail. Without one the log starts at entry 1, so the engine is reset to empty and the whole log applied once (`[raft] … resetting it and applying the N-entry log once`). Either way nothing is applied twice, and the first applied run writes the marker. A node with an empty log (first start, or a standalone data dir switched to raft) keeps its engine as it is.

**Downgrade.** No WAL record version or field changed, and v1.16 replays everything, as it always did — after a clean shutdown on the new build there are no dropped records left to come back. But v1.16 has no notion of the marker: its WAL replay (`applyWALReplay`) installs the record as an ordinary **live key** named `"\x00veltrix/raft-applied"` (in KV-separation mode its value is re-appended to the VLog). On a node downgraded without a wipe (no snapshot, see Persistence → Rollback) the key therefore shows up in `KEYS` / `SCAN` / `DBSIZE` (one extra key), is captured into v1.16's snapshots, and survives v1.16's clean-shutdown checkpoints. It is harmless while the node runs v1.16. **Upgrading that node again needs no wipe.** The marker's value embeds the version of the WAL record that carries it; v1.16's clean-shutdown checkpoint rewrites the record at its own checkpoint version, so whenever v1.16 wrote anything the copy no longer matches and the new build does not trust it. It drops the key from the keyspace (and skips it in a v1.16 snapshot) and takes the no-applied-state path: snapshot restore, or reset and apply the log once — INCRs are not doubled (`tests/upgrade` `TestRaftDowngradeReupgradeIncr`: 10 INCRs on the new build, 5 on v1.16, `GET` = 15 after the re-upgrade). If v1.16 crashed instead of stopping cleanly, the original marker is still intact and trusted and v1.16's later records are cut as after any crash; raft re-applies those entries once. Builds before this fix (the unreleased marker format 1) trusted the rewritten copy.

Standalone and replicated engines never set `RaftAppliedIndex`: they cut nothing and never show the marker key.

**Catch-up (InstallSnapshot).** A follower whose next entry was compacted away receives the snapshot file in chunks (`--raft-snapshot-chunk-kb`, default 2 MB), one chunk in flight, read from an open descriptor so that a newer snapshot replacing the file mid-transfer does not change what is sent. The follower appends a chunk only at the offset it expects and always answers with the offset it wants next, so lost, duplicated and reordered chunks are harmless; a chunk of a different snapshot restarts the transfer. After the last chunk the follower fsyncs and verifies the file, then installs it in the background while it keeps answering heartbeats and votes (the restore never holds the raft lock); its election timer stays quiet meanwhile. The leader keeps heartbeating the follower for the whole transfer and polls until the install is done. A leader change aborts the old transfer; the new leader starts its own.

**Install order and crash safety.** (1) The verified file is renamed over `raft_snapshot.dat` (+ directory fsync) — the commit point. (2) The log keeps the suffix that agrees with the snapshot (Raft §7) or is discarded, and `raft_log.dat` is rewritten. (3) The engine is restored, in place, through the engine that holds the data-dir lock. (4) The applied index moves to the snapshot. A crash before (1) restarts with the old snapshot, log and engine untouched; a crash after it restores the engine from the new snapshot at startup, so a half-done restore is simply redone (step 3 starts with a durable "restore in progress" marker, which keeps the startup restore from being skipped — Applied index, below). `TestRaftSnapshot_InstallCrashAtEveryStage` (tests/integration) kills a follower at each stage and checks it converges.

**Mixed versions (rolling upgrade).** Followers advertise chunked-snapshot support in their AppendEntries replies. To a v1.16 (or older) follower the leader sends the old single-shot InstallSnapshot when the snapshot, in the old format, fits in that build's 256 MB limit; otherwise it logs `NOT sending snapshot … upgrade peer=<id>` and that follower stays behind until upgraded. What a single-shot snapshot does depends on the receiving side:

- **New leader → v1.16 follower:** the follower loses the TTLs of every key in it (the old format has none), and v1.16's restore only writes the snapshot's pairs and never deletes — **every key deleted while that follower lagged survives on it.** Upgrade a v1.16 follower that has fallen behind before it needs a snapshot (in practice: do not let a v1.16 node lag past the leader's compaction during the upgrade).
- **v1.16 leader → new follower:** accepted and installed through the normal path, so deletes are applied (restore = snapshot), but **every key in it loses its TTL** on the new follower — the blob has none. The next snapshot makes that permanent. `TestUpgrade_RaftNewFollowerFromOldLeader` reports it.
- **Upgrade of a node whose own snapshot is a v1.16 `raft_snapshot.gob`:** fixed — `restoreLegacyBlob` keeps the engine's (WAL-replayed) expiry for every key whose value the blob does not change; only a key whose value differs is written without one (`TestUpgrade_RaftSnapshotTTLRepro`).

**Upgrade the followers first, one at a time, and the leader last; move to the next node only after the restarted one has applied a fresh write** (write a marker key through the leader and read it back on the restarted node — node-lifecycle.md#rolling-upgrade). Downgrading to v1.16 and older: those builds do not read `raft_snapshot.dat` — see Persistence → Rollback for which nodes must be wiped.

### Deterministic Apply: version tokens, TTLs and expiry

Every decision a node makes while applying a raft entry must come out the same on every node and on every re-application (CLAUDE.md invariant 66; `storage/raft_clock.go`). Before 2026-10 three of them read the node's own clock:

- **The version token** that `VER` / `GETVER` return and a TXN `SETIF` compares was the key's write timestamp, set from the local clock when the node applied the write. The nodes disagreed on every key's version, so a guarded TXN held on the node whose token the client had read and failed on the others: the replicas diverged (`TestRaftDeterminism_ConcurrentGuardedTxns`). A node rebuilt from the log gave every key a new version, so a guard taken before the rebuild failed on it alone.
- **Relative TTLs** were turned into deadlines with the local clock (`now + ttl`), so the replicas' deadlines differed.
- **Expiry decisions** inside Apply (SETNX / CAS / INCR on a key at its deadline, `SETIF` expecting 0, list and hash ops, `DropNamespace`) used the local clock, and the TTL scanner tombstoned keys on its own time.

Now the leader stamps every command with its wall clock (`fsmCmd.Ts`). Each node turns it into the command's **raft time** `E = max(Ts, E_prev + 1)` — strictly increasing in log order whatever the leaders' clocks — and applies the command at it: every write is stamped `E` (so the version token is `E`, the same on every node, monotonic per key and unique per command), TTL deadlines are `E + ttl`, and expiry is judged at `E`. The clock is state-machine state: it is recorded in the applied-index marker, restored on start, carried by snapshots (format 2 records carry each key's version), and reset with the state, so a clean restart, a crash, a snapshot install and a rebuild from the log all reproduce the same versions (`TestRaftDeterminism_GuardsSurviveRestartAndRebuild`). Reads still hide a key once its deadline passes by the local clock; the TTL scanner, `GET` and `RANGE` only tombstone keys expired at the raft clock (never ahead of the next command). Other fixes in the same pass: Apply waits for the engine's WAL replay (a CAS or guard could be decided on a half-replayed index), namespace quotas are checked by the leader before it submits instead of in Apply (token buckets are per node), and the record-delete search cascade (invariant 60) runs when the record was live at the command's time rather than whenever a not-yet-reaped tombstone remains. Standalone and replicated modes are unchanged: their tokens are local write timestamps.

**Upgrade / mixed versions.** The command format gained one field, `Ts`. A v1.16 node decodes new commands and ignores it; a new node applies a command without it (written by a v1.16 leader, or an old entry re-applied from the log) as before — local clock for TTLs — stamped with a constant legacy value until the clock has started. A key whose version predates the clock (written by v1.16, or by such a command) reports version **1** on every node once the first stamped command is applied; a `SETIF` expecting 1 holds on every node and gives the key a real version. **Restriction: until every node runs the new build and a new-build leader has applied a write, `SETIF` / guarded `TXN` are as nondeterministic as before — do not use them during a rolling upgrade.** (A v1.16 node stamps its own clock; a v1.16 leader sends no `Ts`.) Clients see no wire change: the token is still an opaque u64, 0 = absent.

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

The **retention buffer** (`pendingWrites`) holds every op until every registered replica, live or not, has acked it; acks are tracked per op and per replica. It is bounded by `MaxRetainedOps` (100 000): on overflow the oldest 10 % are dropped and every replica that had not acked them is flagged for a change-feed catch-up (see Replica Recovery). `WaitForReplication` counts the replicas that acked that op, so an ack of a later sequence number never satisfies a wait for an earlier one (concurrent writers can enqueue seq N+1 before N); once the op has left the buffer it falls back to `LastAckSeqNum`. The buffer is in memory; each replica's acknowledgement watermark is persisted, so after a primary restart a replica is caught up from the change feed (Recovery state across a restart, below).

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
   │                             │ 1. if it missed ops that are no longer retained (buffer overflow,
   │                             │    or made before this node restarted): stream the primary's
   │                             │    change feed (StorageEngine.ScanChanges, localOnly: current value
   │                             │    or tombstone of every key THIS node wrote since the watermark
   │                             │    − 10 s, one index pass, pages of 1000, TTLs preserved; the whole
   │                             │    feed if the watermark is older than the tombstone retention)
   │                             │ 2. replay every retained op it has not acked, in SeqNum order,
   │                             │    in BatchSize chunks; repeat until none is left
   └──── any send fails ─────────┤
                                 ▼ (checked under the engine lock — later writes reach it live)
                               SYNC
```

`cmd/server` installs the change-feed source (`engineCatchUp`, `cmd/server/cluster_setup.go`); it waits for the startup WAL replay (`ReplayDone`) before scanning. A library user without `SetCatchUpSource` gets the replay of retained ops only; if ops were dropped the replica is returned to `SYNC` anyway, a warning is logged and `CatchUpGaps` is incremented. The change-feed catch-up ships the primary's current state, not every intermediate version, and like all replication it is applied in arrival order (no conflict resolution).

**Only writes this node originated.** A replica applies a peer's write with `StorageEngine.PutReplicated` / `DeleteReplicated`, which mark the index entry `FlagReplicaApplied` (carried in the WAL record's xflags byte, so it survives restarts); a later local write clears it. The catch-up feed skips marked entries. Without this, a node that came back after being down re-sent its stale copies of its peers' keys to them on its own recovery and undid deletes they had acknowledged (`TestReplicatedRecovery_*` caught it on the primary itself).

#### Recovery state across a restart

The retention buffer and per-op acks are in memory; what survives a restart is one **watermark per replica** in `<--data>/replication_state.json` (`ReplicationConfig.StatePath`, `replication/state.go`): the replica has acknowledged every write this node originated with an earlier timestamp. It is the value the lag monitor already computes for tombstone GC — the timestamp of the oldest write the replica has not acked (retained, evicted, or the start of a catch-up in progress), or now when it is caught up.

- **Written** by the lag monitor once per second when a watermark changed, by `AddReplica`, and by `Close` — temp file, fsync, rename, directory fsync. Never on the write path: steady-state replication pays one small fsync per second per node, independent of the write rate.
- **Read** at startup: a replica with a persisted watermark starts `FAILED` with a pending catch-up from it; the recovery worker probes it at once and it returns to `SYNC` only through the path above (invariant 61), so a replica that is up costs a catch-up of about the last second of writes, and one that is down stays `FAILED` until it answers. Its watermark is reported to tombstone GC immediately (before the first defrag pass), so the deletes it needs are kept. An unreadable file means unknown progress: every replica gets the whole feed. No file (first start, or the upgrade to this build) means every replica is taken as caught up as of now.
- **Why a stale watermark is safe.** A watermark only claims something about writes before it, and it was true when written; progress lost in a crash moves the next catch-up earlier and re-sends keys the replica already has. Re-sending is idempotent: the feed carries each key's current value or tombstone, never an older version, and the replica gets no live batch while `FAILED` / `SYNC_PENDING` — every write after the feed read a key is a retained op, replayed after the catch-up in `SeqNum` order. Replicas apply in arrival order without comparing timestamps, so this ordering is what keeps a re-sent value from overwriting a newer one. `op.Timestamp` is taken after the local write and ops are registered just after it, hence the 10 s `catchUpSkew`.
- **Deletes across a clean restart.** A clean shutdown's WAL checkpoint keeps every tombstone the reaper would still keep (`checkpointKeepsTombstone`), with its timestamp and origin bit, once any replica watermark was reported; replay installs a tombstone for a key it has no entry for. Without replicas the checkpoint still drops every tombstone.
- **A replica further behind than the tombstone retention** (`TombstoneRetention` = 2 × `GCGracePeriodSec`, after which a tombstone is reaped even if a replica has not acked it) is resynced from the whole feed — every live key and every tombstone still held — and flagged: an `ERROR` log line `FULL RESYNC`, `FullResyncs` and `CatchUpGaps` incremented, `DeleteGap` in `GetReplicaLag`. A delete whose tombstone was already reaped cannot be replayed, so the key may survive on that replica; the log says so and names the replica. Rebuild such a replica from a healthy node to be sure.

Limits: a replica only learns writes the primary originated (each node replicates its own writes, and catches peers up only with its own). Concurrent writers to one key on different nodes are not ordered (no conflict resolution), and a catch-up can race such a write like the live stream can.

Replica state is visible in `GET /admin/cluster` (`replication[].state`, `lag_ns`, `last_ack_unix_nano`, `next_probe_unix_nano` while FAILED, `resync_pending`, `delete_gap`) and in Prometheus (below; the recovery counters — `veltrixdb_replication_full_resyncs_total`, `_catchup_gaps_total`, `_replicas_resync_pending`, `_replicas_delete_gap`, `_state_persists_total`, `_state_persist_errors_total` — are in [monitoring.md](monitoring.md#replication-recovery), with the `VeltrixDBReplicationDeleteGap` alert).

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

With no replica watermark (single node, `--mode=raft`) the grace period (`GCGracePeriodSec`, 86400 s) alone applies, as before. Held tombstones are logged per pass (`[gc] tombstones reaped=… held_for_replicas=…`). The coordinator's watermarks are in memory; the replication engine persists its own (Recovery state across a restart, above) and reports them as each replica is registered at startup. A replica that stays down longer than 2 × the grace period can still miss a delete whose tombstone was reaped: it gets a full resync and is flagged (above).

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

- **`--mode=raft`: never.** Every node applies the whole log and holds every key. `cmd/server` marks the transfer agent raft-managed: `--auto-rebalance` is a no-op (logged at startup), `MigrateToNewOwners` returns `ErrMigrationRaftMode` without touching data, and inbound `/transfer/keys` batches are refused with HTTP 403. Raft membership changes go through the log (`RaftNode.AddMember` / `RemoveMember`, behind `/admin/cluster/members` — [node-lifecycle.md](node-lifecycle.md#adding-and-removing-nodes-online-runbook)); a new member is filled by `AppendEntries` / `InstallSnapshot` as a learner before it votes, not by migration. Before this rule, killing the leader of a 3-node raft cluster loaded with 20,000 keys left the survivors with 5,435 and 7,875 keys: every node had sent each key to its single ring owner and deleted it locally.
- **`--mode=replicated`: only on membership changes, and RF-aware.** A node keeps every key it is one of the RF replicas of (`GetReplicasForKey`), copies a key to replicas that newly need it, and deletes a key only when it is no longer among the key's replicas and every current replica acknowledged it. A node failure or recovery is a state change, not a membership change, and moves nothing — catching a recovered replica up is the replication engine's job (Replica Recovery above). With 3 nodes and RF=3 nothing ever moves. The server's replication engine sends every write to every member, so in `cmd/server` every replicated node holds every key, and the server sets the placement RF to the member count (`cluster.AllNodesRF`) so the migration agrees with it. It migrates when a member is added or removed online (invariant 67): every member copies every key to a new member (put-if-absent, marked as a peer write, so a copy never overwrites what the replication stream delivered), while the replication engine gives the new member a catch-up of each member's own writes (`AddMemberReplica`) before live batches; a member is drained (every write it originated acknowledged by every replica, writes refused meanwhile) before it is removed, and then sends its keys to the remaining members, deleting each only once all of them acknowledged it.
