# Node Lifecycle: Failover, Addition, Removal, and Crash Handling

This document explains exactly what happens at each stage of a node's life in a VeltrixDB cluster — from joining to leaving, and from graceful shutdown to sudden crash.

**What `cmd/server` does vs. the library.** A server builds its membership once, at startup, from `--peers id@host:port,...` (or `--seeds nodeID=host:port,...`): every listed node is `AddNode`d to the local partition map. There is no admin endpoint or command that adds, drains or removes a node at runtime, and gossip does not carry membership (`cluster/gossip.go`: nodes a digest names that the receiver does not know are ignored). In `--mode=replicated`, `--auto-rebalance` (default true) subscribes to membership changes (a node added to or removed from the map) and, after a 3 s debounce, runs `Rebalance` + `MigrateToNewOwners` (`cmd/server/rebalancer.go`); node state changes (SUSPECT / FAILED / RECOVERING / ACTIVE) never start a migration. In `--mode=raft` the flag is a no-op: every node holds the full raft-replicated state, so the transfer agent is marked raft-managed, `MigrateToNewOwners` refuses (`ErrMigrationRaftMode`) and inbound `/transfer/keys` batches get HTTP 403 (invariant 62; before this, killing the leader of a loaded 3-node raft cluster deleted about two thirds of the keys on each survivor). Since a running server's membership never changes, in practice it never migrates. The join / drain / force-remove calls below (`AddNodeAndRebalance`, `RemoveNodeAndRebalance`, `ForceRemoveNode`, `RaftNode.AddServer`) are Go APIs in `cluster/` and `consensus/` for an embedding program; the server does not call them.

---

## Node Failover (Leader Goes Down)

**Scenario**: The current Raft leader (e.g., `node-1`) becomes unavailable — either crashed, OOM-killed, or network-isolated.

### Timeline

```
T=0 ms   Leader (node-1) stops sending heartbeats
T=50 ms  Followers expect heartbeat every 50 ms — none arrives
T=400 ms Follower election timer fires (randomized 400–800 ms)
         First follower to fire becomes candidate

T=400 ms node-2 startElection():
         ├─ ps.CurrentTerm++    (term 2)
         ├─ ps.VotedFor = "node-2"
         ├─ role = Candidate
         ├─ persist term + vote → raft_meta.dat (before any RequestVote)
         ├─ resetElectionTimer()
         └─ Send RequestVote{term=2, lastLogIndex=X, lastLogTerm=Y}
                  to node-3 (and node-1, which may be dead)

T=401 ms node-3 receives RequestVote:
         ├─ term 2 > currentTerm 1 → becomeFollower(2)
         ├─ node-2 log is up-to-date (§5.4.1 check passes)
         ├─ VoteGranted = true
         └─ resetElectionTimer()

T=401 ms node-2 receives VoteGranted from node-3:
         votes = 2 (self + node-3) ≥ quorum (3/2 + 1 = 2)  →  becomeLeader()

T=401 ms node-2 becomeLeader():
         ├─ role = Leader
         ├─ LeaderID = "node-2"
         ├─ nextIndex[node-3] = lastLogIndex + 1
         ├─ matchIndex[node-3] = 0
         ├─ Append no-op entry (term=2)   ← Raft §5.4.2
         └─ append no-op to raft_log.dat, fsync, then broadcastAppendEntries()
                                           ← sent at once, includes no-op

T≈402 ms node-2's no-op entry ACKed by node-3 (quorum) — one round trip
         ├─ maybeAdvanceCommit() → commitIndex advances past no-op
         └─ New leader ready to accept client writes
```

### What Clients Experience

- **Writes to a non-leader**: answered `MOVED <leader-addr> <leader-id>`, or `MOVED - (leader unknown, retry)` while no leader is known (`cmd/server/coordinator.go`); writes to the dead old leader simply fail to connect. Clients retry with backoff until they reach the new leader.
- **New writes to `node-2`**: Accepted and committed normally once it is elected (~400–800 ms after the last heartbeat) plus one round trip for the no-op.
- **Reads from followers**: served from local state (whatever the follower has applied). Stale reads are possible if a follower hasn't received the latest commits yet. With `--linearizable-reads`, followers redirect every read (not only GET) to the leader, which runs a ReadIndex fence.
- **Reads from the new leader**: held until the new leader has applied its term's no-op (typically one heartbeat round, bounded by 2 s), so they include every write the old leader acknowledged. Before this barrier a GET immediately after failover could return "not found" for an acknowledged write (`TestRaftClusterFailover`).
- **Searches**: fail with `search incomplete: ... did not answer` naming the dead node until the failure detector marks it failed, then run on the remaining nodes (`--search-allow-partial` answers without it straight away).

### No Data Loss

Any write that received an `OK` response from the old leader was committed by quorum — at least 2 of 3 nodes fsynced it to `raft_log.dat`. The new leader will apply those entries before accepting new writes.

Writes that received `MOVED` were not submitted. Writes that timed out or lost their connection may or may not have been committed. Clients with **at-least-once** semantics should retry with idempotent operations (use `SetIfNotExists` / `CompareAndSwap` for exactly-once semantics).

### The No-Op Entry (Why It Matters)

When `node-2` becomes leader, it cannot immediately commit any uncommitted entries from term 1 — Raft §5.4.2 prohibits committing prior-term entries directly. The no-op entry in term 2 propagates through the cluster; once committed, it implicitly commits all prior-term entries that precede it. Until the no-op commits (one replication round trip) the new leader holds local reads (`WaitLeaderApplied`, up to 2 s).

---

## Node Addition

**Scenario**: Adding `node-4` to an existing 3-node cluster.

### Phase 1: Register the Node

```go
pm.AddNodeAndRebalance("node-4", "10.0.0.4", 9000, "10.0.0.4:9005", 256, ta)
```

(`cmd/server` derives the transfer listener as client port + 5, e.g. `:9005`; `--transfer-addr` overrides it locally.) This runs, on the partition map it is called on:

```
1. pm.AddNodeWithTransfer("node-4", "10.0.0.4", 9000, "10.0.0.4:9005")
   ├─ Node{state=ACTIVE} added to Nodes map
   ├─ 64 virtual nodes added to ConsistentHashRing
   ├─ pm.Version++, pm.epoch++ (split-brain fencing)
   └─ transfer address registered

2. pm.Rebalance(256)
3. go ta.MigrateToNewOwners()      (only if ta != nil)
```

Gossip does **not** propagate the new member: every node's partition map must have `AddNode("node-4", ...)` called on it (for `cmd/server`, restart every node with node-4 in `--peers`). Once node-4 is known, gossip carries its liveness, gossip address and rack.

### Phase 2: Rebalance (Partition Reassignment)

```
pm.Rebalance(256)
    │
    ▼
256 partitions reassigned round-robin over the ACTIVE nodes (sorted by ID):
    Before: node-1, node-2, node-3 each own 85–86 partitions primary
    After:  node-1, node-2, node-3, node-4 each own 64 partitions primary
    (no data has moved yet)

Key ownership itself comes from the consistent-hash ring, not this table:
node-4's 64 vnodes take roughly a quarter of the key space.
```

### Phase 3: Data Migration (Background)

```
TransferAgent.MigrateToNewOwners()   [runs in background goroutine]
    │
    ▼
ScanKeys() → enumerate all live keys on the node that owns ta
    │
    ▼
For each key (placement = GetReplicasForKey: RF ring successors, rack-aware;
              @vec/@txt/@idx keys route as their record):
    now  = replicas under the current membership
    prev = replicas under the agent's baseline membership (before node-4)
    if local node ∈ now:  copy to now − prev (here: node-4, if it became a replica)
    else:                 send to every member of now; delete locally only
                          after all of them acknowledged
    (@vecns/ and @idxdef/ keys are copied to every destination, never moved)

    ▼
Fan out to each destination in parallel via HTTP:
    POST http://10.0.0.4:9005/transfer/keys
    Body: {src:"node-1", epoch:E, keys:[{k,v,ttl}, ...]}  (500 keys per batch)
    (HMAC-signed with --cluster-secret-file; a stale epoch gets 409)
    │
    ▼
node-4 receives and puts each key into its local StorageEngine
    │
    ▼
node-1 deletes the keys it is no longer a replica of, once every replica acked them
```

With RF=3, node-1 stops being a replica only of keys for which node-4 took one of the three slots from it; the rest are copied to node-4 and kept. (With 3 nodes and RF=3 every node is a replica of every key and nothing would ever move.)

**During migration**, the ring routes requests for re-owned keys to `node-4` as soon as it is added, but a key is only on `node-4` once its batch has been delivered: until then a read routed to `node-4` can miss it. `node-1` keeps its copy until every replica acknowledged it (failed batches are not deleted and are retried on the next migration).

### Phase 4: Raft Group Expansion

To add `node-4` to the Raft consensus group (so it participates in quorum decisions), call on the leader:

```go
err := raftNode.AddServer("node-4")
// appends a configuration entry and blocks until it commits under the new
// quorum; ErrNotLeader on followers, ErrConfigChangeInProgress while an
// earlier change is uncommitted. node-4 runs its own RaftNode and receives
// AppendEntries (or InstallSnapshot) from the leader.
```

`AddPeer` still exists but is deprecated: it changes only the local view, without consensus. The new member counts toward quorum as soon as the configuration entry is in effect; `RemoveServer(id)` is the inverse. `cmd/server` never calls either — its Raft membership is the static `--peers` list.

---

## Node Removal (Graceful Departure)

**Scenario**: `node-3` is being decommissioned — hardware replaced, cluster shrinking.

### Step 1: Drain, Remove from Ring, Rebalance

```go
pm.RemoveNodeAndRebalance("node-3", 256, node3TransferAgent)
```

Internally:
```
1. pm.UpdateNodeState("node-3", NodeStateDraining)
   └─ state DRAINING, so health checks and metrics see a deliberate departure
      (routing is unchanged until step 2: the ring ignores node state)

2. pm.RemoveNode("node-3")
   ├─ Delete from Nodes map
   ├─ Remove 64 virtual nodes from ring   ← new requests stop routing here
   └─ pm.Version++, pm.epoch++

3. pm.Rebalance(256)
   └─ node-3's partitions reassigned to node-1 and node-2

4. [background] node3TransferAgent.MigrateToNewOwners()
   └─ Scan node-3's local keys → send each to every member of its current
      replica set → delete it once all of them acknowledged
```

The surviving members should also run `MigrateToNewOwners` (the auto-rebalancer does, on the "removed" event) so that replicas which inherited node-3's slots get copies from them too.

**Critical**: The `TransferAgent` passed to `RemoveNodeAndRebalance` MUST be `node-3`'s own agent (`ta.localNodeID == "node-3"`); any other agent is rejected with an error. `nil` skips evacuation. Like node addition, this changes only the partition map it is called on — every node's map must drop node-3.

### Step 2: Evacuation Completes

```
[transfer] evacuating node=node-3
[transfer] migration done  copied=0  moved=847291  kept-unacked=0  errors=false
[transfer] evacuation complete node=node-3
```

Once all keys are transferred, `node-3` can be safely shut down. Its Raft log entries are already replicated to `node-1` and `node-2`, so no committed data is lost. In raft mode, also remove it from the Raft configuration with `RemoveServer("node-3")` on the leader.

---

## Node Crash (Unclean Failure)

**Scenario**: `node-2` crashes (OOM kill, hardware failure, kernel panic). It stops sending heartbeats without a graceful DRAINING transition.

### Detection Timeline

```
T=0 s     node-2 crashes (last heartbeat recorded via gossip, 1 s rounds)
T=1 s     FailureDetector.checkNodeHealth() tick (every 1 s) on node-1 and node-3
          timeSinceHeartbeat > SuspectThreshold (3 s): NOT YET
T≈3–4 s   first tick with timeSinceHeartbeat > 3 s
          → UpdateNodeState("node-2", NodeStateSuspect)
T≈10–11 s first tick with timeSinceHeartbeat > FailureThreshold (10 s)
          → UpdateNodeState("node-2", NodeStateFailed)
          → failedNodes["node-2"] = true
          → push "node-2" to recoveryQueue
```

### If node-2 Was the Raft Leader

The Raft election timer fires within 400–800 ms after heartbeats stop. A new leader is elected (see "Node Failover" section above). No write is lost as long as it was committed (quorum acknowledged).

### If node-2 Was a Follower

The remaining 2 nodes (leader + 1 follower) still form a quorum for a 3-node cluster. Writes continue normally. The dead node's absence only becomes a problem if a second node also fails — at that point quorum is lost.

### Recovery Attempts

The `backgroundRecoveryWorker` attempts to recover the failed node:

```go
func (fd *FailureDetector) attemptNodeRecovery(nodeID string) bool {
    node := fd.partitionMap.Nodes[nodeID]          // under pm.mu.RLock
    if fd.pingNode(node.Address, node.Port) {       // TCP "PING\n", needs a reply, 2 s timeout
        // Node is back online
        fd.partitionMap.UpdateNodeState(nodeID, NodeStateRecovering)
        fd.triggerRebalance()
        return true
    }
    return false
}
```

Up to `MaxRecoveryRetries` (3) attempts are made. A failed attempt re-queues the node immediately, so the attempts run back to back, each bounded by the 2 s ping timeout. If all fail, the node stays `FAILED` until a heartbeat arrives (see step 6 below) or an operator intervenes.

### RECOVERING → ACTIVE

Every `RecoveryInterval` (5 s) the same worker pings each `RECOVERING` node (`promoteRecoveredNodes`, `cluster/failure_detection.go`). After `RecoveryConfirmations` (2) consecutive successful pings, with a heartbeat younger than `SuspectThreshold` (3 s), the node becomes `ACTIVE` (`veltrixdb_failure_detector_nodes_reactivated_total`); a failed ping resets the count. The transition triggers `Rebalance`, which assigns partitions to ACTIVE nodes only, so the node owns partitions again. No data moves: the node never left the ring or the membership, it kept its data, and the auto-rebalancer ignores state changes (before invariant 62 it ran `MigrateToNewOwners` on FAILED / RECOVERING / ACTIVE events, which deleted keys on the survivors). Search fan-out (`PartitionMap.SearchPeers`) skips only FAILED nodes, so a node is searched again as soon as it is RECOVERING. A RECOVERING node whose heartbeats stop is marked SUSPECT / FAILED by the heartbeat checker as usual.

### Crash Recovery on the Crashed Node

When `node-2` restarts:

```
1. RaftNode starts (NewRaftNodeWithOptions)
   ├─ Restore raft_snapshot.gob if present (state machine + lastIncludedIndex/Term)
   └─ loadState(): raft_meta.dat → (CurrentTerm, VotedFor); replay raft_log.dat → retained
      log tail (torn tail truncated; a legacy raft_state.gob is migrated first)

2. StorageEngine starts → replayWAL()
   ├─ Open wal.log on each disk
   ├─ Decode every record with one reader: binary records (the default,
   │  CRC32C over the whole record) and legacy 10-field text records
   │  (6/7/8-field still accepted) may be mixed in one file
   ├─ A torn or corrupt record ends replay; everything before it is kept
   │  and the stop offset is logged
   └─ applyWALReplay() → rebuild shardedIndex in RAM
      (after a crash wal.log holds every record since the last clean-shutdown
       checkpoint — replay needed)

3. RaftNode starts as Follower
   ├─ Election timer starts (400–800 ms)
   └─ Current leader sends AppendEntries → node-2 becomes follower again

4. Leader backfills missing log entries:
   ├─ AppendEntries consistency check fails → leader backs nextIndex["node-2"] down
   ├─ If the entries node-2 needs were compacted into the leader's snapshot,
   │  the leader sends one InstallSnapshot RPC instead
   └─ node-2 applies all missed entries to StateMachine (StorageEngine)

5. node-2 is caught up → counts toward quorum again

6. FailureDetector.RecordHeartbeat("node-2")   (gossip from node-2 again)
   └─ FAILED → UpdateNodeState("node-2", NodeStateRecovering)
      (searched again from here on; key routing uses the ring, which
       still contains it)

7. Recovery worker, every RecoveryInterval (5 s): 2 consecutive successful
   pings + fresh heartbeat → UpdateNodeState("node-2", NodeStateActive)
   └─ Rebalance: node-2 owns partitions again (no data moves — state
      changes never trigger migration, invariant 62)

8. --mode=replicated only: each peer's ReplicationEngine had marked node-2
   FAILED on its first failed send; its recovery worker probes node-2 with
   backoff (1 s doubling to 30 s), moves it to SYNC_PENDING, replays the
   writes node-2 missed and returns it to SYNC (docs/replication.md,
   Replica Recovery)
```

### Force-Remove (Node Never Comes Back)

If `node-2` cannot be recovered (hardware is destroyed):

```go
pm.ForceRemoveNode("node-2", 256)
```

This removes the node from the ring and rebalances partition assignments to `node-1` and `node-3`. No data evacuation — surviving replicas are the source of truth. `ForceRemoveNode` itself copies nothing; a `MigrateToNewOwners()` pass on each survivor afterwards (the auto-rebalancer runs one on the "removed" event) copies every key the survivor holds to replicas that newly need it, restoring the replica count where spare nodes exist. That pass deletes a key only on a survivor that left the key's replica set, and only after every current replica acknowledged it.

---

## Node Recovery (Returning After Failure)

When a crashed node comes back online after repair:

```
1. raft_meta.dat / raft_log.dat still present → node knows its last term, voted-for and log
2. WAL replay rebuilds the index (last clean-shutdown checkpoint + every record after it)
3. Node joins as Follower
4. Leader sends missing AppendEntries to catch up the log
5. Once matchIndex[recovered_node] == leader.lastLogIndex:
   ├─ Committed entries are re-applied to StorageEngine
   └─ Node is fully caught up
6. Peers' failure detectors see its heartbeat again → RECOVERING
   (if it had been marked FAILED; a SUSPECT node goes straight back to ACTIVE)
7. Two consecutive successful recovery pings → ACTIVE, rebalanced back into
   the partition table
8. It never left the ring (only ForceRemoveNode / RemoveNode take it out),
   so keys it owns route to it throughout
```

During the catch-up phase, the recovering node serves reads from its potentially-stale local state. Clients that need strong consistency should read from the leader or use quorum reads.

---

## Clean Shutdown, Restart and Upgrades

### Clean shutdown

`StorageEngine.Close()` (SIGTERM / normal exit) drains in this order: write batcher → C++ batch engine → io_uring bridge (if enabled) → defrag and compaction workers → segment files and VLogs → WALs. Each WAL is then rewritten as a **compacted checkpoint** — one record per live key, written to a temp file and atomically renamed — so the next startup replays O(live keys), not O(total writes). A crash during the checkpoint leaves the old `wal.log` intact. The native off-heap index is freed **last**, after the checkpoint (its final reader); the Go GC cannot reclaim it.

A node killed without a clean shutdown (SIGKILL, OOM, power loss) keeps its full WAL and replays it on restart, as described above.

### Before starting a node (Linux, cgo builds)

cgo builds keep the index off the Go heap, with one `mmap` per large shard array. Set `vm.max_map_count ≥ 262144` (`scripts/sysctl.conf`); with the default 65530, inserts can fail with `ENOMEM` at large key counts. `VELTRIXDB_INDEX=map` falls back to the Go map index.

### Network front-end in cluster modes

`--mode=raft` and `--mode=replicated` require `--net=go` (the default). `cmd/server` exits at startup if `--net=cpp|uring|poll` is combined with any mode other than `--mode=standalone` (or with `--auth-config`): the C++ front-end writes straight to the local engine and would bypass Raft and replication.

### Upgrade and rollback (WAL format)

New builds write **binary WAL records** by default and replay both binary and text, so an upgraded node reads its existing text `wal.log` and appends binary records after it — a rolling upgrade needs no extra step.

A pre-binary build cannot read binary records, and no build older than TTL-in-WAL can read the record of a key with a TTL (binary version 2 or the 11th text field) — it stops replay there and drops every later record. To roll a node back to any older build:

```
1. Restart the current build once with --wal-format=text-legacy
2. Stop it cleanly (the clean-shutdown checkpoint rewrites wal.log as text, without TTLs)
3. Start the older build
```

Keys written with a TTL come back immortal on the older build. `--wal-format=text` keeps TTLs and is not a rollback mode. Rolling back a node that was not stopped cleanly after step 1 leaves records in `wal.log` that the old build cannot replay.

---

## Summary: What Each Event Guarantees

| Event | Data Loss? | Write Downtime | Read Downtime |
|-------|-----------|----------------|---------------|
| Leader failover (Raft 3-node) | None (committed writes safe) | ~400–800 ms election + one round trip for the no-op | None (followers serve reads) |
| Node addition | None | None | None |
| Graceful removal (DRAINING) | None | None | None |
| Node crash (follower, RF=3) | None (2 of 3 have data) | None | None |
| Node crash (leader, RF=3) | None (committed writes) | ~400–800 ms | None |
| Two nodes crash (RF=3) | Possible (quorum lost) | Indefinite until 1 recovers | Degraded |
| Force-remove dead node | None (surviving replicas) | None | None |

---

## Relevant Prometheus Metrics

| Metric | Meaning |
|--------|---------|
| `veltrixdb_failure_detector_nodes_failed_total` | Counter: nodes marked FAILED by the heartbeat monitor |
| `veltrixdb_failure_detector_nodes_recovered_total` | Counter: nodes that recovered after being marked FAILED. Failed minus recovered over a window = nodes still down |
| `veltrixdb_failure_detector_false_positives_total` | Nodes suspected then recovered (flapping) |
| `veltrixdb_failure_detector_nodes_reactivated_total` | RECOVERING nodes promoted back to ACTIVE |
| `veltrixdb_cluster_nodes_total` | Gauge: registered cluster nodes |
| `veltrixdb_cluster_partition_migrations_total` | Key batches (≤ 500 keys) a new owner acknowledged during migration (`TransferAgent.sendBatches`); keys moved per run are in the `[transfer] migration done  moved=N` log line |
| `veltrixdb_cluster_rebalances_total` | Partition map rebalances triggered |

There is no gauge for nodes currently in SUSPECT state, and the Raft term and
leader are not Prometheus metrics: read them from `GET /admin/cluster`
(`raft.term`, `raft.leader_id`, raft mode only) or `veltrix nodes`.
