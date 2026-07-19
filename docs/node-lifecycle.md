# Node Lifecycle: Failover, Addition, Removal, and Crash Handling

This document explains exactly what happens at each stage of a node's life in a VeltrixDB cluster — from joining to leaving, and from graceful shutdown to sudden crash.

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
         ├─ saveState() → raft_state.gob
         ├─ resetElectionTimer()
         └─ Send RequestVote{term=2, lastLogIndex=X, lastLogTerm=Y}
                  to node-3 (and node-1, which may be dead)

T=401 ms node-3 receives RequestVote:
         ├─ term 2 > currentTerm 1 → becomeFollower(2)
         ├─ node-2 log is up-to-date (§5.4.1 check passes)
         ├─ VoteGranted = true
         └─ resetElectionTimer()

T=401 ms node-2 receives VoteGranted from node-3:
         votes = 2 (self + node-3) > 3/2  →  becomeLeader()

T=401 ms node-2 becomeLeader():
         ├─ role = Leader
         ├─ LeaderID = "node-2"
         ├─ nextIndex[node-3] = lastLogIndex + 1
         ├─ matchIndex[node-3] = 0
         ├─ Append no-op entry (term=2)   ← Raft §5.4.2
         └─ broadcastAppendEntries()       ← includes no-op

T=451 ms node-2's no-op entry ACKed by node-3 (quorum)
         ├─ maybeAdvanceCommit() → commitIndex advances past no-op
         └─ New leader ready to accept client writes
```

### What Clients Experience

- **Writes to the old leader**: Return `ErrNotLeader`. Clients should retry — a well-behaved client retries with exponential backoff until it finds the new leader.
- **New writes to `node-2`**: Accepted and committed normally ~450 ms after the old leader went down.
- **Reads from followers**: Always served from local state (up to the committed `commitIndex`). Stale reads are possible if a follower hasn't received the latest commits yet.

### No Data Loss

Any write that received an `OK` response from the old leader was committed by quorum — at least 2 of 3 nodes persisted it to `raft_state.gob`. The new leader will apply those entries before accepting new writes.

Writes that received `ErrNotLeader` or timed out may or may not have been committed. Clients with **at-least-once** semantics should retry with idempotent operations (use `SetIfNotExists` / `CompareAndSwap` for exactly-once semantics).

### The No-Op Entry (Why It Matters)

When `node-2` becomes leader, it cannot immediately commit any uncommitted entries from term 1 — Raft §5.4.2 prohibits committing prior-term entries directly. The no-op entry in term 2 propagates through the cluster; once committed, it implicitly commits all prior-term entries that precede it. This is why there is a brief (~50 ms) window after leader election before the new leader accepts writes.

---

## Node Addition

**Scenario**: Adding `node-4` to an existing 3-node cluster.

### Phase 1: Register the Node

```go
pm.AddNodeAndRebalance("node-4", "10.0.0.4", 9000, "10.0.0.4:9100", 256, ta)
```

This performs four steps atomically from the coordinator's perspective:

```
1. pm.AddNode("node-4", "10.0.0.4", 9000)
   ├─ Node{state=ACTIVE} added to Nodes map
   ├─ 64 virtual nodes added to ConsistentHashRing
   └─ pm.Version++

2. Gossip propagates new PartitionMap version to all nodes
   └─ All nodes learn about node-4 within ~7 gossip rounds (≈7 s)
```

### Phase 2: Rebalance (Partition Reassignment)

```
pm.Rebalance(256)
    │
    ▼
256 partitions recalculated with 4 nodes:
    Before: node-1, node-2, node-3 each own ~85 partitions primary
    After:  node-1, node-2, node-3, node-4 each own ~64 partitions primary

    ~21 partitions per node are reassigned to node-4
    (but no data has moved yet — only routing table updated)
```

### Phase 3: Data Migration (Background)

```
TransferAgent.MigrateToNewOwners()   [runs in background goroutine]
    │
    ▼
ScanKeys() → enumerate all keys on node-1 (the coordinator)
    │
    ▼
For each key:
    hash = FNV-1a(key)
    newOwner = ring.GetNode(hash)
    if newOwner == "node-4": queue for migration

    ▼
Fan out to node-4 via HTTP:
    POST http://10.0.0.4:9100/transfer/keys
    Body: {src:"node-1", keys:[{k,v,ttl}, ...]}  (500 keys per batch)
    │
    ▼
node-4 receives and puts each key into its local StorageEngine
    │
    ▼
node-1 deletes successfully-migrated keys locally
```

**During migration**, reads/writes to migrating keys are still served correctly — the ring routes new requests to `node-4` immediately after rebalance, and old requests to `node-1` are still served from its local copy until deleted.

### Phase 4: Raft Group Expansion

To add `node-4` to the Raft consensus group (so it participates in quorum decisions):

```go
raftNode.AddPeer("node-4")
// node-4 creates its own RaftNode with the cluster peers and starts
// receiving AppendEntries from the current leader
```

Once `node-4`'s Raft log is caught up (leader's `nextIndex["node-4"]` matches `matchIndex["node-4"]`), it counts toward quorum.

---

## Node Removal (Graceful Departure)

**Scenario**: `node-3` is being decommissioned — hardware replaced, cluster shrinking.

### Step 1: Mark as Draining

```go
pm.UpdateNodeState("node-3", NodeStateDraining)
```

State becomes `DRAINING`. Health checks and metrics see a deliberate departure. New requests stop being routed to `node-3`.

### Step 2: Remove from Ring + Rebalance

```go
pm.RemoveNodeAndRebalance("node-3", 256, node3TransferAgent)
```

Internally:
```
1. pm.RemoveNode("node-3")
   ├─ Delete from Nodes map
   ├─ Remove 64 virtual nodes from ring
   └─ pm.Version++

2. pm.Rebalance(256)
   └─ node-3's partitions reassigned to node-1 and node-2

3. [background] node3TransferAgent.MigrateToNewOwners()
   └─ Scan node-3's local keys → identify new owners → transfer
```

**Critical**: The `TransferAgent` passed to `RemoveNodeAndRebalance` MUST be `node-3`'s own agent (`ta.localNodeID == "node-3"`). Using another node's agent would scan the wrong data store.

### Step 3: Evacuation Completes

```
[transfer] evacuating node=node-3
[transfer] migration done  moved=847291  errors=false
[transfer] evacuation complete node=node-3
```

Once all keys are transferred, `node-3` can be safely shut down. Its Raft log entries are already replicated to `node-1` and `node-2`, so no committed data is lost.

---

## Node Crash (Unclean Failure)

**Scenario**: `node-2` crashes (OOM kill, hardware failure, kernel panic). It stops sending heartbeats without a graceful DRAINING transition.

### Detection Timeline

```
T=0 s   node-2 crashes
T=1 s   FailureDetector.checkNodeHealth() tick on node-1 and node-3
        timeSinceHeartbeat > SuspectThreshold (3 s): NOT YET
T=3 s   checkNodeHealth() tick:
        timeSinceHeartbeat = 3 s > SuspectThreshold
        → UpdateNodeState("node-2", NodeStateSuspect)
T=10 s  checkNodeHealth() tick:
        timeSinceHeartbeat = 10 s > FailureThreshold
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
    if fd.pingNode(nodeID, node.Address, node.Port) {
        // Node is back online
        fd.partitionMap.UpdateNodeState(nodeID, NodeStateRecovering)
        fd.triggerRebalance()
        return true
    }
    return false
}
```

Up to `MaxRecoveryRetries` (3) attempts are made, spaced `RecoveryInterval` (5 s) apart. If all fail, the node stays `FAILED` and an operator must intervene.

### Crash Recovery on the Crashed Node

When `node-2` restarts:

```
1. RaftNode.loadState()
   └─ Read raft_state.gob → restore (CurrentTerm, VotedFor, Log)

2. StorageEngine starts → replayWAL()
   ├─ Open wal.log on each disk
   ├─ Parse every 7-field WAL record
   └─ applyWALReplay() → rebuild shardedIndex in RAM
      (WAL file was not zeroed on crash — replay needed)

3. RaftNode starts as Follower
   ├─ Election timer starts (400–800 ms)
   └─ Current leader sends AppendEntries → node-2 becomes follower again

4. Leader backfills missing log entries:
   ├─ node-2.nextIndex was saved as 0 on leader → leader sends full log
   └─ node-2 applies all missed entries to StateMachine (StorageEngine)

5. node-2 is caught up → counts toward quorum again

6. FailureDetector.RecordHeartbeat("node-2")
   └─ UpdateNodeState("node-2", NodeStateRecovering) → NodeStateActive
```

### Force-Remove (Node Never Comes Back)

If `node-2` cannot be recovered (hardware is destroyed):

```go
pm.ForceRemoveNode("node-2", 256)
```

This removes the node from the ring and rebalances partition assignments to `node-1` and `node-3`. No data evacuation — surviving replicas already have the data (replication factor ensures this). The Replication Engine's anti-entropy re-replicates any missing copies to restore `RF=3`.

---

## Node Recovery (Returning After Failure)

When a crashed node comes back online after repair:

```
1. raft_state.gob still present → node knows its last term and voted-for
2. WAL replay rebuilds the index from the last clean state
3. Node joins as Follower
4. Leader sends missing AppendEntries to catch up the log
5. Once matchIndex[recovered_node] == leader.lastLogIndex:
   ├─ Committed entries are re-applied to StorageEngine
   └─ Node is fully caught up
6. PartitionMap.UpdateNodeState → NodeStateActive
7. Node re-enters ring → new writes routed to it
```

During the catch-up phase, the recovering node serves reads from its potentially-stale local state. Clients that need strong consistency should read from the leader or use quorum reads.

---

## Summary: What Each Event Guarantees

| Event | Data Loss? | Write Downtime | Read Downtime |
|-------|-----------|----------------|---------------|
| Leader failover (Raft 3-node) | None (committed writes safe) | ~400–800 ms election + ~50 ms no-op commit | None (followers serve reads) |
| Node addition | None | None | None |
| Graceful removal (DRAINING) | None | None | None |
| Node crash (follower, RF=3) | None (2 of 3 have data) | None | None |
| Node crash (leader, RF=3) | None (committed writes) | ~450 ms | None |
| Two nodes crash (RF=3) | Possible (quorum lost) | Indefinite until 1 recovers | Degraded |
| Force-remove dead node | None (surviving replicas) | None | None |

---

## Relevant Prometheus Metrics

| Metric | Meaning |
|--------|---------|
| `veltrixdb_fd_suspected_nodes` | Nodes currently in SUSPECT state |
| `veltrixdb_fd_failed_nodes` | Nodes currently in FAILED state |
| `veltrixdb_fd_false_positives_total` | Nodes suspected then recovered (flapping) |
| `veltrixdb_cluster_partition_migrations_total` | Data migration batches completed |
| `veltrixdb_cluster_rebalances_total` | Partition map rebalances triggered |
| `veltrixdb_raft_term` | Current Raft term (increments on every election) |
| `veltrixdb_raft_leader_id` | Which node is the current leader |
