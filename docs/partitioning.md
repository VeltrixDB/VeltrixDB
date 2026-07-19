# Partitioning and Rebalancing in VeltrixDB

VeltrixDB uses **consistent hashing with virtual nodes** to distribute data across cluster nodes. This document covers how keys are routed, how partitions are assigned, and how the cluster rebalances when nodes are added or removed.

---

## Single-Node Partitioning (Within a Node)

Before reaching cluster-level partitioning, every key is routed to a specific **shard** and **disk** on the local node:

```
key  ──► FNV-1a hash ──► shard_id = hash & 0x3FF   (0..1023)
                       ──► disk_idx = shard_id % numDisks
```

1024 in-memory shards each hold their own `sync.RWMutex`. This allows concurrent reads and writes to different key ranges without global locking. The C++ `kNumShards` constant and Go `numShards` constant are both 1024 — they must always match.

---

## Cluster-Level Partitioning

### Consistent Hash Ring

The `ConsistentHashRing` (`cluster/partition_map.go`) maps a continuous 64-bit hash space onto physical nodes using **virtual nodes**:

```go
type ConsistentHashRing struct {
    virtualNodes map[uint64]string  // hash → node_id
    sortedKeys   []uint64           // sorted for binary search
    replicas     int                // virtual nodes per physical node (default: 64)
}
```

**Virtual nodes** (default 64 per physical node) provide two benefits:
1. **Load balance**: each physical node owns ~`64 / totalVirtualNodes` of the ring. More virtual nodes → more even distribution.
2. **Gradual rebalancing**: adding one physical node moves `1/N` of the ring's data (where N = new node count) instead of bulk-moving half the data.

### How Keys Are Routed

```
key = "user:42"
    │
    ▼
hash = FNV-1a("user:42")   →   uint64 hash value
    │
    ▼
Binary search in sortedKeys for first virtualNode hash >= key hash
    │
    ▼
If none found, wrap around to sortedKeys[0]   (ring property)
    │
    ▼
nodeID = virtualNodes[foundHash]              →  "node-2"
    │
    ▼
Route request to node-2
```

### PartitionMap

The `PartitionMap` ties the ring to partition metadata:

```go
type PartitionMap struct {
    Version            uint64                    // increments on every topology change
    Timestamp          int64                     // last modification time (ns)
    Nodes              map[string]*Node          // nodeID → Node
    Partitions         map[uint32]*PartitionInfo // 256 partitions
    Ring               *ConsistentHashRing
    ReplicationFactor  int                       // default: 3
}
```

**256 hash partitions** divide the uint64 range into equal-sized buckets:

```
partition_i covers hash range: [i × (2^64/256), (i+1) × (2^64/256))
```

Each partition has a primary node and up to `ReplicationFactor - 1` replica nodes.

### PartitionInfo

```go
type PartitionInfo struct {
    PartitionID   uint32
    HashRange     [2]uint64  // [start, end)
    PrimaryNode   string     // which node is primary
    ReplicaNodes  []string   // backup nodes (up to RF-1)
    Version       uint64
    LastModified  int64
}
```

### Getting Replicas for a Key

```go
// Returns [primaryNode, replica1, replica2, ...]
nodes, err := pm.GetReplicasForKey(key)
```

Internally: hash the key, find the primary on the ring, then walk the ring clockwise skipping physical nodes already seen until `ReplicationFactor` distinct nodes are collected.

---

## Node States

Each cluster node cycles through these states:

| State | Meaning |
|-------|---------|
| `ACTIVE` | Fully operational, accepting reads and writes |
| `SUSPECT` | Missing heartbeats for 3 s — health uncertain |
| `FAILED` | Missing heartbeats for 10 s — removed from routing |
| `RECOVERING` | Recently came back online — being re-integrated |
| `DRAINING` | Graceful departure in progress — ring already updated, data evacuating |

State transitions are propagated via the **gossip protocol** — every node sends its view of cluster state to 3 random peers every second. All nodes converge to a consistent view within a few gossip rounds.

---

## Rebalancing

Rebalancing is triggered by `pm.Rebalance(partitionCount)` after any topology change (node addition or removal).

### How Rebalance Works

```go
func (pm *PartitionMap) Rebalance(partitionCount uint32) error {
    // 1. Collect all ACTIVE nodes
    nodeList := [active nodes from pm.Nodes]

    // 2. Clear existing partition assignments
    pm.Partitions = make(map[uint32]*PartitionInfo)

    // 3. For each partition 0..partitionCount-1:
    for i := range partitions {
        partitionHash = FNV-1a(fmt.Sprintf("partition-%d", i))
        
        // Assign primary + (RF-1) replicas
        // Nodes selected via: (partitionHash + j) % len(nodeList)
        // Deduplication: skip node if already assigned to this partition
        
        pm.Partitions[i] = &PartitionInfo{
            PrimaryNode:  nodeList[primaryIdx].ID,
            ReplicaNodes: [replica node IDs],
            HashRange:    [rangeStart, rangeEnd),
        }
    }

    pm.Version++
}
```

The assignment formula `(partitionHash + j) % len(nodeList)` distributes partitions round-robin across nodes. With 256 partitions and 3 nodes, each node gets ~85 primary partitions plus ~170 replica assignments.

### After Rebalance: Data Migration

`Rebalance()` only updates routing metadata. No data moves during rebalance. Data migration is handled by `TransferAgent.MigrateToNewOwners()`:

```
MigrateToNewOwners()
    │
    ▼
ScanKeys() → all local keys
    │
    ▼
For each key:
    owner = pm.GetNodeForKey(key)  ← using updated ring
    if owner == localNodeID: skip
    else: queue for transfer to owner
    │
    ▼
Fan out to destination nodes in parallel:
    ├─ Batch 500 keys × ≤64 KB average ≈ 32 MB per HTTP POST /transfer/keys
    ├─ Destination receives batch → calls store.Put(key, value, ttl) for each
    └─ After confirmed delivery: store.Delete(key) locally
```

**Safety guarantee**: `Put` on destination happens before `Delete` on source. If the HTTP POST fails, keys are NOT deleted locally — the next `MigrateToNewOwners()` call retries.

**Concurrency**: Each destination node receives its batch in a separate goroutine. A cluster-wide migration is parallel across all `N-1` destination nodes simultaneously.

---

## Adding a Node

```
1. pm.AddNode(nodeID, address, port)
   ├─ Create Node{state=ACTIVE}
   ├─ Add 64 virtual nodes to ConsistentHashRing
   └─ pm.Version++

2. pm.Rebalance(256)
   ├─ Recompute partition assignments with new node included
   └─ ~1/N of partitions reassigned to new node

3. ta.MigrateToNewOwners()  [background goroutine]
   ├─ Scan all local keys
   ├─ Identify keys now owned by new node
   └─ Stream to new node in 500-key batches
        ├─ New node receives via POST /transfer/keys
        └─ Source deletes after confirmed delivery

4. Gossip propagates updated PartitionMap to all nodes
```

**Shortcut**: `pm.AddNodeAndRebalance(nodeID, addr, port, transferAddr, 256, ta)` combines steps 1–3 in one call.

The migration runs in the background. Reads/writes continue normally during migration — the ring immediately routes new requests to the new node, and in-flight data is transferred concurrently.

---

## Removing a Node (Graceful)

```
1. pm.UpdateNodeState(nodeID, NodeStateDraining)
   └─ Health checks and metrics see graceful departure, not sudden failure

2. pm.RemoveNode(nodeID)
   ├─ Delete from Nodes map
   └─ Remove 64 virtual nodes from ring

3. pm.Rebalance(256)
   └─ Reassign departed node's partitions to surviving nodes

4. ta.MigrateToNewOwners()  [departing node's own TransferAgent]
   └─ Evacuate all local keys to their new owners
      (ta.localNodeID MUST == nodeID — using another node's TA would scan wrong data)
```

**Important**: `ta` must be the **departing node's own TransferAgent**. The `RemoveNodeAndRebalance()` function enforces this:

```go
if ta != nil && ta.localNodeID != nodeID {
    return fmt.Errorf("ta.localNodeID %q != nodeID %q", ta.localNodeID, nodeID)
}
```

**Shortcut**: `pm.RemoveNodeAndRebalance(nodeID, 256, ta)` does all four steps.

---

## Force-Removing a Dead Node

For a node that crashed or is unreachable (cannot gracefully evacuate its data):

```
pm.ForceRemoveNode(nodeID, 256)
    ├─ Remove from ring immediately
    └─ Rebalance (surviving nodes take over partition ownership)
    (no data migration — surviving replicas are the source of truth)
```

After `ForceRemoveNode`, the Replication Engine's anti-entropy mechanism re-replicates the missing RF-1 copies to restore the desired replication factor on surviving nodes.

---

## Gossip Protocol

Cluster topology changes (node state, partition map version) propagate via **epidemic gossip**:

```
Every 1 second, each node:
    1. Build GossipMessage{senderID, partitionMapVersion, nodeStates}
    2. Select 3 random peer nodes (fanout=3)
    3. Send gossip async to each peer
```

A receiver that sees a higher `PartitionMapVer` than its own fetches the latest map. Within `O(log N)` gossip rounds, all nodes converge to the same view. This means topology changes propagate to a 100-node cluster in ~7 rounds (~7 seconds at 1s gossip interval).

---

## Partition Map Versioning

Every mutation to the partition map increments `pm.Version`. Nodes reject operations based on stale maps: if a node's `PartitionMap.Version` is lower than the cluster's current version, it fetches the latest before routing.

```go
// Version increments on:
pm.AddNode(...)      → Version++
pm.RemoveNode(...)   → Version++
pm.UpdateNodeState(...)  → Version++ (on actual state change)
pm.Rebalance(...)    → Version++
```

---

## Configuration Reference

| Parameter | Default | Effect |
|-----------|---------|--------|
| `ReplicationFactor` | 3 | Copies of each partition (1 primary + 2 replicas) |
| `VirtualNodesPerNode` | 64 | Virtual nodes per physical node on the ring |
| `PartitionCount` | 256 | Total hash partitions |
| `HeartbeatInterval` | 1 s | Gossip tick interval |
| `SuspectThreshold` | 3 s | Missed heartbeats before SUSPECT |
| `FailureThreshold` | 10 s | Missed heartbeats before FAILED |
| `transferBatchSize` | 500 keys | Keys per HTTP migration batch |
| `transferHTTPTimeout` | 60 s | Timeout per migration HTTP call |
