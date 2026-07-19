package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// NodeState represents the state of a cluster node
type NodeState int

const (
	NodeStateActive    NodeState = iota
	NodeStateSuspect
	NodeStateFailed
	NodeStateRecovering
	NodeStateDraining // graceful departure in progress; ring already updated, data evacuating
)

// Node represents a cluster node
type Node struct {
	ID            string
	Address       string
	Port          int
	GossipAddr    string       // "<host>:<port>" of the node's gossip listener; "" if unknown.
	Rack          string       // failure domain (rack / cloud zone); "" if unknown. See SetNodeRack.
	State         atomic.Value // NodeState
	LastHeartbeat int64        // Unix nanoseconds
	Version       uint64        // Partition map version
	RepairCount   atomic.Uint64
}

// PartitionReplica represents a single partition replica
type PartitionReplica struct {
	PartitionID   uint32
	NodeID        string
	Role          string // "primary" or "replica"
	SequenceNum   uint64
	Version       uint64
}

// PartitionMap maps hash ranges to nodes
type PartitionMap struct {
	mu              sync.RWMutex
	Version         uint64
	Timestamp       int64
	Nodes           map[string]*Node
	Partitions      map[uint32]*PartitionInfo
	Ring            *ConsistentHashRing
	ReplicationFactor int
	metrics         *ClusterMetrics

	// epoch is the split-brain fencing counter. Every membership-changing
	// operation (AddNode / RemoveNode and their fenced *WithEpoch variants)
	// advances it monotonically. Partition-map updates and partition-transfer
	// requests that carry a stale epoch are rejected with ErrStaleEpoch.
	// See epoch.go.
	epoch uint64

	// configuredPartitions is the partition count this map is operating with:
	// initialised from ClusterConfig.PartitionCount and updated by every
	// successful Rebalance. This is the authoritative count for automatic
	// rebalances (e.g. FailureDetector.triggerRebalance) — never a metric.
	configuredPartitions uint32

	// memberSubs receive a MembershipEvent on every node add/remove/state
	// change. Sends are non-blocking (events drop when a subscriber lags);
	// consumers treat an event as "topology changed, re-examine" rather than a
	// lossless log.
	memberSubs []chan MembershipEvent
}

// MembershipEvent describes one cluster membership or node-state change.
type MembershipEvent struct {
	NodeID   string
	Type     string // "added" | "removed" | "state"
	NewState NodeState
}

// SubscribeMembership registers and returns a buffered channel that receives
// membership events. There is no unsubscribe — subscribers live for the
// process lifetime (the server's rebalancer).
func (pm *PartitionMap) SubscribeMembership() <-chan MembershipEvent {
	ch := make(chan MembershipEvent, 64)
	pm.mu.Lock()
	pm.memberSubs = append(pm.memberSubs, ch)
	pm.mu.Unlock()
	return ch
}

// notifyMembershipLocked fans an event out to subscribers without blocking.
// Caller must hold pm.mu.
func (pm *PartitionMap) notifyMembershipLocked(ev MembershipEvent) {
	for _, ch := range pm.memberSubs {
		select {
		case ch <- ev:
		default:
		}
	}
}

// PartitionInfo stores information about a partition
type PartitionInfo struct {
	PartitionID   uint32
	HashRange     [2]uint64 // [start, end)
	PrimaryNode   string
	ReplicaNodes  []string
	Version       uint64
	LastModified  int64
}

// ConsistentHashRing implements consistent hashing with virtual nodes
type ConsistentHashRing struct {
	mu           sync.RWMutex
	virtualNodes map[uint64]string // hash → node_id
	sortedKeys   []uint64 // Sorted hash values for binary search
	replicas     int      // Number of virtual nodes per physical node
}

// ClusterMetrics tracks cluster-level metrics
type ClusterMetrics struct {
	NodeAdditions     atomic.Uint64
	NodeRemovals      atomic.Uint64
	PartitionMigrations atomic.Uint64
	FailureDetections atomic.Uint64
	Rebalances        atomic.Uint64
	MetaUpdateTime    atomic.Int64 // Latest update timestamp
}

// ClusterConfig contains cluster configuration
type ClusterConfig struct {
	ReplicationFactor   int
	VirtualNodesPerNode int
	HeartbeatInterval   time.Duration
	HeartbeatTimeout    time.Duration
	SuspectThreshold    time.Duration
	PartitionCount      uint32
	MaxMetadataSize     uint64
}

// DefaultClusterConfig returns sensible defaults
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		ReplicationFactor:   3,
		VirtualNodesPerNode: 64,
		HeartbeatInterval:   1 * time.Second,
		HeartbeatTimeout:    3 * time.Second,
		SuspectThreshold:    3 * time.Second,
		PartitionCount:      256,
		MaxMetadataSize:     1024 * 1024, // 1MB
	}
}

// NewPartitionMap creates a new partition map
func NewPartitionMap(config *ClusterConfig) *PartitionMap {
	return &PartitionMap{
		Version:              0,
		Timestamp:            time.Now().UnixNano(),
		Nodes:                make(map[string]*Node),
		Partitions:           make(map[uint32]*PartitionInfo),
		Ring:                 NewConsistentHashRing(config.VirtualNodesPerNode),
		ReplicationFactor:    config.ReplicationFactor,
		metrics:              &ClusterMetrics{},
		configuredPartitions: config.PartitionCount,
	}
}

// AddNode adds a new node to the cluster
func (pm *PartitionMap) AddNode(nodeID, address string, port int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.addNodeLocked(nodeID, address, port)
}

// addNodeLocked is the body of AddNode; caller must hold pm.mu.
func (pm *PartitionMap) addNodeLocked(nodeID, address string, port int) error {
	if _, exists := pm.Nodes[nodeID]; exists {
		return fmt.Errorf("node %s already exists", nodeID)
	}

	node := &Node{
		ID:            nodeID,
		Address:       address,
		Port:          port,
		LastHeartbeat: time.Now().UnixNano(),
		Version:       pm.Version,
	}
	node.State.Store(NodeStateActive)

	pm.Nodes[nodeID] = node
	pm.Ring.AddNode(nodeID)

	pm.Version++
	pm.epoch++ // membership change → advance the fencing epoch (see epoch.go)
	pm.Timestamp = time.Now().UnixNano()
	pm.metrics.NodeAdditions.Add(1)
	pm.notifyMembershipLocked(MembershipEvent{NodeID: nodeID, Type: "added", NewState: NodeStateActive})

	return nil
}

// RemoveNode removes a node from the cluster
func (pm *PartitionMap) RemoveNode(nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.removeNodeLocked(nodeID)
}

// removeNodeLocked is the body of RemoveNode; caller must hold pm.mu.
func (pm *PartitionMap) removeNodeLocked(nodeID string) error {
	if _, exists := pm.Nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	delete(pm.Nodes, nodeID)
	pm.Ring.RemoveNode(nodeID)

	pm.Version++
	pm.epoch++ // membership change → advance the fencing epoch (see epoch.go)
	pm.Timestamp = time.Now().UnixNano()
	pm.metrics.NodeRemovals.Add(1)
	pm.notifyMembershipLocked(MembershipEvent{NodeID: nodeID, Type: "removed"})

	return nil
}

// SetNodeGossipAddr registers the "<host>:<port>" of nodeID's gossip listener.
// Gossip digests also propagate this address, so explicit registration is only
// required for bootstrap peers (or tests) whose address is known out-of-band.
func (pm *PartitionMap) SetNodeGossipAddr(nodeID, gossipAddr string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	node, exists := pm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	node.GossipAddr = gossipAddr
	return nil
}

// GetNodeGossipAddr returns the gossip listener address of nodeID, or "".
func (pm *PartitionMap) GetNodeGossipAddr(nodeID string) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if node, exists := pm.Nodes[nodeID]; exists {
		return node.GossipAddr
	}
	return ""
}

// SetNodeRack records the failure domain (rack / cloud zone) of nodeID.
// Replica placement (Rebalance, GetReplicasForKey) spreads copies across
// distinct racks whenever the topology allows it. Racks propagate between
// members via gossip digests, so explicit registration is only required for
// the local node (--rack-id) or in tests. Two nodes with rack "" count as
// the SAME (unknown) rack, so mixed racked/unracked clusters still prefer
// a racked node for the second copy.
func (pm *PartitionMap) SetNodeRack(nodeID, rack string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	node, exists := pm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	node.Rack = rack
	return nil
}

// GetNodeRack returns the recorded rack of nodeID, or "".
func (pm *PartitionMap) GetNodeRack(nodeID string) string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if node, exists := pm.Nodes[nodeID]; exists {
		return node.Rack
	}
	return ""
}

// GetNodeForKey returns the primary node for a given key
func (pm *PartitionMap) GetNodeForKey(key string) (string, error) {
	hash := hashKey(key)
	return pm.Ring.GetNode(hash)
}

// GetReplicasForKey returns the replica set for a key: the ring successor as
// primary, then backups walking the ring — rack-aware, preferring successors
// whose rack differs from every copy already chosen (falling back to plain
// ring order when RF exceeds the distinct racks). Also deduplicates the
// primary, which the pre-1.1 version returned twice.
func (pm *PartitionMap) GetReplicasForKey(key string) ([]string, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	hash := hashKey(key)
	// Ring successors in order, one entry per distinct physical node.
	order, err := pm.Ring.GetNodeWithReplicas(hash, len(pm.Nodes))
	if err != nil {
		return nil, err
	}
	// Materialize as *Node (skipping members no longer in the map) and pick
	// with the same rack-aware routine Rebalance uses.
	nodeList := make([]*Node, 0, len(order))
	for _, id := range order {
		if n, ok := pm.Nodes[id]; ok {
			nodeList = append(nodeList, n)
		}
	}
	if len(nodeList) == 0 {
		return nil, fmt.Errorf("no known nodes on ring")
	}
	return pickReplicas(nodeList, 0, pm.ReplicationFactor), nil
}

// UpdateNodeState updates the state of a node
func (pm *PartitionMap) UpdateNodeState(nodeID string, newState NodeState) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	node, exists := pm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	
	oldState := node.State.Load().(NodeState)
	node.State.Store(newState)
	
	if oldState != newState {
		pm.Version++
		pm.Timestamp = time.Now().UnixNano()
		if newState == NodeStateFailed {
			pm.metrics.FailureDetections.Add(1)
		}
		pm.notifyMembershipLocked(MembershipEvent{NodeID: nodeID, Type: "state", NewState: newState})
	}

	return nil
}

// UpdateNodeHeartbeat records a heartbeat from a node
func (pm *PartitionMap) UpdateNodeHeartbeat(nodeID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	node, exists := pm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	
	node.LastHeartbeat = time.Now().UnixNano()
	
	// Recover from suspect state if was suspect
	state := node.State.Load().(NodeState)
	if state == NodeStateSuspect {
		node.State.Store(NodeStateActive)
		pm.Version++
		pm.Timestamp = time.Now().UnixNano()
	}
	
	return nil
}

// GetPartition returns a partition by ID
func (pm *PartitionMap) GetPartition(partitionID uint32) (*PartitionInfo, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	partition, exists := pm.Partitions[partitionID]
	if !exists {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}
	
	return partition, nil
}

// Rebalance redistributes partitions across nodes
func (pm *PartitionMap) Rebalance(partitionCount uint32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	
	if len(pm.Nodes) == 0 {
		return fmt.Errorf("no nodes in cluster")
	}
	
	// Distribute partitions uniformly.  The node list is sorted by ID so the
	// assignment is deterministic: every cluster member computing Rebalance
	// independently derives the SAME partition table (Go map iteration order
	// is randomized, so iterating pm.Nodes directly would not be).
	nodeList := make([]*Node, 0, len(pm.Nodes))
	for _, node := range pm.Nodes {
		if node.State.Load().(NodeState) == NodeStateActive {
			nodeList = append(nodeList, node)
		}
	}
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].ID < nodeList[j].ID })

	if len(nodeList) == 0 {
		return fmt.Errorf("no active nodes in cluster")
	}

	// Clear existing partitions
	pm.Partitions = make(map[uint32]*PartitionInfo)

	// Assign partitions round-robin: perfectly balanced (max skew 1) and
	// deterministic, unlike the previous hash-mod placement whose skew for
	// small partition counts could reach 5:1. Backup replicas are chosen
	// rack-aware: a copy never shares a failure domain with an earlier copy
	// unless every remaining node would (RF > distinct racks).
	for i := uint32(0); i < partitionCount; i++ {
		start := int(uint64(i) % uint64(len(nodeList)))
		replicas := pickReplicas(nodeList, start, pm.ReplicationFactor)
		
		rangeStart := (uint64(i) * (^uint64(0) / uint64(partitionCount)))
		rangeEnd := ((uint64(i) + 1) * (^uint64(0) / uint64(partitionCount)))
		
		pm.Partitions[i] = &PartitionInfo{
			PartitionID:  i,
			HashRange:    [2]uint64{rangeStart, rangeEnd},
			PrimaryNode:  replicas[0],
			ReplicaNodes: replicas[1:],
			Version:      pm.Version,
			LastModified: time.Now().UnixNano(),
		}
	}
	
	pm.Version++
	pm.Timestamp = time.Now().UnixNano()
	pm.metrics.Rebalances.Add(1)
	pm.configuredPartitions = partitionCount // remember for automatic rebalances

	return nil
}

// pickReplicas selects up to rf distinct node IDs from nodeList, walking
// forward from start (wrapping). Two passes keep it rack-aware yet total:
// pass 1 only accepts a node whose rack differs from every rack already
// used; pass 2 fills any remaining slots ignoring racks. With no racks
// configured every node shares the "" rack, so pass 1 stops after the
// first pick and pass 2 reproduces the historical plain round-robin —
// racked and rackless clusters both get deterministic placement.
func pickReplicas(nodeList []*Node, start, rf int) []string {
	n := len(nodeList)
	replicas := make([]string, 0, rf)
	seen := make(map[string]bool, rf)
	usedRacks := make(map[string]bool, rf)

	for pass := 0; pass < 2 && len(replicas) < rf; pass++ {
		for off := 0; off < n && len(replicas) < rf; off++ {
			cand := nodeList[(start+off)%n]
			if seen[cand.ID] {
				continue
			}
			if pass == 0 && len(replicas) > 0 && usedRacks[cand.Rack] {
				continue
			}
			replicas = append(replicas, cand.ID)
			seen[cand.ID] = true
			usedRacks[cand.Rack] = true
		}
	}
	return replicas
}

// PartitionCount returns the partition count this map is configured with —
// initialised from ClusterConfig.PartitionCount and updated by every
// successful Rebalance. This (never a metrics counter) is the value to pass
// to Rebalance when re-triggering automatically.
func (pm *PartitionMap) PartitionCount() uint32 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.configuredPartitions
}

// GetNodeStats returns statistics for all nodes
func (pm *PartitionMap) GetNodeStats() map[string]NodeStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	stats := make(map[string]NodeStats)
	
	for nodeID, node := range pm.Nodes {
		state := node.State.Load().(NodeState)
		heartbeatAge := time.Now().UnixNano() - node.LastHeartbeat
		
		stats[nodeID] = NodeStats{
			NodeID:        nodeID,
			Address:       node.Address,
			Port:          node.Port,
			Rack:          node.Rack,
			State:         state.String(),
			HeartbeatAgeNs: heartbeatAge,
			Version:       node.Version,
			RepairCount:   node.RepairCount.Load(),
		}
	}
	
	return stats
}

// String representation of NodeState
func (ns NodeState) String() string {
	switch ns {
	case NodeStateActive:
		return "ACTIVE"
	case NodeStateSuspect:
		return "SUSPECT"
	case NodeStateFailed:
		return "FAILED"
	case NodeStateRecovering:
		return "RECOVERING"
	case NodeStateDraining:
		return "DRAINING"
	default:
		return "UNKNOWN"
	}
}

// NodeStats represents statistics for a node
type NodeStats struct {
	NodeID        string
	Address       string
	Port          int
	Rack          string
	State         string
	HeartbeatAgeNs int64
	Version       uint64
	RepairCount   uint64
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodesPerNode int) *ConsistentHashRing {
	return &ConsistentHashRing{
		virtualNodes: make(map[uint64]string),
		sortedKeys:   make([]uint64, 0),
		replicas:     virtualNodesPerNode,
	}
}

// AddNode adds a node to the ring
func (chr *ConsistentHashRing) AddNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()
	
	for i := 0; i < chr.replicas; i++ {
		hash := hashValue(fmt.Sprintf("%s:%d", nodeID, i))
		chr.virtualNodes[hash] = nodeID
		chr.sortedKeys = append(chr.sortedKeys, hash)
	}
	
	sort.Slice(chr.sortedKeys, func(i, j int) bool {
		return chr.sortedKeys[i] < chr.sortedKeys[j]
	})
}

// RemoveNode removes a node from the ring
func (chr *ConsistentHashRing) RemoveNode(nodeID string) {
	chr.mu.Lock()
	defer chr.mu.Unlock()
	
	keysToRemove := make([]uint64, 0)
	for hash, id := range chr.virtualNodes {
		if id == nodeID {
			keysToRemove = append(keysToRemove, hash)
		}
	}
	
	for _, hash := range keysToRemove {
		delete(chr.virtualNodes, hash)
	}
	
	// Rebuild sorted keys
	chr.sortedKeys = make([]uint64, 0, len(chr.virtualNodes))
	for hash := range chr.virtualNodes {
		chr.sortedKeys = append(chr.sortedKeys, hash)
	}
	sort.Slice(chr.sortedKeys, func(i, j int) bool {
		return chr.sortedKeys[i] < chr.sortedKeys[j]
	})
}

// GetNode returns the node responsible for a hash
func (chr *ConsistentHashRing) GetNode(hash uint64) (string, error) {
	chr.mu.RLock()
	defer chr.mu.RUnlock()
	
	if len(chr.sortedKeys) == 0 {
		return "", fmt.Errorf("ring is empty")
	}
	
	// Binary search for the first key >= hash
	idx := sort.Search(len(chr.sortedKeys), func(i int) bool {
		return chr.sortedKeys[i] >= hash
	})
	
	if idx == len(chr.sortedKeys) {
		idx = 0 // Wrap around
	}
	
	return chr.virtualNodes[chr.sortedKeys[idx]], nil
}

// GetNodeWithReplicas returns N replica nodes for a hash
func (chr *ConsistentHashRing) GetNodeWithReplicas(hash uint64, count int) ([]string, error) {
	chr.mu.RLock()
	defer chr.mu.RUnlock()
	
	if len(chr.sortedKeys) == 0 {
		return nil, fmt.Errorf("ring is empty")
	}
	
	replicas := make([]string, 0, count)
	seen := make(map[string]bool)
	
	idx := sort.Search(len(chr.sortedKeys), func(i int) bool {
		return chr.sortedKeys[i] >= hash
	})
	
	if idx == len(chr.sortedKeys) {
		idx = 0
	}
	
	for i := 0; i < len(chr.sortedKeys) && len(replicas) < count; i++ {
		nodeID := chr.virtualNodes[chr.sortedKeys[(idx+i)%len(chr.sortedKeys)]]
		if !seen[nodeID] {
			replicas = append(replicas, nodeID)
			seen[nodeID] = true
		}
	}
	
	return replicas, nil
}

// Hash functions

// hashKey computes hash of a key
func hashKey(key string) uint64 {
	return hashValue(key)
}

// HashKey is the exported key-hash used by GetNodeForKey / the consistent-hash
// ring.  A cluster-aware client that builds its own ConsistentHashRing (with the
// same VirtualNodesPerNode) and routes with ring.GetNode(cluster.HashKey(key))
// reaches the identical owner node the server would pick — enabling client-side
// routing that matches the server exactly.
func HashKey(key string) uint64 {
	return hashValue(key)
}

// hashValue computes FNV-1a and passes it through a 64-bit finalizer.
//
// Raw FNV-1a has weak avalanche for inputs that differ only in a short
// suffix: sequential keys ("user:0001".."user:0999") land within a few
// multiples of the FNV prime of each other — far closer together than a
// vnode arc (2^64/128) — so entire sequential runs collapse onto ONE ring
// arc and one node. The fmix64 finalizer (MurmurHash3 §finalization) makes
// every input bit affect every output bit, restoring uniform placement.
// Changing this constant re-shards the ring; server and cluster client both
// route via this function so they stay mutually consistent.
func hashValue(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return fmix64(h.Sum64())
}

// fmix64 is the MurmurHash3 64-bit finalizer.
func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// GetMetrics returns the cluster metrics for external observation.
func (pm *PartitionMap) GetMetrics() *ClusterMetrics { return pm.metrics }

// GetVersion returns the current partition map version.
func (pm *PartitionMap) GetVersion() uint64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.Version
}

// GetNodeCount returns the number of nodes currently in the cluster.
func (pm *PartitionMap) GetNodeCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.Nodes)
}
