package cluster

// epoch.go — split-brain fencing primitive.
//
// The partition map carries a monotonically-increasing epoch that advances on
// every membership change (AddNode / RemoveNode, and their fenced variants
// below). Any operation performed on behalf of a remote coordinator carries
// the epoch that coordinator observed; if it is older than the local epoch the
// operation is rejected with ErrStaleEpoch.
//
// This fences off a coordinator stranded on the losing side of a network
// partition: after the partition heals, its queued membership changes and
// partition-transfer batches carry a stale epoch and are refused instead of
// clobbering the newer topology.
//
// Enforcement points:
//   - PartitionMap.AddNodeWithEpoch / RemoveNodeWithEpoch — fenced membership ops
//   - TransferAgent.handleReceive — rejects KeyBatch.Epoch < local epoch (HTTP 409)
//   - GossipProtocol.mergeDigest — digests with a stale epoch still count as
//     liveness evidence but are not allowed to update membership metadata;
//     digests with a newer epoch advance the local epoch via AdvanceEpochTo.

import "errors"

// ErrStaleEpoch is returned when an operation carries a cluster epoch older
// than the local partition map's epoch.
var ErrStaleEpoch = errors.New("cluster: stale epoch")

// Epoch returns the current fencing epoch of the partition map.
func (pm *PartitionMap) Epoch() uint64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.epoch
}

// ValidateEpoch returns ErrStaleEpoch if observed is older than the current
// epoch. Equal epochs are accepted; newer epochs are accepted too — a newer
// epoch means the caller has seen a membership change this node hasn't yet.
func (pm *PartitionMap) ValidateEpoch(observed uint64) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if observed < pm.epoch {
		return ErrStaleEpoch
	}
	return nil
}

// AdvanceEpochTo raises the local epoch to at least e. Monotonic: a lower e
// is a no-op, so the epoch never retreats. Used when gossip reveals a peer
// with a newer epoch.
func (pm *PartitionMap) AdvanceEpochTo(e uint64) {
	pm.mu.Lock()
	if e > pm.epoch {
		pm.epoch = e
	}
	pm.mu.Unlock()
}

// AddNodeWithEpoch is the fenced variant of AddNode: the caller passes the
// epoch it observed when it decided to add the node. If the local epoch has
// advanced past it (another membership change won the race), the operation is
// rejected with ErrStaleEpoch. The check and the mutation are atomic under
// pm.mu.
func (pm *PartitionMap) AddNodeWithEpoch(nodeID, address string, port int, observedEpoch uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if observedEpoch < pm.epoch {
		return ErrStaleEpoch
	}
	return pm.addNodeLocked(nodeID, address, port)
}

// RemoveNodeWithEpoch is the fenced variant of RemoveNode. See
// AddNodeWithEpoch for semantics.
func (pm *PartitionMap) RemoveNodeWithEpoch(nodeID string, observedEpoch uint64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if observedEpoch < pm.epoch {
		return ErrStaleEpoch
	}
	return pm.removeNodeLocked(nodeID)
}
