package cluster

import (
	"errors"
	"testing"
)

// TestEpoch_AdvancesOnMembershipChange: every AddNode / RemoveNode advances
// the fencing epoch monotonically.
func TestEpoch_AdvancesOnMembershipChange(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)

	e0 := pm.Epoch()
	if err := pm.AddNode("ep-1", "127.0.0.1", 9000); err != nil {
		t.Fatal(err)
	}
	e1 := pm.Epoch()
	if e1 <= e0 {
		t.Errorf("epoch did not advance on AddNode: %d → %d", e0, e1)
	}

	if err := pm.RemoveNode("ep-1"); err != nil {
		t.Fatal(err)
	}
	e2 := pm.Epoch()
	if e2 <= e1 {
		t.Errorf("epoch did not advance on RemoveNode: %d → %d", e1, e2)
	}

	// Failed membership ops must NOT advance the epoch.
	if err := pm.RemoveNode("ghost"); err == nil {
		t.Fatal("expected error removing unknown node")
	}
	if pm.Epoch() != e2 {
		t.Errorf("epoch advanced on failed RemoveNode: %d → %d", e2, pm.Epoch())
	}
}

// TestEpoch_FencedOpsRejectStale: the *WithEpoch membership variants reject
// operations carrying an epoch older than the current one with ErrStaleEpoch.
func TestEpoch_FencedOpsRejectStale(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	cfg.ReplicationFactor = 1
	pm := NewPartitionMap(cfg)
	if err := pm.AddNode("fence-1", "127.0.0.1", 9000); err != nil {
		t.Fatal(err)
	}

	observed := pm.Epoch()
	// A competing membership change advances the epoch past `observed`.
	if err := pm.AddNode("fence-2", "127.0.0.1", 9001); err != nil {
		t.Fatal(err)
	}

	err := pm.AddNodeWithEpoch("fence-late", "127.0.0.1", 9002, observed)
	if !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("AddNodeWithEpoch(stale) = %v; want ErrStaleEpoch", err)
	}
	if _, ok := pm.Nodes["fence-late"]; ok {
		t.Error("fenced-out AddNode still mutated the map")
	}

	err = pm.RemoveNodeWithEpoch("fence-1", observed)
	if !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("RemoveNodeWithEpoch(stale) = %v; want ErrStaleEpoch", err)
	}
	if _, ok := pm.Nodes["fence-1"]; !ok {
		t.Error("fenced-out RemoveNode still mutated the map")
	}

	// With the current epoch both operations succeed.
	if err := pm.AddNodeWithEpoch("fence-late", "127.0.0.1", 9002, pm.Epoch()); err != nil {
		t.Errorf("AddNodeWithEpoch(current) = %v; want nil", err)
	}
	if err := pm.RemoveNodeWithEpoch("fence-1", pm.Epoch()); err != nil {
		t.Errorf("RemoveNodeWithEpoch(current) = %v; want nil", err)
	}
}

// TestEpoch_AdvanceToMonotonic: AdvanceEpochTo raises the epoch to a newer
// remote value but never retreats.
func TestEpoch_AdvanceToMonotonic(t *testing.T) {
	t.Parallel()

	cfg := DefaultClusterConfig()
	pm := NewPartitionMap(cfg)

	pm.AdvanceEpochTo(10)
	if e := pm.Epoch(); e != 10 {
		t.Fatalf("Epoch() = %d after AdvanceEpochTo(10); want 10", e)
	}
	pm.AdvanceEpochTo(4) // lower — must be a no-op
	if e := pm.Epoch(); e != 10 {
		t.Errorf("epoch retreated to %d; want 10", e)
	}
	if err := pm.ValidateEpoch(9); !errors.Is(err, ErrStaleEpoch) {
		t.Errorf("ValidateEpoch(9) = %v; want ErrStaleEpoch", err)
	}
	if err := pm.ValidateEpoch(10); err != nil {
		t.Errorf("ValidateEpoch(10) = %v; want nil", err)
	}
	if err := pm.ValidateEpoch(11); err != nil {
		t.Errorf("ValidateEpoch(11 — newer) = %v; want nil", err)
	}
}
