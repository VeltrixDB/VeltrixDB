package storage

import (
	"fmt"
	"testing"
)

// TestAntiEntropy_ReconcilesMissedWrites is the regression test for the
// delivery gap: a replica that MISSED writes entirely (not just reordered)
// must be brought back into convergence by anti-entropy. The live LWW path is
// deliberately NOT used here — writes are applied only to one engine, modelling
// a replica that was partitioned/crashed while the other took writes.
func TestAntiEntropy_ReconcilesMissedWrites(t *testing.T) {
	a := newTestEngine(t)
	b := newTestEngine(t)

	// A took 300 writes that B never saw (B was "partitioned").
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("only-on-a-%04d", i)
		if _, err := a.ApplyLWWPut(key, []byte(fmt.Sprintf("v-%d", i)), -1, int64(1000+i)); err != nil {
			t.Fatalf("A put: %v", err)
		}
	}
	// B independently took some writes A never saw.
	for i := 0; i < 120; i++ {
		key := fmt.Sprintf("only-on-b-%04d", i)
		if _, err := b.ApplyLWWPut(key, []byte(fmt.Sprintf("w-%d", i)), -1, int64(2000+i)); err != nil {
			t.Fatalf("B put: %v", err)
		}
	}
	// And a set of shared keys where each has a different, out-of-sync value:
	// A holds the NEWER version for evens, B holds the newer for odds.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("shared-%04d", i)
		if i%2 == 0 {
			a.ApplyLWWPut(key, []byte("A-newer"), -1, int64(9000+i)) // higher clock on A
			b.ApplyLWWPut(key, []byte("B-older"), -1, int64(3000+i))
		} else {
			a.ApplyLWWPut(key, []byte("A-older"), -1, int64(3000+i))
			b.ApplyLWWPut(key, []byte("B-newer"), -1, int64(9000+i)) // higher clock on B
		}
	}

	// Heal: reconcile in both directions (each node pulls the other's divergent
	// shards). One round each suffices because pull is LWW-idempotent.
	if _, err := a.Reconcile(EnginePeer{E: b}); err != nil {
		t.Fatalf("A.Reconcile: %v", err)
	}
	if _, err := b.Reconcile(EnginePeer{E: a}); err != nil {
		t.Fatalf("B.Reconcile: %v", err)
	}

	// Both replicas must now agree on every key, and on the correct LWW winner.
	check := func(key, want string) {
		t.Helper()
		va, ea := a.Get(key)
		vb, eb := b.Get(key)
		if (ea != nil) != (eb != nil) {
			t.Fatalf("key %s DIVERGED: A present=%v B present=%v", key, ea == nil, eb == nil)
		}
		if ea == nil && string(va) != string(vb) {
			t.Fatalf("key %s DIVERGED: A=%q B=%q", key, va, vb)
		}
		if ea == nil && string(va) != want {
			t.Errorf("key %s: converged to %q, want %q", key, va, want)
		}
		if ea != nil {
			t.Errorf("key %s missing after reconcile", key)
		}
	}

	for i := 0; i < 300; i++ {
		check(fmt.Sprintf("only-on-a-%04d", i), fmt.Sprintf("v-%d", i))
	}
	for i := 0; i < 120; i++ {
		check(fmt.Sprintf("only-on-b-%04d", i), fmt.Sprintf("w-%d", i))
	}
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			check(fmt.Sprintf("shared-%04d", i), "A-newer")
		} else {
			check(fmt.Sprintf("shared-%04d", i), "B-newer")
		}
	}
	t.Logf("anti-entropy reconciled 300 A-only + 120 B-only + 100 conflicting keys to convergence")
}

// TestAntiEntropy_TombstoneReconciles verifies a delete that a replica never
// saw is propagated by anti-entropy (the key ends up absent on both), and that
// a newer resurrection on the peer wins.
func TestAntiEntropy_TombstoneReconciles(t *testing.T) {
	a := newTestEngine(t)
	b := newTestEngine(t)

	// Both start with the same key; then A deletes it (B misses the delete).
	a.ApplyLWWPut("k1", []byte("v1"), -1, 100)
	b.ApplyLWWPut("k1", []byte("v1"), -1, 100)
	a.ApplyLWWDelete("k1", 200) // newer delete only on A

	// A separate key that B resurrects with a newer put after A tombstoned it.
	a.ApplyLWWPut("k2", []byte("v2"), -1, 100)
	a.ApplyLWWDelete("k2", 150)                    // A: deleted at 150
	b.ApplyLWWPut("k2", []byte("v2-new"), -1, 300) // B: resurrected at 300 (newer)

	b.Reconcile(EnginePeer{E: a})
	a.Reconcile(EnginePeer{E: b})

	// k1: newer delete (ts=200) wins → absent on both.
	if _, err := a.Get("k1"); err == nil {
		t.Errorf("k1 should be deleted on A")
	}
	if _, err := b.Get("k1"); err == nil {
		t.Errorf("k1 should be deleted on B after reconcile (missed delete propagated)")
	}
	// k2: newer resurrection (ts=300) wins → present with the new value on both.
	for _, e := range []*StorageEngine{a, b} {
		v, err := e.Get("k2")
		if err != nil || string(v) != "v2-new" {
			t.Errorf("k2 want v2-new, got %q err=%v", string(v), err)
		}
	}
}

// TestAntiEntropy_DigestDetectsDivergence sanity-checks the digest primitive:
// identical shards hash equal; any missing/older/extra/tombstoned entry changes
// the digest.
func TestAntiEntropy_DigestDetectsDivergence(t *testing.T) {
	a := newTestEngine(t)
	b := newTestEngine(t)

	same := func() bool {
		da, _ := a.ShardDigests()
		db, _ := b.ShardDigests()
		if len(da) != len(db) {
			return false
		}
		for s, d := range da {
			if db[s] != d {
				return false
			}
		}
		return true
	}

	a.ApplyLWWPut("x", []byte("1"), -1, 10)
	b.ApplyLWWPut("x", []byte("1"), -1, 10)
	if !same() {
		t.Fatal("identical state should produce identical digests")
	}

	b.ApplyLWWPut("x", []byte("2"), -1, 20) // newer clock on B
	if same() {
		t.Fatal("a newer version must change the shard digest")
	}

	// Reconcile A from B, digests must match again.
	a.Reconcile(EnginePeer{E: b})
	if !same() {
		t.Fatal("digests must match after reconcile")
	}
}
