package storage

import (
	"fmt"
	"testing"
)

// TestTxn_BasicCommit: Begin txn; Set 3 keys; Commit; verify all 3 readable.
func TestTxn_BasicCommit(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	txn := se.BeginTxn()
	txn.Set("txn-key1", []byte("v1"), -1)
	txn.Set("txn-key2", []byte("v2"), -1)
	txn.Set("txn-key3", []byte("v3"), -1)

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cases := []struct {
		key  string
		want string
	}{
		{"txn-key1", "v1"},
		{"txn-key2", "v2"},
		{"txn-key3", "v3"},
	}
	for _, c := range cases {
		got, err := se.Get(c.key)
		if err != nil {
			t.Fatalf("Get(%s): %v", c.key, err)
		}
		if string(got) != c.want {
			t.Fatalf("key %s: expected %q, got %q", c.key, c.want, string(got))
		}
	}
}

// TestTxn_Rollback: Begin txn; Set keys but DON'T commit; keys not visible.
func TestTxn_Rollback(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	txn := se.BeginTxn()
	txn.Set("txn-unset-key", []byte("value"), -1)
	txn.Abort()

	_, err := se.Get("txn-unset-key")
	if err == nil {
		t.Fatal("expected key to be absent after Abort, but Get succeeded")
	}
}

// TestTxn_ConflictDetection: Begin txn; SetIf with captured version;
// concurrent Put changes the key; Commit returns ErrTxnConflict.
func TestTxn_ConflictDetection(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	key := "txn-conflict-key"
	// Write the initial value so there is a version to capture.
	if err := se.Put(key, []byte("v1"), -1); err != nil {
		t.Fatalf("Put initial: %v", err)
	}

	// Capture the current WriteTimestampUs as the expected version.
	entry, _, ok := se.index.get(key)
	if !ok {
		t.Fatal("index entry not found after Put")
	}
	capturedVersion := uint64(entry.WriteTimestampUs)

	// Build a transaction that guards on the captured version.
	txn := se.BeginTxn()
	txn.SetIf(key, []byte("v2-from-txn"), -1, capturedVersion)

	// Now concurrently update the key so the version advances.
	if err := se.Put(key, []byte("v1-updated-concurrently"), -1); err != nil {
		t.Fatalf("concurrent Put: %v", err)
	}

	// Commit should fail with ErrTxnConflict because WriteTimestampUs changed.
	err := txn.Commit()
	if err != ErrTxnConflict {
		t.Fatalf("expected ErrTxnConflict, got: %v", err)
	}
}

// TestTxn_Delete: Txn with Set + Delete; after Commit only Set key exists.
func TestTxn_Delete(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	// Pre-create a key so Delete in the txn has something to remove.
	if err := se.Put("txn-del-pre", []byte("exists"), -1); err != nil {
		t.Fatalf("Put pre-existing key: %v", err)
	}

	txn := se.BeginTxn()
	txn.Set("txn-del-set", []byte("created"), -1)
	txn.Delete("txn-del-pre")

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// "txn-del-set" should exist.
	if got, err := se.Get("txn-del-set"); err != nil || string(got) != "created" {
		t.Fatalf("expected txn-del-set=created, got %q err %v", string(got), err)
	}
	// "txn-del-pre" should be gone.
	if _, err := se.Get("txn-del-pre"); err == nil {
		t.Fatal("expected txn-del-pre to be deleted, but Get succeeded")
	}
}

// TestTxn_MultipleCommits: Same txn object can only be committed once.
func TestTxn_MultipleCommits(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	txn := se.BeginTxn()
	txn.Set("txn-once-key", []byte("once"), -1)

	if err := txn.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	// Second Commit should return an error.
	if err := txn.Commit(); err == nil {
		t.Fatal("expected error on second Commit, got nil")
	}
}

// TestTxn_ReadCommitted: Inside txn, Read sees committed state (not txn buffer).
func TestTxn_ReadCommitted(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	// Write a committed value.
	if err := se.Put("txn-read-key", []byte("committed"), -1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	txn := se.BeginTxn()
	// Stage a new value (not yet committed).
	txn.Set("txn-read-key", []byte("staged"), -1)

	// txn.Read should see the committed value, not the staged one.
	val, err := txn.Read("txn-read-key")
	if err != nil {
		t.Fatalf("txn.Read: %v", err)
	}
	if string(val) != "committed" {
		t.Fatalf("expected read-committed value %q, got %q", "committed", string(val))
	}
	txn.Abort()
}

// TestTxn_LargeBatch: Txn with 500 key-value pairs; Commit succeeds; all readable.
func TestTxn_LargeBatch(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	txn := se.BeginTxn()
	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("txn-large-%04d", i)
		val := fmt.Sprintf("value-%d", i)
		txn.Set(key, []byte(val), -1)
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit large batch: %v", err)
	}

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("txn-large-%04d", i)
		wantVal := fmt.Sprintf("value-%d", i)
		got, err := se.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if string(got) != wantVal {
			t.Fatalf("key %s: expected %q, got %q", key, wantVal, string(got))
		}
	}
}
