package storage

import (
	"errors"
	"testing"
)

// TestQuota_RateLimit: Register namespace with low writes/s; send rapid writes;
// some return ErrRateLimited.
func TestQuota_RateLimit(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	ns := "rl-ns"
	// Allow only 5 writes/s with a burst of 5 so the bucket drains quickly.
	se.SetNamespaceLimit(ns, QuotaLimit{WritesPerSec: 5, BurstWrites: 5, MaxKeys: 0})

	var rateLimited int
	for i := 0; i < 100; i++ {
		err := se.PutNS(ns, "key", []byte("v"), -1)
		if errors.Is(err, ErrRateLimited) {
			rateLimited++
		}
	}
	if rateLimited == 0 {
		t.Fatal("expected at least one ErrRateLimited, got none")
	}
}

// TestQuota_MaxKeys: Register namespace with MaxKeys=5; write 5 keys OK;
// write 6th NEW key -> ErrQuotaExceeded; overwrite existing key -> OK.
func TestQuota_MaxKeys(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	ns := "maxkeys-ns"
	// No rate limit; only key-count cap.
	se.SetNamespaceLimit(ns, QuotaLimit{WritesPerSec: 0, MaxKeys: 5})

	// Write exactly 5 unique keys.
	for i := 0; i < 5; i++ {
		k := "key-" + string(rune('A'+i))
		if err := se.PutNS(ns, k, []byte("v"), -1); err != nil {
			t.Fatalf("PutNS key %s: %v", k, err)
		}
	}

	// 6th new key should be rejected.
	err := se.PutNS(ns, "key-F", []byte("v"), -1)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded for 6th new key, got: %v", err)
	}

	// Overwrite of an existing key should succeed (not a new key).
	if err := se.PutNS(ns, "key-A", []byte("v2"), -1); err != nil {
		t.Fatalf("overwrite of existing key should succeed, got: %v", err)
	}
}

// TestQuota_UnregisteredNamespace: No quota registered -> all writes succeed.
func TestQuota_UnregisteredNamespace(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	ns := "unregistered-ns"
	for i := 0; i < 20; i++ {
		k := "key"
		if err := se.PutNS(ns, k, []byte("v"), -1); err != nil {
			t.Fatalf("PutNS %d without quota should succeed, got: %v", i, err)
		}
	}
}

// TestQuota_ResetOnDelete: Delete key; key count decrements; new key can be added.
func TestQuota_ResetOnDelete(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	ns := "del-ns"
	se.SetNamespaceLimit(ns, QuotaLimit{WritesPerSec: 0, MaxKeys: 3})

	// Fill to capacity.
	for i := 0; i < 3; i++ {
		k := string(rune('a' + i))
		if err := se.PutNS(ns, k, []byte("v"), -1); err != nil {
			t.Fatalf("PutNS %s: %v", k, err)
		}
	}

	// Confirm the 4th new key fails.
	if err := se.PutNS(ns, "d", []byte("v"), -1); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded, got: %v", err)
	}

	// Delete one key to free a slot.
	if err := se.DeleteNS(ns, "a"); err != nil {
		t.Fatalf("DeleteNS: %v", err)
	}

	// Now a new key should succeed.
	if err := se.PutNS(ns, "d", []byte("v"), -1); err != nil {
		t.Fatalf("PutNS after delete should succeed, got: %v", err)
	}
}
