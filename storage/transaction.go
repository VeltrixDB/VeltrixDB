package storage

// transaction.go — multi-key transactions via batched MultiPut.
//
// Semantics:
//   - All Set operations buffered in memory; nothing visible until Commit.
//   - Commit goes through MultiPut, which atomically appends one batch to
//     each disk's WAL+VLog. Within a single disk the batch is ordered;
//     across disks it is not strictly serializable but each disk durably
//     persists the entire batch or none of it.
//   - Reads inside the transaction see *committed* state (they bypass the
//     buffer) — this is "read-committed" not "snapshot isolation". For
//     true MVCC, see future-work note below.
//   - Optimistic concurrency: each Set captures the version of the key
//     it overwrites at Set time. On Commit, if any captured version is
//     stale (someone else wrote in between), Commit returns ErrTxnConflict
//     and the caller must retry.
//
// Future work (out of scope for this implementation):
//   - True snapshot isolation requires versioned reads (MVCC). Index Vault
//     would need to keep N versions per key with a vacuum policy. Multi-week.
//   - Cross-disk strict serializability would require 2PC across WAL flushers;
//     infrastructure exists (channels per disk) but adds latency. Days.

import (
	"errors"
	"sync"
	"time"
)

// ErrTxnConflict is returned by Txn.Commit when an optimistic-CAS check fails.
var ErrTxnConflict = errors.New("transaction conflict")

// Txn is a builder for a batched, atomic write set.
type Txn struct {
	se        *StorageEngine
	mu        sync.Mutex
	writes    []txnOp
	committed bool
}

type txnOp struct {
	key             string
	value           []byte
	ttl             int32
	delete          bool
	expectedVersion uint64
	hasExpected     bool
}

// BeginTxn starts a fresh transaction. Not safe to share across goroutines.
func (se *StorageEngine) BeginTxn() *Txn {
	return &Txn{se: se}
}

// KeyVersion returns the optimistic-concurrency version token for key — the
// value a client passes back to Txn.SetIf (and the TXN wire command), and
// exactly what Commit compares it with (versionToken).  It is the key's
// WriteTimestampUs: in raft mode the raft-clock stamp of the command that
// last wrote it, the same on every node and across restarts, snapshot
// installs and rebuilds from the log — LegacyVersion (1) for a key last
// written before the raft clock started; elsewhere the local write time.
// Returns 0 when the key is absent, tombstoned, or TTL-expired — so
// "expect 0" expresses "commit only if the key still does not exist".  It is
// monotonic per key: every later write of the key gets a larger one
// (raft_clock.go, invariant 66).
func (se *StorageEngine) KeyVersion(key string) uint64 {
	entry, _, ok := se.index.get(key)
	if !ok {
		return 0
	}
	_, floor := se.RaftClock()
	return versionToken(entry, time.Now().UnixMicro(), floor)
}

// Set stages key=value in the transaction. ttl < 0 means no TTL.
func (t *Txn) Set(key string, value []byte, ttl int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = append(t.writes, txnOp{key: key, value: value, ttl: ttl})
}

// SetIf stages key=value but adds an optimistic-CAS guard: if at Commit
// time the key's stored version differs from expectedVersion, the entire
// transaction aborts with ErrTxnConflict.
func (t *Txn) SetIf(key string, value []byte, ttl int32, expectedVersion uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = append(t.writes, txnOp{
		key: key, value: value, ttl: ttl,
		expectedVersion: expectedVersion, hasExpected: true,
	})
}

// Delete stages a tombstone for key.
func (t *Txn) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = append(t.writes, txnOp{key: key, delete: true})
}

// Read returns the *currently-committed* value for key. Reads do not see
// pending writes inside this transaction.
func (t *Txn) Read(key string) ([]byte, error) {
	return t.se.Get(key)
}

// Commit applies all staged operations atomically.  Returns ErrTxnConflict
// when any SetIf guard fails (optimistic-concurrency abort) — the caller
// is expected to retry with the latest read.
func (t *Txn) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.committed {
		return errors.New("transaction already committed")
	}

	// Phase 1: validate all SetIf guards under shard read locks.  The live
	// version is KeyVersion's token, judged at the decision time — inside
	// raft Apply the command's time, so every node decides alike
	// (raft_clock.go).  An expired key counts as absent (version 0), as in
	// KeyVersion; it used to count as live until something tombstoned it.
	nowUs := t.se.decisionNowUs()
	_, floor := t.se.RaftClock()
	for _, op := range t.writes {
		if !op.hasExpected {
			continue
		}
		var liveVersion uint64
		if entry, _, ok := t.se.index.get(op.key); ok {
			liveVersion = versionToken(entry, nowUs, floor)
		}
		if liveVersion != op.expectedVersion {
			return ErrTxnConflict
		}
	}

	// Phase 2: separate writes from deletes; drive each through the engine's
	// batched paths.
	var puts []MultiPutRequest
	for _, op := range t.writes {
		if op.delete {
			continue
		}
		puts = append(puts, MultiPutRequest{Key: op.key, Value: op.value, TTL: op.ttl})
	}
	if len(puts) > 0 {
		errs := t.se.MultiPut(puts)
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}
	for _, op := range t.writes {
		if !op.delete {
			continue
		}
		if err := t.se.Delete(op.key); err != nil {
			return err
		}
	}
	t.committed = true
	return nil
}

// Abort discards the transaction. Idempotent.
func (t *Txn) Abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = nil
	t.committed = true
}

// Size returns the number of buffered operations.
func (t *Txn) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.writes)
}
