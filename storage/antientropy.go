package storage

// antientropy.go — digest-based anti-entropy reconciliation for replicated mode.
//
// The push-replication path (replication/engine.go) delivers live writes, and
// the LWW apply (lww.go) makes concurrent writes converge REGARDLESS of arrival
// order. But delivery itself is not guaranteed: a replica that is partitioned
// or crashed can MISS writes entirely, and the retry-only resend never
// reconciles a replica that fell behind and was marked failed. That is the gap
// that leaves a replica permanently behind ("zombie"/stale data).
//
// This file closes it with classic digest-based (Merkle-style) anti-entropy:
//
//   1. Each node summarises every shard as a 64-bit digest folded over its
//      entries' (key, LWW clock, tombstone) — so any missing/older/extra entry
//      changes the shard's digest.
//   2. A node periodically compares its per-shard digests with a peer's. For
//      every shard whose digests differ, it PULLS the peer's entries for that
//      shard and LWW-applies them locally.
//   3. Because apply is last-writer-wins, pulling is safe and idempotent: newer
//      local data is kept, older peer data is ignored, missing data is filled.
//      Run in both directions (each node reconciles against the other), the two
//      replicas converge to the union under LWW — even for writes neither the
//      live path nor retry ever delivered.
//
// The digest fold is XOR of a strong per-entry hash: order-independent (no need
// to sort a shard) and cheap. A false "match" (two differing shards hashing
// equal) is astronomically unlikely with the 64-bit mix and, even if it
// happened, the next periodic round with any further write would surface it.

// SyncEntry is one key's replicable state, shipped during anti-entropy. It
// carries the LWW clock so the receiver applies it under the same
// last-writer-wins rule as a live write.
type SyncEntry struct {
	Key        string
	Value      []byte
	LWWStampNs int64 // origin wall-clock (ns) — the LWW clock for this key
	Tombstone  bool
	TTLSeconds int32 // remaining TTL in seconds; -1 = immortal, 0 = none/expired
}

// PeerSync is the read side of a replica used for anti-entropy. An in-process
// implementation wraps a *StorageEngine directly (see EnginePeer); the
// production implementation talks to a remote node over the replication
// transport (replication.TCPPeerSync).
type PeerSync interface {
	// ShardDigests returns the peer's non-empty per-shard digests.
	ShardDigests() (map[uint16]uint64, error)
	// FetchShard returns all of the peer's entries for one shard.
	FetchShard(shardID uint16) ([]SyncEntry, error)
}

// splitmix64 finalizer — strong 64-bit avalanche for the per-entry hash so the
// XOR fold is not defeated by structured keys/clocks.
func syncMix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// syncEntryHash hashes one entry's identity+version for the shard digest fold.
func syncEntryHash(key string, lwwNs int64, tombstone bool) uint64 {
	h := fnv64a(key)
	h = syncMix(h ^ uint64(lwwNs))
	if tombstone {
		h ^= 0x9E3779B97F4A7C15 // golden ratio — flips the hash for tombstones
	}
	return h
}

// ShardDigest returns the digest of one shard: the XOR fold of every entry's
// (key, LWW clock, tombstone) hash. Empty shards return 0. Tombstones ARE
// included — a replica that has a live key where the peer has a tombstone (or
// vice-versa) must be detected as divergent.
func (se *StorageEngine) ShardDigest(shardID uint16) uint64 {
	shard := &se.index.shards[shardID]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	var d uint64
	for key, e := range shard.entries {
		d ^= syncEntryHash(key, lwwClock(e), e.IsTombstone())
	}
	return d
}

// ShardDigests returns every non-empty shard's digest. At 8192 shards this is a
// map of at most 8192 uint64 entries (~192 KB fully populated) — small enough
// to exchange each anti-entropy round.
func (se *StorageEngine) ShardDigests() (map[uint16]uint64, error) {
	out := make(map[uint16]uint64)
	for i := 0; i < numShards; i++ {
		if d := se.ShardDigest(uint16(i)); d != 0 {
			out[uint16(i)] = d
		}
	}
	return out, nil
}

// FetchShard returns all entries (live and tombstoned) for one shard as
// SyncEntries, ready to ship to a peer for LWW reconciliation. Values are read
// after the shard lock is released (via Get), so a concurrent write may race;
// that is harmless because the receiver applies under LWW.
func (se *StorageEngine) FetchShard(shardID uint16) ([]SyncEntry, error) {
	type meta struct {
		key       string
		lww       int64
		tombstone bool
	}
	shard := &se.index.shards[shardID]
	shard.mu.RLock()
	metas := make([]meta, 0, len(shard.entries))
	for key, e := range shard.entries {
		metas = append(metas, meta{key: key, lww: lwwClock(e), tombstone: e.IsTombstone()})
	}
	shard.mu.RUnlock()

	out := make([]SyncEntry, 0, len(metas))
	for _, m := range metas {
		ent := SyncEntry{Key: m.key, LWWStampNs: m.lww, Tombstone: m.tombstone}
		if !m.tombstone {
			// Value + TTL are read after the lock is released; skip keys that
			// vanished (a concurrent delete) — the tombstone will sync instead.
			val, err := se.Get(m.key)
			if err != nil {
				continue
			}
			ent.Value = val
			ent.TTLSeconds = se.GetTTLForKey(m.key)
		}
		out = append(out, ent)
	}
	return out, nil
}

// ApplySyncEntry LWW-applies one entry received during anti-entropy. It returns
// whether the entry won locally (was newer than what we had). Missing values on
// a non-tombstone entry are treated as a no-op (the peer raced a delete).
func (se *StorageEngine) ApplySyncEntry(e SyncEntry) (bool, error) {
	if e.Tombstone {
		return se.ApplyLWWDelete(e.Key, e.LWWStampNs)
	}
	return se.ApplyLWWPut(e.Key, e.Value, e.TTLSeconds, e.LWWStampNs)
}

// Reconcile pulls divergent shards from peer and LWW-applies them locally,
// converging this engine toward the union of both replicas. Returns the number
// of entries that won locally (i.e. actually changed state). Safe to run
// concurrently with live traffic; every apply goes through the LWW gate.
//
// Run in both directions periodically (A.Reconcile(B) and B.Reconcile(A)) for
// full convergence between two replicas.
func (se *StorageEngine) Reconcile(peer PeerSync) (int, error) {
	peerDigests, err := peer.ShardDigests()
	if err != nil {
		return 0, err
	}
	repaired := 0
	// Only shards the peer actually has can be pulled; a shard the peer lacks
	// entirely but we have is repaired when the PEER runs Reconcile against us.
	for shardID, pd := range peerDigests {
		if se.ShardDigest(shardID) == pd {
			continue // identical — nothing to pull
		}
		entries, ferr := peer.FetchShard(shardID)
		if ferr != nil {
			return repaired, ferr
		}
		for _, e := range entries {
			applied, aerr := se.ApplySyncEntry(e)
			if aerr != nil {
				return repaired, aerr
			}
			if applied {
				repaired++
			}
		}
	}
	return repaired, nil
}

// EnginePeer adapts a local *StorageEngine to the PeerSync interface for
// in-process reconciliation (embedded multi-engine setups and tests).
type EnginePeer struct{ E *StorageEngine }

func (p EnginePeer) ShardDigests() (map[uint16]uint64, error) { return p.E.ShardDigests() }
func (p EnginePeer) FetchShard(shardID uint16) ([]SyncEntry, error) {
	return p.E.FetchShard(shardID)
}
