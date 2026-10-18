package storage

// changes.go — durable change-feed catch-up for cross-region replication.
//
// The live CDC stream (cdc.go) is in-memory: a repl-ship subscriber that is
// down misses every event published while it was away.  ChangesSince closes
// that gap: it scans the index for entries written at-or-after a microsecond
// cursor, so a shipper can persist "last shipped timestamp" and replay the
// delta on restart before rejoining the live stream.
//
// Semantics:
//   - The cursor is WriteTimestampUs (single node clock — monotone in
//     practice; CDCEvent.Timestamp carries the same value on the live path).
//   - The boundary is INCLUSIVE (>= since): events sharing the checkpoint's
//     exact microsecond re-ship rather than being lost.  Destination applies
//     are last-write-wins Puts/Deletes, so replays are harmless.
//   - Tombstones ARE included (Op="DEL") — deletes made during shipper
//     downtime must reach the remote region too.  Expired-TTL entries are
//     skipped (they expire remotely on their own).
//   - Results are timestamp-ordered.  With `limit`, pagination resumes from
//     the last returned event's timestamp (inclusive — see above).

import (
	"sort"
	"time"
)

// ChangesSinceResult is one page of the catch-up feed.
type ChangesSinceResult struct {
	Events []CDCEvent
	// Cursor is the resume point for the next page (timestamp of the last
	// event returned). Only meaningful when More is true.
	Cursor int64
	// More reports whether entries beyond this page matched the scan.
	More bool
}

// ChangesSince returns up to limit index entries written at or after sinceUs,
// as CDC-shaped events ordered by write timestamp. limit <= 0 means no limit.
func (se *StorageEngine) ChangesSince(sinceUs int64, limit int) ChangesSinceResult {
	type change struct {
		key       string
		ts        int64
		tombstone bool
	}
	nowUs := time.Now().UnixMicro()

	var matches []change
	for i := range se.index.shards {
		shard := &se.index.shards[i]
		shard.mu.RLock()
		shard.entries.rangeAll(func(k string, entry *IndexEntry) bool {
			if entry.WriteTimestampUs < sinceUs {
				return true
			}
			if !entry.IsTombstone() && entry.IsExpired(nowUs) {
				return true
			}
			matches = append(matches, change{key: k, ts: entry.WriteTimestampUs, tombstone: entry.IsTombstone()})
			return true
		})
		shard.mu.RUnlock()
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].ts < matches[j].ts })

	more := false
	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
		more = true
	}

	events := make([]CDCEvent, 0, len(matches))
	for _, m := range matches {
		if m.tombstone {
			events = append(events, CDCEvent{Op: "DEL", Key: m.key, Timestamp: m.ts})
			continue
		}
		val, err := se.Get(m.key)
		if err != nil {
			continue // raced with a delete/expiry after the scan — skip
		}
		events = append(events, CDCEvent{Op: "PUT", Key: m.key, Value: val, Timestamp: m.ts})
	}

	res := ChangesSinceResult{Events: events, More: more}
	if len(events) > 0 {
		res.Cursor = events[len(events)-1].Timestamp
	} else {
		res.Cursor = sinceUs
	}
	return res
}

// ScanChanges streams the same events as ChangesSince — the current value
// (or tombstone) of every key written at or after sinceUs, expired keys
// skipped — to emit in pages of at most pageSize, in ONE pass over the index
// and without ordering them by timestamp.  It is the replication catch-up
// source: ChangesSince re-scans and re-sorts the whole index for every page,
// O(N × pages), which a full resync of a large engine cannot afford.
//
// Snapshot semantics are per shard: a key written after its shard was
// scanned may be missing, and a key deleted between the scan and its value
// read is skipped.  A caller that needs those writes must get them from
// elsewhere (the replication engine replays them from its retention buffer
// after the catch-up).  An emit error stops the scan and is returned.
//
// localOnly skips entries whose latest write was applied from a replication
// peer (FlagReplicaApplied): the replication catch-up must only re-send
// writes this node originated.
func (se *StorageEngine) ScanChanges(sinceUs int64, pageSize int, localOnly bool, emit func([]CDCEvent) error) error {
	if pageSize <= 0 {
		pageSize = 1000
	}
	type change struct {
		key       string
		ts        int64
		tombstone bool
	}
	var matches []change
	page := make([]CDCEvent, 0, pageSize)
	flush := func() error {
		if len(page) == 0 {
			return nil
		}
		err := emit(page)
		page = make([]CDCEvent, 0, pageSize) // emit may retain the slice
		return err
	}
	for i := range se.index.shards {
		nowUs := time.Now().UnixMicro()
		matches = matches[:0]
		shard := &se.index.shards[i]
		shard.mu.RLock()
		shard.entries.rangeAll(func(k string, entry *IndexEntry) bool {
			if entry.WriteTimestampUs < sinceUs {
				return true
			}
			if localOnly && entry.Flags&FlagReplicaApplied != 0 {
				return true
			}
			if !entry.IsTombstone() && entry.IsExpired(nowUs) {
				return true
			}
			matches = append(matches, change{key: k, ts: entry.WriteTimestampUs, tombstone: entry.IsTombstone()})
			return true
		})
		shard.mu.RUnlock()
		for _, m := range matches {
			if m.tombstone {
				page = append(page, CDCEvent{Op: "DEL", Key: m.key, Timestamp: m.ts})
			} else {
				val, err := se.Get(m.key)
				if err != nil {
					continue // raced with a delete/expiry after the scan
				}
				page = append(page, CDCEvent{Op: "PUT", Key: m.key, Value: val, Timestamp: m.ts})
			}
			if len(page) >= pageSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// PutReplicated is Put for a write applied from a --mode=replicated peer: the
// entry is marked FlagReplicaApplied (durably, in the WAL), so this node's
// catch-up feed (ScanChanges localOnly) never re-sends it as its own.
func (se *StorageEngine) PutReplicated(key string, value []byte, ttl int32) error {
	return se.putOrigin(key, value, ttl, 0, false, FlagReplicaApplied)
}

// DeleteReplicated is Delete for a delete applied from a replication peer
// (see PutReplicated).
func (se *StorageEngine) DeleteReplicated(key string) error {
	return se.delOrigin(key, false, FlagReplicaApplied)
}
