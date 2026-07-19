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
		for k, entry := range shard.entries {
			if entry.WriteTimestampUs < sinceUs {
				continue
			}
			if !entry.IsTombstone() && entry.IsExpired(nowUs) {
				continue
			}
			matches = append(matches, change{key: k, ts: entry.WriteTimestampUs, tombstone: entry.IsTombstone()})
		}
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
