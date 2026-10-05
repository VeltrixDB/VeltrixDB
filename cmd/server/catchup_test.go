package main

import (
	"sort"
	"testing"

	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// fakeFeed mimics StorageEngine.ChangesSince: inclusive cursor, timestamp order.
type fakeFeed struct {
	events []storage.CDCEvent
	ttl    map[string]int32
}

func (f *fakeFeed) ChangesSince(sinceUs int64, limit int) storage.ChangesSinceResult {
	var m []storage.CDCEvent
	for _, e := range f.events {
		if e.Timestamp >= sinceUs {
			m = append(m, e)
		}
	}
	sort.SliceStable(m, func(i, j int) bool { return m[i].Timestamp < m[j].Timestamp })
	res := storage.ChangesSinceResult{Cursor: sinceUs}
	if limit > 0 && len(m) > limit {
		m, res.More = m[:limit], true
	}
	res.Events = m
	if len(m) > 0 {
		res.Cursor = m[len(m)-1].Timestamp
	}
	return res
}

func (f *fakeFeed) GetTTLForKey(k string) int32 {
	if t, ok := f.ttl[k]; ok {
		return t
	}
	return -1
}

// TestEngineCatchUp_PagesFeedIntoOps: the recovery catch-up source converts
// the change feed to replication ops (tombstones, TTLs, ns timestamps), skips
// keys about to expire, and does not loop when a full page shares one
// timestamp.
func TestEngineCatchUp_PagesFeedIntoOps(t *testing.T) {
	f := &fakeFeed{ttl: map[string]int32{"ttl": 30, "expiring": 0}}
	f.events = append(f.events,
		storage.CDCEvent{Op: "PUT", Key: "old", Value: []byte("x"), Timestamp: 5},
		storage.CDCEvent{Op: "DEL", Key: "gone", Timestamp: 10},
		storage.CDCEvent{Op: "PUT", Key: "ttl", Value: []byte("t"), Timestamp: 11},
		storage.CDCEvent{Op: "PUT", Key: "expiring", Value: []byte("e"), Timestamp: 12},
	)
	// More than one page at a single timestamp.
	for i := 0; i < catchUpPageSize+5; i++ {
		f.events = append(f.events, storage.CDCEvent{Op: "PUT", Key: "same", Value: []byte("s"), Timestamp: 20})
	}

	seen := map[string]*replication.WriteOperation{}
	count := 0
	err := engineCatchUp(f, "n1")(10_000, func(ops []*replication.WriteOperation) error {
		for _, op := range ops {
			seen[op.Key] = op
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := seen["old"]; ok {
		t.Error("event before the cursor was shipped")
	}
	if op := seen["gone"]; op == nil || !op.IsTombstone || op.Timestamp != 10_000 {
		t.Errorf("tombstone op = %+v", op)
	}
	if op := seen["ttl"]; op == nil || op.TTL != 30 || string(op.Value) != "t" {
		t.Errorf("ttl op = %+v", op)
	}
	if _, ok := seen["expiring"]; ok {
		t.Error("key with <1s TTL left was shipped (would become immortal)")
	}
	if seen["same"] == nil || count < catchUpPageSize+5 {
		t.Errorf("same-timestamp run not fully shipped (count=%d)", count)
	}
}
