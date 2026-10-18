package main

import (
	"errors"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/replication"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// fakeFeed mimics StorageEngine.ScanChanges: inclusive cursor, pages of at
// most pageSize, no particular order.
type fakeFeed struct {
	events []storage.CDCEvent
	ttl    map[string]int32
	pages  int
}

func (f *fakeFeed) ScanChanges(sinceUs int64, pageSize int, localOnly bool, emit func([]storage.CDCEvent) error) error {
	var page []storage.CDCEvent
	for _, e := range f.events {
		if e.Timestamp < sinceUs {
			continue
		}
		page = append(page, e)
		if len(page) == pageSize {
			f.pages++
			if err := emit(page); err != nil {
				return err
			}
			page = nil
		}
	}
	if len(page) > 0 {
		f.pages++
		return emit(page)
	}
	return nil
}

func (f *fakeFeed) GetTTLForKey(k string) int32 {
	if t, ok := f.ttl[k]; ok {
		return t
	}
	return -1
}

// TestEngineCatchUp_PagesFeedIntoOps: the recovery catch-up source converts
// the change feed to replication ops (tombstones, TTLs, ns timestamps), skips
// keys about to expire, and ships a run longer than one page in full.
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
	err := engineCatchUp(f, nil, "n1")(10_000, func(ops []*replication.WriteOperation) error {
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

// TestEngineCatchUp_WaitsForReplayAndStopsOnError: the catch-up does not scan
// before the startup WAL replay finished (it would miss keys and the replica
// would be declared caught up without them), and an emit error stops it.
func TestEngineCatchUp_WaitsForReplayAndStopsOnError(t *testing.T) {
	f := &fakeFeed{}
	for i := 0; i < 3*catchUpPageSize; i++ {
		f.events = append(f.events, storage.CDCEvent{Op: "DEL", Key: "k", Timestamp: 1})
	}
	replay := make(chan struct{})
	done := make(chan error, 1)
	boom := errors.New("replica apply failed")
	go func() {
		done <- engineCatchUp(f, replay, "n1")(0, func([]*replication.WriteOperation) error { return boom })
	}()
	select {
	case err := <-done:
		t.Fatalf("catch-up ran before replay finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(replay)
	if err := <-done; !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the emit error", err)
	}
	if f.pages != 1 {
		t.Errorf("scan emitted %d pages after the first failed, want 1", f.pages)
	}
}
