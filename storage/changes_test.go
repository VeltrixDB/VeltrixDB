package storage

import (
	"testing"
	"time"
)

// TestChangesSince_CatchUpFeed: writes + a delete are all visible from cursor
// zero, pagination respects limit, and a later cursor excludes older writes.
func TestChangesSince_CatchUpFeed(t *testing.T) {
	se := newListTestEngine(t)

	se.Put("c1", []byte("v1"), -1)
	se.Put("c2", []byte("v2"), -1)
	time.Sleep(2 * time.Millisecond) // separate timestamps at µs resolution
	midUs := time.Now().UnixMicro()
	time.Sleep(2 * time.Millisecond)
	se.Put("c3", []byte("v3"), -1)
	se.Put("c4", []byte("v4"), -1)
	se.Delete("c1")

	// Full feed from zero: c2,c3,c4 as PUT; c1 as DEL (tombstone).
	res := se.ChangesSince(0, 0)
	got := map[string]string{}
	for _, ev := range res.Events {
		got[ev.Key] = ev.Op
	}
	if got["c2"] != "PUT" || got["c3"] != "PUT" || got["c4"] != "PUT" {
		t.Fatalf("missing PUTs in feed: %v", got)
	}
	if got["c1"] != "DEL" {
		t.Fatalf("tombstone not in feed: %v", got)
	}

	// Cursor after c1/c2: only later writes (c3, c4) and the delete of c1
	// (its tombstone timestamp postdates midUs).
	res = se.ChangesSince(midUs, 0)
	got = map[string]string{}
	for _, ev := range res.Events {
		got[ev.Key] = ev.Op
	}
	if _, ok := got["c2"]; ok {
		t.Fatalf("c2 written before cursor leaked into feed: %v", got)
	}
	if got["c3"] != "PUT" || got["c4"] != "PUT" || got["c1"] != "DEL" {
		t.Fatalf("expected c3,c4 PUT + c1 DEL after cursor: %v", got)
	}

	// Pagination: limit=1 → More=true and a usable next cursor.
	res = se.ChangesSince(0, 1)
	if len(res.Events) != 1 || !res.More {
		t.Fatalf("limit=1: events=%d more=%v", len(res.Events), res.More)
	}
	res2 := se.ChangesSince(res.Cursor, 0)
	if len(res2.Events) == 0 {
		t.Fatal("resume from pagination cursor returned nothing")
	}

	// Events must be timestamp-ordered.
	res = se.ChangesSince(0, 0)
	for i := 1; i < len(res.Events); i++ {
		if res.Events[i].Timestamp < res.Events[i-1].Timestamp {
			t.Fatal("feed not timestamp-ordered")
		}
	}
}
