package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func newListTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	t.Cleanup(func() { _ = se.Close() })
	return se
}

func TestList_PushPopOrder(t *testing.T) {
	se := newListTestEngine(t)

	// RPUSH a b c → [a b c]; LPUSH z → [z a b c]
	for _, v := range []string{"a", "b", "c"} {
		if _, _, err := se.RPush("l", []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	n, _, err := se.LPush("l", []byte("z"))
	if err != nil || n != 4 {
		t.Fatalf("LPush n=%d err=%v, want 4", n, err)
	}
	if got := se.LLen("l"); got != 4 {
		t.Fatalf("LLen = %d, want 4", got)
	}

	vals, err := se.LRange("l", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"z", "a", "b", "c"}
	if len(vals) != len(want) {
		t.Fatalf("LRange len=%d want %d", len(vals), len(want))
	}
	for i, w := range want {
		if string(vals[i]) != w {
			t.Fatalf("LRange[%d] = %q, want %q", i, vals[i], w)
		}
	}

	// Pops from both ends.
	v, found, _, err := se.LPop("l")
	if err != nil || !found || string(v) != "z" {
		t.Fatalf("LPop = %q found=%v err=%v, want z", v, found, err)
	}
	v, found, _, err = se.RPop("l")
	if err != nil || !found || string(v) != "c" {
		t.Fatalf("RPop = %q found=%v err=%v, want c", v, found, err)
	}
	if got := se.LLen("l"); got != 2 {
		t.Fatalf("LLen after pops = %d, want 2", got)
	}

	// Drain to empty; next pop reports found=false.
	se.LPop("l")
	se.LPop("l")
	if _, found, _, _ := se.LPop("l"); found {
		t.Fatal("LPop on empty list reported found")
	}
	if got := se.LLen("l"); got != 0 {
		t.Fatalf("LLen empty = %d", got)
	}
}

func TestList_RangeNegativeIndices(t *testing.T) {
	se := newListTestEngine(t)
	for i := 0; i < 5; i++ {
		se.RPush("r", []byte(fmt.Sprintf("v%d", i)))
	}
	vals, _ := se.LRange("r", -2, -1)
	if len(vals) != 2 || string(vals[0]) != "v3" || string(vals[1]) != "v4" {
		t.Fatalf("LRange -2 -1 = %q", vals)
	}
	vals, _ = se.LRange("r", 1, 2)
	if len(vals) != 2 || string(vals[0]) != "v1" || string(vals[1]) != "v2" {
		t.Fatalf("LRange 1 2 = %q", vals)
	}
	if vals, _ := se.LRange("r", 4, 1); len(vals) != 0 {
		t.Fatalf("inverted range returned %q", vals)
	}
}

func TestList_SurvivesRestart(t *testing.T) {
	cfg := DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se.ReplayDone
	se.RPush("pl", []byte("one"))
	se.RPush("pl", []byte("two"))
	se.Close()

	se2, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	<-se2.ReplayDone
	defer se2.Close()
	if n := se2.LLen("pl"); n != 2 {
		t.Fatalf("LLen after restart = %d, want 2", n)
	}
	vals, _ := se2.LRange("pl", 0, -1)
	if len(vals) != 2 || !bytes.Equal(vals[0], []byte("one")) || !bytes.Equal(vals[1], []byte("two")) {
		t.Fatalf("LRange after restart = %q", vals)
	}
}

func TestSet_AddRemMembers(t *testing.T) {
	se := newListTestEngine(t)

	added, _, err := se.SAdd("s", "alpha")
	if err != nil || !added {
		t.Fatalf("SAdd alpha added=%v err=%v", added, err)
	}
	added, _, _ = se.SAdd("s", "alpha")
	if added {
		t.Fatal("duplicate SAdd reported added")
	}
	se.SAdd("s", "beta")

	if !se.SIsMember("s", "alpha") || se.SIsMember("s", "gamma") {
		t.Fatal("SIsMember wrong")
	}
	if n := se.SCard("s"); n != 2 {
		t.Fatalf("SCard = %d, want 2", n)
	}
	members := se.SMembers("s")
	if len(members) != 2 {
		t.Fatalf("SMembers = %v", members)
	}

	removed, _, _ := se.SRem("s", "alpha")
	if !removed {
		t.Fatal("SRem existing member reported not removed")
	}
	removed, _, _ = se.SRem("s", "alpha")
	if removed {
		t.Fatal("SRem absent member reported removed")
	}
	if n := se.SCard("s"); n != 1 {
		t.Fatalf("SCard after SRem = %d, want 1", n)
	}
}

// TestListSet_NoKeyspaceCollision: list/set internal keys must not leak into
// plain GETs or collide with hash fields on the same user key.
func TestListSet_NoKeyspaceCollision(t *testing.T) {
	se := newListTestEngine(t)
	se.Put("k", []byte("plain"), -1)
	se.HSet("k", "f", []byte("hash"), -1)
	se.RPush("k", []byte("list"))
	se.SAdd("k", "member")

	if v, err := se.Get("k"); err != nil || string(v) != "plain" {
		t.Fatalf("plain key corrupted: %q err=%v", v, err)
	}
	if v, _ := se.HGet("k", "f"); string(v) != "hash" {
		t.Fatalf("hash field corrupted: %q", v)
	}
	if n := se.LLen("k"); n != 1 {
		t.Fatalf("list len = %d", n)
	}
	if !se.SIsMember("k", "member") {
		t.Fatal("set member lost")
	}
}
