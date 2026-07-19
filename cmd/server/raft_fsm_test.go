package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// newFSMTestEngine boots a storage engine on a fresh temp dir with short flush
// windows (macOS F_FULLFSYNC is slow) and waits for WAL replay.
func newFSMTestEngine(t *testing.T) *storage.StorageEngine {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = t.TempDir()
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	engine, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("storage init: %v", err)
	}
	<-engine.ReplayDone
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func mustEncodeCmd(t *testing.T, c fsmCmd) []byte {
	t.Helper()
	b, err := encodeCmd(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// TestRaftFSM_ApplyBatch_PutCoalescing applies a run of plain PUTs via
// ApplyBatch (which routes them through engine.MultiPut) and verifies every key
// landed with the correct value and no Raft-level errors were returned.
func TestRaftFSM_ApplyBatch_PutCoalescing(t *testing.T) {
	f := newRaftFSM(newFSMTestEngine(t))
	const n = 256
	cmds := make([][]byte, n)
	for i := 0; i < n; i++ {
		cmds[i] = mustEncodeCmd(t, fsmCmd{
			Op: opPut, Key: fmt.Sprintf("k%04d", i), Value: []byte(fmt.Sprintf("v%04d", i)),
		})
	}
	results := f.ApplyBatch(cmds)
	if len(results) != n {
		t.Fatalf("results len = %d, want %d", len(results), n)
	}
	for i, r := range results {
		if r != nil {
			t.Fatalf("result[%d] = %v, want nil", i, r)
		}
	}
	for i := 0; i < n; i++ {
		got, err := f.engine.Get(fmt.Sprintf("k%04d", i))
		if err != nil {
			t.Fatalf("get k%04d: %v", i, err)
		}
		if want := fmt.Sprintf("v%04d", i); string(got) != want {
			t.Fatalf("k%04d = %q, want %q", i, got, want)
		}
	}
}

// TestRaftFSM_ApplyBatch_SameKeyOrder verifies coalesced PUTs to the same key
// preserve log order (last write wins) — the determinism guarantee that lets
// the applier vary batch boundaries across nodes safely.
func TestRaftFSM_ApplyBatch_SameKeyOrder(t *testing.T) {
	f := newRaftFSM(newFSMTestEngine(t))
	cmds := [][]byte{
		mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "k", Value: []byte("1")}),
		mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "k", Value: []byte("2")}),
		mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "k", Value: []byte("3")}),
	}
	f.ApplyBatch(cmds)
	got, err := f.engine.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != "3" {
		t.Fatalf("last-write-wins broken: k = %q, want %q", got, "3")
	}
}

// TestRaftFSM_ApplyBatch_MixedEquivalence applies a mixed PUT/DELETE/CAS/INCR
// sequence two ways — once entry-by-entry via Apply, once as a single
// ApplyBatch — and asserts both engines converge to byte-identical state, in
// order.  This proves the batched path (with PUT coalescing and non-PUT ops
// applied singly between runs) is semantically identical to serial Apply.
func TestRaftFSM_ApplyBatch_MixedEquivalence(t *testing.T) {
	seq := []fsmCmd{
		{Op: opPut, Key: "a", Value: []byte("1")},
		{Op: opPut, Key: "b", Value: []byte("2")},
		{Op: opDelete, Key: "a"},
		{Op: opPut, Key: "c", Value: []byte("3")},
		{Op: opPut, Key: "b", Value: []byte("22")}, // overwrite after a DELETE split
		{Op: opIncr, Key: "ctr", Delta: 5},
		{Op: opPut, Key: "d", Value: []byte("4")},
		{Op: opPut, Key: "e", Value: []byte("5")},
		{Op: opCAS, Key: "c", Expected: []byte("3"), Value: []byte("33")},
		{Op: opPut, Key: "f", Value: []byte("6")},
		{Op: opDelete, Key: "e"},
		{Op: opPut, Key: "g", Value: []byte("7")},
	}

	// Path 1: single Apply.
	f1 := newRaftFSM(newFSMTestEngine(t))
	for i, c := range seq {
		if err := f1.Apply(mustEncodeCmd(t, c)); err != nil {
			t.Fatalf("single apply[%d]: %v", i, err)
		}
	}

	// Path 2: one ApplyBatch over the whole sequence.
	f2 := newRaftFSM(newFSMTestEngine(t))
	cmds := make([][]byte, len(seq))
	for i, c := range seq {
		cmds[i] = mustEncodeCmd(t, c)
	}
	results := f2.ApplyBatch(cmds)
	for i, r := range results {
		if r != nil {
			t.Fatalf("batch result[%d] = %v, want nil", i, r)
		}
	}

	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "ctr"} {
		v1, e1 := f1.engine.Get(k)
		v2, e2 := f2.engine.Get(k)
		if (e1 == nil) != (e2 == nil) {
			t.Fatalf("key %q existence divergence: single err=%v batch err=%v", k, e1, e2)
		}
		if !bytes.Equal(v1, v2) {
			t.Fatalf("key %q value divergence: single=%q batch=%q", k, v1, v2)
		}
	}
}

// TestRaftFSM_ApplyBatch_CASResultDelivered asserts that an atomic op embedded
// between coalesced PUTs still delivers its result to the registered reqID
// waiter, exactly as the single-Apply path does.
func TestRaftFSM_ApplyBatch_CASResultDelivered(t *testing.T) {
	f := newRaftFSM(newFSMTestEngine(t))
	if err := f.Apply(mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "k", Value: []byte("old")})); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const reqID = 4242
	ch := f.register(reqID)
	defer f.unregister(reqID)

	cmds := [][]byte{
		mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "p", Value: []byte("x")}),
		mustEncodeCmd(t, fsmCmd{Op: opCAS, ReqID: reqID, Key: "k", Expected: []byte("old"), Value: []byte("new")}),
		mustEncodeCmd(t, fsmCmd{Op: opPut, Key: "q", Value: []byte("y")}),
	}
	f.ApplyBatch(cmds)

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("CAS result err: %v", res.err)
		}
		if res.casRes != storage.CASSuccess {
			t.Fatalf("CAS result = %v, want CASSuccess", res.casRes)
		}
	default:
		t.Fatal("CAS result not delivered to reqID waiter")
	}

	got, err := f.engine.Get("k")
	if err != nil || string(got) != "new" {
		t.Fatalf("CAS did not apply: k = %q err=%v", got, err)
	}
	// The surrounding PUTs must also have landed.
	if v, _ := f.engine.Get("p"); string(v) != "x" {
		t.Fatalf("pre-CAS put lost: p = %q", v)
	}
	if v, _ := f.engine.Get("q"); string(v) != "y" {
		t.Fatalf("post-CAS put lost: q = %q", v)
	}
}

// TestRaftFSM_NamespaceHashVectorOps applies the B3 ops (namespace, hash-field,
// vector, secondary-index) through the FSM and verifies engine state — the
// guarantee that raft mode replays these deterministically on every member.
func TestRaftFSM_NamespaceHashVectorOps(t *testing.T) {
	f := newRaftFSM(newFSMTestEngine(t))

	apply := func(c fsmCmd) {
		t.Helper()
		if err := f.Apply(mustEncodeCmd(t, c)); err != nil {
			t.Fatalf("apply %+v: %v", c, err)
		}
	}

	// Namespace put/get/delete.
	apply(fsmCmd{Op: opNSPut, Ns: "users", Key: "u1", Value: []byte("alice"), TTL: -1})
	apply(fsmCmd{Op: opNSPut, Ns: "users", Key: "u2", Value: []byte("bob"), TTL: -1})
	if v, err := f.engine.GetNS("users", "u1"); err != nil || string(v) != "alice" {
		t.Fatalf("nsget u1 = %q err=%v", v, err)
	}
	apply(fsmCmd{Op: opNSDelete, Ns: "users", Key: "u1"})
	if _, err := f.engine.GetNS("users", "u1"); err == nil {
		t.Fatal("u1 still present after opNSDelete")
	}

	// Namespace drop returns the deleted count via the result channel.
	const dropReq = 777
	ch := f.register(dropReq)
	apply(fsmCmd{Op: opNSDrop, Ns: "users", ReqID: dropReq})
	select {
	case res := <-ch:
		if res.err != nil || res.intVal != 1 {
			t.Fatalf("nsdrop result = %+v, want intVal=1", res)
		}
	default:
		t.Fatal("nsdrop result not delivered")
	}
	f.unregister(dropReq)

	// Hash fields.
	apply(fsmCmd{Op: opHSet, Key: "sess", Field: "name", Value: []byte("carol"), TTL: -1})
	apply(fsmCmd{Op: opHSet, Key: "sess", Field: "city", Value: []byte("pune"), TTL: -1})
	if v, err := f.engine.HGet("sess", "name"); err != nil || string(v) != "carol" {
		t.Fatalf("hget = %q err=%v", v, err)
	}
	apply(fsmCmd{Op: opHDel, Key: "sess", Field: "city"})
	if n := f.engine.HLen("sess"); n != 1 {
		t.Fatalf("hlen = %d, want 1", n)
	}
	apply(fsmCmd{Op: opHExpire, Key: "sess", Field: "name", TTL: 3600})
	if ttl := f.engine.HTTL("sess", "name"); ttl <= 0 || ttl > 3600 {
		t.Fatalf("httl = %d, want (0, 3600]", ttl)
	}

	// Vector upsert: registered + searchable + persisted.
	apply(fsmCmd{Op: opVSet, Ns: "fsmvecs", Key: "v1", Vec: []float32{1, 0, 0}})
	apply(fsmCmd{Op: opVSet, Ns: "fsmvecs", Key: "v2", Vec: []float32{0, 1, 0}})
	matches, err := f.engine.SearchVector("fsmvecs", []float32{0.9, 0.1, 0}, 1)
	if err != nil || len(matches) != 1 || matches[0].ID != "v1" {
		t.Fatalf("vsearch = %+v err=%v, want v1", matches, err)
	}

	// Secondary index create → visible via lookup after an NS write; then drop.
	apply(fsmCmd{Op: opIdxCreate, Key: "bycity", Field: "city"})
	apply(fsmCmd{Op: opNSPut, Ns: "profiles", Key: "p1", Value: []byte(`{"city":"pune"}`), TTL: -1})
	if keys := f.engine.LookupBySecondary("bycity", "pune"); len(keys) == 0 {
		t.Fatal("secondary index lookup empty after opIdxCreate + opNSPut")
	}
	apply(fsmCmd{Op: opIdxDrop, Key: "bycity"})
}
