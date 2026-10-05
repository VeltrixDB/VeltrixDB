package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// Regression tests for four read/write-path bugs:
//
//  1. A TTL'd key was served from the LIRS cache after it expired, until the
//     background TTL scanner (30 s default) tombstoned it.
//  2. An empty value written with MultiPut read back as not-found from
//     MultiGet while Get returned it.
//  3. CDC and the audit log saw only engine.Put / engine.Delete: MultiPut,
//     atomic ops and the TXN commit emitted nothing.
//  4. CDC subscriber eviction counted TOTAL drops, not consecutive ones.

// TestTTL_ExpiredKeyNotServedFromCache: every cache-hit read path must treat
// an expired entry as not found, without waiting for the TTL scanner.
func TestTTL_ExpiredKeyNotServedFromCache(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t) // TTLCheckInterval is 30 s: the scanner never runs here

	if err := se.Put("ttl-get", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := se.PutNS("ns1", "ttl-ns", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	if err := se.HSet("ttl-hash", "f", []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	// MultiPut is write-around; read the key once so it is cache-resident,
	// then overwrite it in a batch so PutIfPresent refreshes the cached copy
	// with a TTL.
	if err := se.Put("ttl-mput", []byte("old"), -1); err != nil {
		t.Fatal(err)
	}
	if _, err := se.Get("ttl-mput"); err != nil {
		t.Fatal(err)
	}
	if errs := se.MultiPut([]MultiPutRequest{{Key: "ttl-mput", Value: []byte("v"), TTL: 1}}); errs[0] != nil {
		t.Fatal(errs[0])
	}
	// Prime the cache on every key and confirm the values are live first.
	for _, k := range []string{"ttl-get", NSKey("ns1", "ttl-ns"), HashFieldKey("ttl-hash", "f"), "ttl-mput"} {
		if v, err := se.Get(k); err != nil || string(v) != "v" {
			t.Fatalf("before expiry Get(%q) = %q, %v", k, v, err)
		}
	}

	time.Sleep(1100 * time.Millisecond)

	if _, err := se.Get("ttl-get"); !errors.Is(err, ErrKeyExpired) && !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after expiry: err=%v, want ErrKeyExpired", err)
	}
	if v, needIO, err := se.GetNoIO("ttl-mput"); err == nil {
		t.Errorf("GetNoIO after expiry = %q (needIO=%v), want an error", v, needIO)
	}
	if _, err := se.GetNS("ns1", "ttl-ns"); err == nil {
		t.Error("GetNS after expiry returned the value")
	}
	if _, err := se.HGet("ttl-hash", "f"); err == nil {
		t.Error("HGet after expiry returned the value")
	}
	if fs, _ := se.HGetAll("ttl-hash"); len(fs) != 0 {
		t.Errorf("HGetAll after expiry = %v, want none", fs)
	}
	for _, r := range se.MultiGet([]string{"ttl-get", "ttl-mput"}) {
		if r.Found {
			t.Errorf("MultiGet(%q) after expiry: Found=true value=%q", r.Key, r.Value)
		}
	}
	// An overwrite without TTL must make the key readable again from cache.
	if err := se.Put("ttl-get", []byte("again"), -1); err != nil {
		t.Fatal(err)
	}
	if v, err := se.Get("ttl-get"); err != nil || string(v) != "again" {
		t.Errorf("Get after immortal overwrite = %q, %v", v, err)
	}
}

// TestMultiGet_EmptyValueAgreesWithGet: GET and MGET must agree on empty
// values whether written by Put or MultiPut (nil and zero-length slices).
func TestMultiGet_EmptyValueAgreesWithGet(t *testing.T) {
	t.Parallel()
	se := newTestEngine(t)

	if err := se.Put("empty-put", []byte{}, -1); err != nil {
		t.Fatal(err)
	}
	errs := se.MultiPut([]MultiPutRequest{
		{Key: "empty-mput-nil", Value: nil, TTL: -1},
		{Key: "empty-mput-zero", Value: []byte{}, TTL: -1},
		{Key: "nonempty-mput", Value: []byte("x"), TTL: -1},
	})
	for i, err := range errs {
		if err != nil {
			t.Fatalf("MultiPut[%d]: %v", i, err)
		}
	}
	keys := []string{"empty-put", "empty-mput-nil", "empty-mput-zero", "nonempty-mput"}
	// Twice: the first pass reads from disk / dirty state, the second from
	// whatever the first pass cached.
	for pass := 0; pass < 2; pass++ {
		res := se.MultiGet(keys)
		for i, k := range keys {
			v, err := se.Get(k)
			if err != nil {
				t.Fatalf("pass %d: Get(%q): %v", pass, k, err)
			}
			if !res[i].Found || res[i].Err != nil {
				t.Errorf("pass %d: MultiGet(%q) Found=%v Err=%v but Get returned %q", pass, k, res[i].Found, res[i].Err, v)
			}
			if string(res[i].Value) != string(v) {
				t.Errorf("pass %d: MultiGet(%q)=%q, Get=%q", pass, k, res[i].Value, v)
			}
			if res[i].Found && res[i].Value == nil {
				t.Errorf("pass %d: MultiGet(%q) Found with a nil Value; callers test Value==nil", pass, k)
			}
		}
	}
	// A missing key is still reported as not found.
	if r := se.MultiGet([]string{"never-written"})[0]; r.Found {
		t.Error("MultiGet of a missing key reported Found")
	}
}

// readAuditKeys polls path until want records are present (or timeout) and
// returns "op key" strings.
func readAuditKeys(t *testing.T, path string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var out []string
		if f, err := os.Open(path); err == nil {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				var rec AuditRecord
				if json.Unmarshal(sc.Bytes(), &rec) == nil {
					out = append(out, rec.Op+" "+rec.Key+" "+rec.Status)
				}
			}
			f.Close()
		}
		if len(out) >= want || time.Now().After(deadline) {
			return out
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestCDCAndAudit_CoverEveryWritePath: one CDC event and one audit record per
// key for MultiPut, TXN commit, CAS, INCR, DECR and SETNX — not only Put and
// Delete.
func TestCDCAndAudit_CoverEveryWritePath(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := testStorageConfig(dir)
	auditPath := filepath.Join(dir, "audit", "audit.jsonl")
	cfg.AuditLogPath = auditPath
	cfg.AuditSyncEvery = 10 * time.Millisecond
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { se.Close() })

	ch, cancel := se.Subscribe(1024, "")
	defer cancel()

	if errs := se.MultiPut([]MultiPutRequest{
		{Key: "mp-a", Value: []byte("1"), TTL: -1},
		{Key: "mp-b", Value: []byte("2"), TTL: -1},
		{Key: "mp-c", Value: []byte("3"), TTL: -1},
	}); errs[0] != nil || errs[1] != nil || errs[2] != nil {
		t.Fatalf("MultiPut: %v", errs)
	}
	txn := se.BeginTxn()
	txn.Set("txn-a", []byte("t"), -1)
	txn.Delete("mp-c")
	if err := txn.Commit(); err != nil {
		t.Fatalf("txn: %v", err)
	}
	if res, err := se.CompareAndSwap("mp-a", []byte("1"), []byte("1b"), -1); err != nil || res != CASSuccess {
		t.Fatalf("CAS: %v %v", res, err)
	}
	if res, err := se.CompareAndSwap("mp-a", []byte("nope"), []byte("x"), -1); err != nil || res != CASMismatch {
		t.Fatalf("CAS mismatch: %v %v", res, err)
	}
	if _, err := se.Increment("ctr", 5, -1); err != nil {
		t.Fatal(err)
	}
	if _, err := se.Decrement("ctr", 2, -1); err != nil {
		t.Fatal(err)
	}
	if res, err := se.SetIfNotExists("nx", []byte("n"), -1); err != nil || res != SetNXCreated {
		t.Fatalf("SETNX: %v %v", res, err)
	}
	if res, err := se.SetIfNotExists("nx", []byte("n2"), -1); err != nil || res != SetNXExists {
		t.Fatalf("SETNX exists: %v %v", res, err)
	}

	// Mutations that changed state: 3 MultiPut + txn-a + DEL mp-c + CAS +
	// INCR + DECR + SETNX = 9 CDC events. The CAS mismatch and the SETNX on
	// an existing key wrote nothing and must not appear in CDC.
	wantCDC := []string{
		"PUT mp-a 1", "PUT mp-b 2", "PUT mp-c 3",
		"PUT txn-a t", "DEL mp-c ",
		"PUT mp-a 1b",
		"PUT ctr 5", "PUT ctr 3",
		"PUT nx n",
	}
	evs := drainWithTimeout(ch, len(wantCDC), 2*time.Second)
	var got []string
	for _, ev := range evs {
		if ev.Timestamp == 0 {
			t.Errorf("CDC event %s %s has no timestamp", ev.Op, ev.Key)
		}
		got = append(got, ev.Op+" "+ev.Key+" "+string(ev.Value))
	}
	// MultiPut fans out per disk, so only the per-key order is fixed.
	sort.Strings(got)
	sort.Strings(wantCDC)
	if strings.Join(got, "|") != strings.Join(wantCDC, "|") {
		t.Errorf("CDC events:\n got  %v\n want %v", got, wantCDC)
	}
	if extra := drainWithTimeout(ch, 1, 100*time.Millisecond); len(extra) != 0 {
		t.Errorf("unexpected extra CDC events: %v", extra)
	}

	wantAudit := []string{
		"PUT mp-a ok", "PUT mp-b ok", "PUT mp-c ok",
		"PUT txn-a ok", "DEL mp-c ok",
		"CAS mp-a ok", "CAS mp-a mismatch",
		"INCR ctr ok", "DECR ctr ok",
		"SETNX nx ok", "SETNX nx exists",
	}
	gotAudit := readAuditKeys(t, auditPath, len(wantAudit))
	sort.Strings(gotAudit)
	sort.Strings(wantAudit)
	if strings.Join(gotAudit, "|") != strings.Join(wantAudit, "|") {
		t.Errorf("audit records:\n got  %v\n want %v", gotAudit, wantAudit)
	}
}

// TestCDC_EvictionRequiresConsecutiveDrops: a subscriber that occasionally
// falls behind but keeps draining must not be evicted; three drops in a row
// must evict it.
func TestCDC_EvictionRequiresConsecutiveDrops(t *testing.T) {
	t.Parallel()
	b := NewCDCBroker()
	ch, cancel := b.Subscribe(1, "")
	defer cancel()

	// Five rounds of: fill the 1-slot buffer, drop one event, drain. That is
	// five drops in total but never two in a row.
	for round := 0; round < 5; round++ {
		b.Broadcast(CDCEvent{Op: "PUT", Key: fmt.Sprintf("k%d", round)}) // queued
		b.Broadcast(CDCEvent{Op: "PUT", Key: "dropped"})                 // dropped
		if _, ok := <-ch; !ok {
			t.Fatalf("round %d: subscriber evicted after %d non-consecutive drops", round, round+1)
		}
	}
	if _, dropped, subs := b.Stats(); dropped != 5 || subs != 1 {
		t.Fatalf("dropped=%d subs=%d, want 5 and 1", dropped, subs)
	}

	// Now three consecutive drops: evicted, channel closed.
	b.Broadcast(CDCEvent{Op: "PUT", Key: "fill"})
	for i := 0; i < 3; i++ {
		b.Broadcast(CDCEvent{Op: "PUT", Key: "drop"})
	}
	if _, _, subs := b.Stats(); subs != 0 {
		t.Fatalf("subscriber not evicted after 3 consecutive drops (subs=%d)", subs)
	}
	<-ch // the buffered "fill" event
	if _, ok := <-ch; ok {
		t.Fatal("channel still open after eviction")
	}
}
