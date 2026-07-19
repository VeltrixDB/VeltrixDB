package main

// server_test.go — end-to-end tests for the extended wire surface added in
// Phase B1: RANGE / SCANCUR, TXN, IDXCREATE / IDXQUERY / IDXDROP, VSET /
// VSEARCH, QUERY, and RBAC enforcement. Each test spins the real serve loop
// (handleConn) on a random port with a temp data dir and talks to it through
// the real client package — both the text (client.TCPConn) and binary
// (client.BinaryConn) protocols.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
	"github.com/VeltrixDB/veltrixdb/security"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// testServer wraps a live engine + TCP accept loop.
type testServer struct {
	engine *storage.StorageEngine
	ln     net.Listener
	addr   string
}

// startTestServer boots a storage engine on dataDir and serves handleConn on
// a random 127.0.0.1 port. ae == nil → RBAC disabled (all ops permitted).
// It mirrors cmd/server startup: waits for WAL replay, then rebuilds the
// vector indexes from persisted "@vec/..." keys.
func startTestServer(t *testing.T, dataDir string, ae *security.AuthEnforcer) *testServer {
	t.Helper()
	cfg := storage.DefaultStorageConfig()
	cfg.DataDirPath = dataDir
	// Short flush windows keep per-Put latency low on macOS F_FULLFSYNC.
	cfg.WALFlushWindowMs = 2
	cfg.VLogFlushWindowMs = 2
	engine, err := storage.NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("storage init: %v", err)
	}
	<-engine.ReplayDone
	if _, err := engine.RebuildVectorIndexes(); err != nil {
		t.Fatalf("vector rebuild: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		engine.Close()
		t.Fatalf("listen: %v", err)
	}
	if ae == nil {
		ae = security.NewAuthEnforcer()
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleConn(conn, engine, ae, newStandaloneCoordinator(engine))
		}
	}()
	return &testServer{engine: engine, ln: ln, addr: ln.Addr().String()}
}

func (ts *testServer) stop(t *testing.T) {
	t.Helper()
	ts.ln.Close()
	if err := ts.engine.Close(); err != nil {
		t.Logf("engine close: %v", err)
	}
}

func dialText(t *testing.T, addr string) *client.TCPConn {
	t.Helper()
	tc, err := client.DialTCP(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial text %s: %v", addr, err)
	}
	return tc
}

func dialBinary(t *testing.T, addr string) *client.BinaryConn {
	t.Helper()
	bc, err := client.DialBinary(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial binary %s: %v", addr, err)
	}
	return bc
}

// ── RANGE + SCANCUR, including restart persistence ───────────────────────────

func TestRangeScanAndCursor(t *testing.T) {
	dir := t.TempDir()
	ts := startTestServer(t, dir, nil)

	bc := dialBinary(t, ts.addr)
	entries := make([]client.MPutEntry, 0, 10)
	for i := 0; i < 10; i++ {
		entries = append(entries, client.MPutEntry{
			Key: fmt.Sprintf("rng/%02d", i), Value: []byte(fmt.Sprintf("v%d", i)), TTL: -1,
		})
	}
	if _, err := bc.MPut(entries); err != nil {
		t.Fatalf("mput: %v", err)
	}

	// Binary RANGE: [rng/02, rng/06) ascending.
	kvs, err := bc.RangeScan("rng/02", "rng/06", 0, false)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(kvs) != 4 || kvs[0].Key != "rng/02" || kvs[3].Key != "rng/05" {
		t.Fatalf("range: unexpected result %+v", kvs)
	}

	// Binary RANGE reverse with limit.
	kvs, err = bc.RangeScan("rng/00", "rng/09", 3, true)
	if err != nil {
		t.Fatalf("range rev: %v", err)
	}
	if len(kvs) != 3 || kvs[0].Key != "rng/08" || kvs[2].Key != "rng/06" {
		t.Fatalf("range rev: unexpected result %+v", kvs)
	}

	// Binary SCANCUR pagination: walk everything in pages of 4.
	var all []client.KV
	cursor := ""
	for pages := 0; ; pages++ {
		kvs, next, err := bc.ScanCursor(cursor, 4)
		if err != nil {
			t.Fatalf("scancur: %v", err)
		}
		all = append(all, kvs...)
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatalf("scancur: runaway pagination")
		}
	}
	if len(all) != 10 {
		t.Fatalf("scancur: want 10 keys, got %d", len(all))
	}

	// Text protocol RANGE + SCANCUR.
	tc := dialText(t, ts.addr)
	tkvs, err := tc.RangeScan("rng/03", "rng/05", 0, false)
	if err != nil {
		t.Fatalf("text range: %v", err)
	}
	if len(tkvs) != 2 || tkvs[0].Key != "rng/03" || string(tkvs[0].Value) != "v3" {
		t.Fatalf("text range: unexpected result %+v", tkvs)
	}
	tkvs, next, err := tc.ScanCursor("", 5)
	if err != nil {
		t.Fatalf("text scancur: %v", err)
	}
	if len(tkvs) != 5 || next == "" {
		t.Fatalf("text scancur: want 5 keys + cursor, got %d %q", len(tkvs), next)
	}
	tc.Close()
	bc.Close()

	// Restart the server on the same data dir: the ordered index is rebuilt
	// from the checkpointed WAL, so RANGE must still work.
	ts.stop(t)
	ts = startTestServer(t, dir, nil)
	defer ts.stop(t)

	bc = dialBinary(t, ts.addr)
	defer bc.Close()
	kvs, err = bc.RangeScan("rng/02", "rng/06", 0, false)
	if err != nil {
		t.Fatalf("range after restart: %v", err)
	}
	if len(kvs) != 4 || kvs[0].Key != "rng/02" || string(kvs[0].Value) != "v2" {
		t.Fatalf("range after restart: unexpected result %+v", kvs)
	}
	if _, _, err := bc.ScanCursor("", 4); err != nil {
		t.Fatalf("scancur after restart: %v", err)
	}
}

// ── TXN commit + conflict ─────────────────────────────────────────────────────

func TestTxnCommitAndConflict(t *testing.T) {
	ts := startTestServer(t, t.TempDir(), nil)
	defer ts.stop(t)

	bc := dialBinary(t, ts.addr)
	defer bc.Close()

	// Commit: SETIF with expected version 0 on fresh keys + unconditional SET.
	err := bc.Txn([]client.TxnOp{
		{Op: "SETIF", Key: "txn/a", Value: []byte("1"), ExpectedVersion: 0},
		{Op: "SET", Key: "txn/b", Value: []byte("2")},
	})
	if err != nil {
		t.Fatalf("txn commit: %v", err)
	}
	if v, _ := bc.Get("txn/a"); string(v) != "1" {
		t.Fatalf("txn/a = %q, want 1", v)
	}
	if v, _ := bc.Get("txn/b"); string(v) != "2" {
		t.Fatalf("txn/b = %q, want 2", v)
	}

	// Read the committed version, then simulate a concurrent writer.
	ver, err := bc.KeyVersion("txn/a")
	if err != nil || ver == 0 {
		t.Fatalf("keyversion: %d %v", ver, err)
	}
	if err := bc.Put("txn/a", []byte("interloper"), -1); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Conflict: the stale version must abort the whole batch.
	err = bc.Txn([]client.TxnOp{
		{Op: "SETIF", Key: "txn/a", Value: []byte("3"), ExpectedVersion: ver},
		{Op: "SET", Key: "txn/c", Value: []byte("4")},
	})
	if !errors.Is(err, client.ErrTxnConflict) {
		t.Fatalf("txn: want ErrTxnConflict, got %v", err)
	}
	if v, _ := bc.Get("txn/c"); v != nil {
		t.Fatalf("txn/c must not exist after aborted txn, got %q", v)
	}

	// Retry with the fresh version succeeds.
	ver, _ = bc.KeyVersion("txn/a")
	if err := bc.Txn([]client.TxnOp{
		{Op: "SETIF", Key: "txn/a", Value: []byte("3"), ExpectedVersion: ver},
		{Op: "DEL", Key: "txn/b"},
	}); err != nil {
		t.Fatalf("txn retry: %v", err)
	}
	if v, _ := bc.Get("txn/a"); string(v) != "3" {
		t.Fatalf("txn/a = %q, want 3", v)
	}
	if v, _ := bc.Get("txn/b"); v != nil {
		t.Fatalf("txn/b must be deleted, got %q", v)
	}

	// Text-protocol TXN: conflict via SETIF 0 on an existing key.
	tc := dialText(t, ts.addr)
	defer tc.Close()
	err = tc.Txn([]client.TxnOp{
		{Op: "SETIF", Key: "txn/a", Value: []byte("x"), ExpectedVersion: 0},
	})
	if !errors.Is(err, client.ErrTxnConflict) {
		t.Fatalf("text txn: want ErrTxnConflict, got %v", err)
	}
}

// ── IDXCREATE / IDXQUERY roundtrip + persistence across restart ──────────────

func TestSecondaryIndexRoundtripAndPersistence(t *testing.T) {
	dir := t.TempDir()
	ts := startTestServer(t, dir, nil)

	bc := dialBinary(t, ts.addr)

	// Data written BEFORE the index exists → covered by backfill.
	if err := bc.Put("user/1", []byte(`{"city":"berlin","age":30}`), -1); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := bc.IdxCreate("city", "city"); err != nil {
		t.Fatalf("idxcreate: %v", err)
	}
	// Data written AFTER the index exists → covered by the Put hot path.
	if err := bc.Put("user/2", []byte(`{"city":"berlin","age":40}`), -1); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := bc.Put("user/3", []byte(`{"city":"madrid","age":50}`), -1); err != nil {
		t.Fatalf("put: %v", err)
	}

	keys, err := bc.IdxQuery("city", "berlin", 0)
	if err != nil {
		t.Fatalf("idxquery: %v", err)
	}
	if !containsAll(keys, "user/1", "user/2") || containsAll(keys, "user/3") {
		t.Fatalf("idxquery berlin: got %v", keys)
	}

	// Update moves the key between index values.
	if err := bc.Put("user/1", []byte(`{"city":"madrid","age":30}`), -1); err != nil {
		t.Fatalf("put update: %v", err)
	}
	keys, _ = bc.IdxQuery("city", "berlin", 0)
	if containsAll(keys, "user/1") {
		t.Fatalf("idxquery berlin after update still has user/1: %v", keys)
	}
	keys, _ = bc.IdxQuery("city", "madrid", 0)
	if !containsAll(keys, "user/1", "user/3") {
		t.Fatalf("idxquery madrid: got %v", keys)
	}
	bc.Close()

	// The definition must be persisted to index_defs.json.
	data, err := os.ReadFile(filepath.Join(dir, "index_defs.json"))
	if err != nil {
		t.Fatalf("index_defs.json: %v", err)
	}
	var defs []storage.FieldIndexDef
	if err := json.Unmarshal(data, &defs); err != nil {
		t.Fatalf("index_defs.json parse: %v", err)
	}
	if len(defs) != 1 || defs[0].Name != "city" || defs[0].Field != "city" {
		t.Fatalf("index_defs.json content: %+v", defs)
	}

	// Restart: index definition and entries must survive, and NEW writes
	// must keep maintaining the index.
	ts.stop(t)
	ts = startTestServer(t, dir, nil)
	defer ts.stop(t)

	bc = dialBinary(t, ts.addr)
	defer bc.Close()
	keys, err = bc.IdxQuery("city", "madrid", 0)
	if err != nil {
		t.Fatalf("idxquery after restart: %v", err)
	}
	if !containsAll(keys, "user/1", "user/3") {
		t.Fatalf("idxquery madrid after restart: got %v", keys)
	}
	if err := bc.Put("user/4", []byte(`{"city":"madrid","age":60}`), -1); err != nil {
		t.Fatalf("put after restart: %v", err)
	}
	keys, _ = bc.IdxQuery("city", "madrid", 0)
	if !containsAll(keys, "user/4") {
		t.Fatalf("index not maintained after restart: %v", keys)
	}

	// IDXDROP removes the definition and entries.
	if err := bc.IdxDrop("city"); err != nil {
		t.Fatalf("idxdrop: %v", err)
	}
	keys, _ = bc.IdxQuery("city", "madrid", 0)
	if len(keys) != 0 {
		t.Fatalf("idxquery after drop: got %v", keys)
	}
}

// ── VSET / VSEARCH roundtrip (+ persistence across restart) ──────────────────

func TestVectorRoundtrip(t *testing.T) {
	dir := t.TempDir()
	ts := startTestServer(t, dir, nil)

	bc := dialBinary(t, ts.addr)
	vecs := map[string][]float32{
		"east":  {1, 0, 0},
		"north": {0, 1, 0},
		"upish": {0.1, 0.1, 1},
	}
	for id, v := range vecs {
		if err := bc.VSet(id, v); err != nil {
			t.Fatalf("vset %s: %v", id, err)
		}
	}
	matches, err := bc.VSearch(2, []float32{0.9, 0.1, 0})
	if err != nil {
		t.Fatalf("vsearch: %v", err)
	}
	if len(matches) != 2 || matches[0].ID != "east" {
		t.Fatalf("vsearch: unexpected result %+v", matches)
	}
	if matches[0].Score < 0.9 {
		t.Fatalf("vsearch: east score %f too low", matches[0].Score)
	}

	// Dimension mismatch must be a clean protocol error.
	if err := bc.VSet("bad", []float32{1, 2}); err == nil {
		t.Fatalf("vset with wrong dim must fail")
	}
	bc.Close()

	// Text protocol VSEARCH against the same index.
	tc := dialText(t, ts.addr)
	tm, err := tc.VSearch(1, []float32{0, 1, 0})
	if err != nil {
		t.Fatalf("text vsearch: %v", err)
	}
	if len(tm) != 1 || tm[0].ID != "north" {
		t.Fatalf("text vsearch: %+v", tm)
	}
	tc.Close()

	// Restart: vectors must be rebuilt from the persisted "@vec/..." keys.
	ts.stop(t)
	ts = startTestServer(t, dir, nil)
	defer ts.stop(t)

	bc = dialBinary(t, ts.addr)
	defer bc.Close()
	matches, err = bc.VSearch(1, []float32{1, 0, 0})
	if err != nil {
		t.Fatalf("vsearch after restart: %v", err)
	}
	if len(matches) != 1 || matches[0].ID != "east" {
		t.Fatalf("vsearch after restart: %+v", matches)
	}
}

// ── QUERY with and without a matching index ──────────────────────────────────

func TestQueryLanguage(t *testing.T) {
	ts := startTestServer(t, t.TempDir(), nil)
	defer ts.stop(t)

	tc := dialText(t, ts.addr)
	defer tc.Close()

	// Populate namespace "users" via NSPUT (text protocol).
	users := map[string]string{
		"u1": `{"name":"ana","age":30,"city":"berlin"}`,
		"u2": `{"name":"bob","age":45,"city":"madrid"}`,
		"u3": `{"name":"cyn","age":22,"city":"berlin"}`,
	}
	for k, v := range users {
		nsput(t, ts.addr, "users", k, v)
	}

	// No index → namespace scan path.
	kvs, err := tc.Query("users", "age", ">", "25", 0)
	if err != nil {
		t.Fatalf("query >: %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("query age>25: want 2, got %+v", kvs)
	}
	kvs, err = tc.Query("users", "city", "=", "berlin", 0)
	if err != nil {
		t.Fatalf("query = (scan): %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("query city=berlin (scan): want 2, got %+v", kvs)
	}
	kvs, err = tc.Query("users", "name", "contains", "o", 0)
	if err != nil || len(kvs) != 1 || kvs[0].Key != "u2" {
		t.Fatalf("query contains: %+v %v", kvs, err)
	}

	// Create an index on city → equality queries take the index path and
	// must return identical results.
	if err := tc.IdxCreate("city-idx", "city"); err != nil {
		t.Fatalf("idxcreate: %v", err)
	}
	kvs, err = tc.Query("users", "city", "=", "berlin", 0)
	if err != nil {
		t.Fatalf("query = (indexed): %v", err)
	}
	if len(kvs) != 2 {
		t.Fatalf("query city=berlin (indexed): want 2, got %+v", kvs)
	}
	// LIMIT applies on the indexed path too.
	kvs, err = tc.Query("users", "city", "=", "berlin", 1)
	if err != nil || len(kvs) != 1 {
		t.Fatalf("query indexed limit: %+v %v", kvs, err)
	}

	// Strict parser: garbage must produce a usage error.
	if _, err := tc.Query("users", "city", "LIKE", "berlin", 0); err == nil {
		t.Fatalf("query with bad op must fail")
	}

	// Binary QUERY (0x28) returns the same rows.
	bc := dialBinary(t, ts.addr)
	defer bc.Close()
	bkvs, err := bc.Query("users", "age", "<=", "30", 0)
	if err != nil {
		t.Fatalf("binary query: %v", err)
	}
	if len(bkvs) != 2 {
		t.Fatalf("binary query age<=30: want 2, got %+v", bkvs)
	}
}

// nsput issues one raw NSPUT text command over a throwaway connection (the
// text client package has no namespace helper).
func nsput(t *testing.T, addr, ns, key, value string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("nsput dial: %v", err)
	}
	defer c.Close()
	fmt.Fprintf(c, "NSPUT %s %s %s\n", ns, key, value)
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("nsput read: %v", err)
	}
	if string(buf[:n]) != "OK\n" {
		t.Fatalf("nsput %s/%s: %q", ns, key, buf[:n])
	}
}

// ── RBAC: readonly user rejected on TXN ──────────────────────────────────────

func TestRBACReadonlyDeniedTxn(t *testing.T) {
	dir := t.TempDir()

	// Auth config: one readonly user, one admin.
	authCfg := fmt.Sprintf(`{"users":[
		{"username":"ro","password_hash":"%s","role":"readonly"},
		{"username":"root","password_hash":"%s","role":"admin"}
	]}`, security.HashPassword("ropass", "ro"), security.HashPassword("rootpass", "root"))
	authPath := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(authPath, []byte(authCfg), 0o600); err != nil {
		t.Fatalf("write auth config: %v", err)
	}
	ae := security.NewAuthEnforcer()
	if err := ae.LoadConfig(authPath); err != nil {
		t.Fatalf("auth config: %v", err)
	}

	ts := startTestServer(t, filepath.Join(dir, "data"), ae)
	defer ts.stop(t)

	// Admin seeds a key.
	tcAdmin := dialText(t, ts.addr)
	defer tcAdmin.Close()
	if err := tcAdmin.Auth("root", "rootpass"); err != nil {
		t.Fatalf("admin auth: %v", err)
	}
	if err := tcAdmin.Put("rbac/k", []byte("v")); err != nil {
		t.Fatalf("admin put: %v", err)
	}

	// Readonly user: reads OK, TXN (PermWrite) denied — text protocol.
	tcRO := dialText(t, ts.addr)
	defer tcRO.Close()
	if err := tcRO.Auth("ro", "ropass"); err != nil {
		t.Fatalf("ro auth: %v", err)
	}
	if v, err := tcRO.Get("rbac/k"); err != nil || string(v) != "v" {
		t.Fatalf("ro get: %q %v", v, err)
	}
	err := tcRO.Txn([]client.TxnOp{{Op: "SET", Key: "rbac/x", Value: []byte("1")}})
	if err == nil || errors.Is(err, client.ErrTxnConflict) {
		t.Fatalf("readonly TXN must be denied, got %v", err)
	}

	// Binary protocol: TXN from a readonly connection is denied too.
	bcRO := dialBinary(t, ts.addr)
	defer bcRO.Close()
	if err := bcRO.Auth("ro", "ropass"); err != nil {
		t.Fatalf("ro binary auth: %v", err)
	}
	err = bcRO.Txn([]client.TxnOp{{Op: "SET", Key: "rbac/y", Value: []byte("1")}})
	if err == nil || errors.Is(err, client.ErrTxnConflict) {
		t.Fatalf("readonly binary TXN must be denied, got %v", err)
	}
	// The write must not have landed.
	if v, err := tcAdmin.Get("rbac/x"); err == nil && v != nil {
		t.Fatalf("rbac/x must not exist, got %q", v)
	}
}

// containsAll reports whether haystack contains every needle.
func containsAll(haystack []string, needles ...string) bool {
	set := make(map[string]bool, len(haystack))
	for _, h := range haystack {
		set[h] = true
	}
	for _, n := range needles {
		if !set[n] {
			return false
		}
	}
	return true
}
