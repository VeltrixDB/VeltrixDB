package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VeltrixDB/veltrixdb/adminapi"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// ── fake server: the real adminapi handlers over a mock engine ────────────────

type mockEngine struct {
	cache  storage.CacheStats
	vlogs  []storage.VLogStats
	quotas []storage.QuotaSnapshot
	events []storage.CDCEvent
	m      *storage.StorageMetrics
	limits map[string]storage.QuotaLimit
	subPfx string
}

func newMockEngine() *mockEngine {
	m := &storage.StorageMetrics{}
	m.Writes, m.Reads, m.Deletes = &atomic.Uint64{}, &storage.StripedCounter{}, &atomic.Uint64{}
	m.AtomicOps, m.AuditDropped = &atomic.Uint64{}, &atomic.Uint64{}
	m.Writes.Store(1234)
	return &mockEngine{m: m, limits: map[string]storage.QuotaLimit{}}
}

func (e *mockEngine) GetIndexSize() int                                 { return 7 }
func (e *mockEngine) GetCacheStats() storage.CacheStats                 { return e.cache }
func (e *mockEngine) GetVLogStats() []storage.VLogStats                 { return e.vlogs }
func (e *mockEngine) GetMetrics() *storage.StorageMetrics               { return e.m }
func (e *mockEngine) GetWALTotals() (uint64, uint64)                    { return 4096, 10 }
func (e *mockEngine) Checkpoint() error                                 { return nil }
func (e *mockEngine) ListNamespaces() []storage.NSInfo                  { return nil }
func (e *mockEngine) QuotaStats() []storage.QuotaSnapshot               { return e.quotas }
func (e *mockEngine) MigrateAll() (int, int)                            { return 0, 0 }
func (e *mockEngine) CDCStats() (uint64, uint64, int)                   { return 3, 0, 1 }
func (e *mockEngine) SetNamespaceLimit(ns string, l storage.QuotaLimit) { e.limits[ns] = l }
func (e *mockEngine) ChangesSince(int64, int) storage.ChangesSinceResult {
	return storage.ChangesSinceResult{}
}

// Subscribe hands out the canned events then closes the channel, which makes
// the real /admin/cdc handler end the stream.
func (e *mockEngine) Subscribe(_ int, prefix string) (<-chan storage.CDCEvent, func()) {
	e.subPfx = prefix
	ch := make(chan storage.CDCEvent, len(e.events))
	for _, ev := range e.events {
		ch <- ev
	}
	close(ch)
	return ch, func() {}
}

// newServer mirrors cmd/server/main.go's health mux: /healthz → 200 "ok",
// /readyz → readyCode + readyBody, /admin/* behind adminapi.Guard(token).
func newServer(t *testing.T, e *mockEngine, token string, readyCode int, readyBody string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(readyCode)
		fmt.Fprintln(w, readyBody)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "veltrixdb_storage_wal_flushes_total{node=\"n1\"} 5")
	})
	mux.HandleFunc("/traces", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"service":"veltrixdb","name":"storage.Put","start_unix_ns":1700000000000000000,"duration_ns":60000000}`)
	})
	adminMux := http.NewServeMux()
	adminapi.Register(adminMux, e, "/admin", nil)
	adminMux.HandleFunc("/admin/cluster", func(w http.ResponseWriter, r *http.Request) {
		// Shape of cmd/server/admin_cluster.go topologyResponse.
		fmt.Fprint(w, `{"node_id":"n1","mode":"replicated","epoch":3,"partition_count":256,"consistency":"quorum",
			"replication":[{"node_id":"n2","state":"LAG","last_ack_seq":99,"lag_bytes":2048,"lag_ns":250000000}],
			"nodes":[{"node_id":"n1","address":"10.0.0.1","port":9000,"state":"active"}]}`)
	})
	mux.Handle("/admin/", adminapi.Guard(token, adminMux))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// capture runs the CLI with argv against srv and returns stdout, stderr, code.
func capture(t *testing.T, srv *httptest.Server, argv ...string) (string, string, int) {
	t.Helper()
	var o, e bytes.Buffer
	oldOut, oldErr, oldTok, oldColor := out, errOut, adminToken, useColor
	out, errOut, useColor = &o, &e, false
	defer func() { out, errOut, adminToken, useColor = oldOut, oldErr, oldTok, oldColor }()
	addr := strings.TrimPrefix(srv.URL, "http://")
	code := run(append([]string{"--addr", addr}, argv...))
	return o.String(), e.String(), code
}

// ── bug 1: status readiness ───────────────────────────────────────────────────

func TestStatusReadiness(t *testing.T) {
	cases := []struct {
		name      string
		code      int
		body      string
		wantReady bool
		wantText  string
	}{
		{"ready", 200, "ready", true, "READY"},
		{"initializing", 503, "initializing", false, "NOT READY (503: initializing)"},
		{"degraded", 503, "degraded: disks [1] failed", false, "NOT READY (503: degraded: disks [1] failed)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(t, newMockEngine(), "", tc.code, tc.body)
			stdout, stderr, code := capture(t, srv, "status")
			if code != 0 {
				t.Fatalf("exit %d, stderr=%s", code, stderr)
			}
			if !strings.Contains(stdout, "HEALTHY") || strings.Contains(stdout, "UNHEALTHY") {
				t.Errorf("health line wrong:\n%s", stdout)
			}
			if got := strings.Contains(stdout, "NOT READY"); got == tc.wantReady {
				t.Errorf("NOT READY present=%v, want ready=%v:\n%s", got, tc.wantReady, stdout)
			}
			if !strings.Contains(stdout, tc.wantText) {
				t.Errorf("missing %q:\n%s", tc.wantText, stdout)
			}
		})
	}
}

// ── bug 2: cdc-tail field names ───────────────────────────────────────────────

func TestCDCTailParsesServerEvents(t *testing.T) {
	e := newMockEngine()
	e.events = []storage.CDCEvent{
		{Op: "PUT", Key: "orders/1", Value: []byte("hello"), Timestamp: 1_700_000_000_123_456},
		{Op: "DEL", Key: "orders/1", Timestamp: 1_700_000_001_000_000},
	}
	srv := newServer(t, e, "", 200, "ready")
	stdout, stderr, code := capture(t, srv, "cdc-tail", "--prefix", "orders/")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if e.subPfx != "orders/" {
		t.Errorf("prefix sent = %q", e.subPfx)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), stdout)
	}
	if !strings.Contains(lines[0], "PUT") || !strings.Contains(lines[0], "orders/1") || !strings.Contains(lines[0], `(5 B) "hello"`) {
		t.Errorf("PUT line = %q", lines[0])
	}
	if !strings.Contains(lines[1], "DEL") || !strings.Contains(lines[1], "orders/1") || strings.Contains(lines[1], " B)") {
		t.Errorf("DEL line = %q", lines[1])
	}
	if strings.Contains(lines[0], "1970") || strings.HasPrefix(lines[0], "00:00:00") {
		t.Errorf("timestamp not decoded: %q", lines[0])
	}
}

func TestFormatCDCLine(t *testing.T) {
	cases := []struct {
		in     string
		ok     bool
		substr []string
	}{
		{`{"Op":"PUT","Key":"k","Value":"aGk=","Timestamp":1}`, true, []string{"PUT", "k", `"hi"`}},
		{`{"op":"put","key":"k2","value":"aGk=","timestamp":1}`, true, []string{"put", "k2", `"hi"`}}, // lowercase accepted
		{`{"Op":"DEL","Key":"gone","Value":null,"Timestamp":1}`, true, []string{"DEL", "gone"}},
		{`{"cursor":5,"more":false}`, false, nil},
		{`not json`, false, nil},
	}
	old := useColor
	useColor = false
	defer func() { useColor = old }()
	for _, tc := range cases {
		s, ok := formatCDCLine([]byte(tc.in))
		if ok != tc.ok {
			t.Errorf("%s: ok=%v", tc.in, ok)
		}
		for _, sub := range tc.substr {
			if !strings.Contains(s, sub) {
				t.Errorf("%s: %q missing %q", tc.in, s, sub)
			}
		}
	}
}

// ── bug 3: admin token ────────────────────────────────────────────────────────

func TestAdminToken(t *testing.T) {
	srv := newServer(t, newMockEngine(), "s3cret", 200, "ready")

	_, stderr, code := capture(t, srv, "version")
	if code != 1 || !strings.Contains(stderr, "HTTP 401") || !strings.Contains(stderr, "--admin-token") {
		t.Errorf("no token: code=%d stderr=%s", code, stderr)
	}
	_, stderr, code = capture(t, srv, "version", "--admin-token", "wrong")
	if code != 1 || !strings.Contains(stderr, "rejected") {
		t.Errorf("wrong token: code=%d stderr=%s", code, stderr)
	}
	stdout, stderr, code := capture(t, srv, "--admin-token", "s3cret", "version")
	if code != 0 || !strings.Contains(stdout, "Schema version") {
		t.Errorf("good token: code=%d out=%s err=%s", code, stdout, stderr)
	}

	t.Setenv("VELTRIX_ADMIN_TOKEN", "s3cret")
	stdout, _, code = capture(t, srv, "status")
	if code != 0 || strings.Contains(stdout, "NOT READY") {
		t.Errorf("env token status: code=%d\n%s", code, stdout)
	}
	e := newMockEngine()
	e.events = []storage.CDCEvent{{Op: "PUT", Key: "a", Value: []byte("1"), Timestamp: 1}}
	srv2 := newServer(t, e, "s3cret", 200, "ready")
	stdout, _, code = capture(t, srv2, "cdc-tail")
	if code != 0 || !strings.Contains(stdout, "PUT") {
		t.Errorf("env token cdc-tail: code=%d %s", code, stdout)
	}
}

// ── bug 4: flags after the command ────────────────────────────────────────────

func TestParseArgs(t *testing.T) {
	cases := []struct {
		argv   []string
		watch  int
		json   bool
		prefix string
		token  string
		pos    []string
	}{
		{[]string{"top", "--watch", "2"}, 2, false, "", "", []string{"top"}},
		{[]string{"--watch", "2", "top"}, 2, false, "", "", []string{"top"}},
		{[]string{"compaction", "--watch=5", "--json"}, 5, true, "", "", []string{"compaction"}},
		{[]string{"metrics", "vlog_gc", "--json"}, 0, true, "", "", []string{"metrics", "vlog_gc"}},
		{[]string{"cdc-tail", "--prefix=orders/"}, 0, false, "orders/", "", []string{"cdc-tail"}},
		{[]string{"put", "k", "--admin-token", "t", "v"}, 0, false, "", "t", []string{"put", "k", "v"}},
		{[]string{"put", "k", "--", "-1", "--json"}, 0, false, "", "", []string{"put", "k", "-1", "--json"}},
	}
	t.Setenv("VELTRIX_ADMIN_TOKEN", "")
	for _, tc := range cases {
		c, pos, err := parseArgs(tc.argv, &bytes.Buffer{})
		if err != nil {
			t.Errorf("%v: %v", tc.argv, err)
			continue
		}
		if c.watchSec != tc.watch || c.rawJSON != tc.json || c.prefix != tc.prefix || c.token != tc.token ||
			strings.Join(pos, "|") != strings.Join(tc.pos, "|") {
			t.Errorf("%v: got watch=%d json=%v prefix=%q token=%q pos=%v", tc.argv, c.watchSec, c.rawJSON, c.prefix, c.token, pos)
		}
	}
	if _, _, err := parseArgs([]string{"top", "--bogus"}, &bytes.Buffer{}); err == nil {
		t.Error("unknown flag after command should error")
	}
	t.Setenv("VELTRIX_ADMIN_TOKEN", "fromenv")
	if c, _, _ := parseArgs([]string{"status"}, &bytes.Buffer{}); c.token != "fromenv" {
		t.Errorf("env token = %q", c.token)
	}
}

// ── bug 6: JSON field names from the real admin API ──────────────────────────

func TestStatsFieldNames(t *testing.T) {
	e := newMockEngine()
	e.cache = storage.CacheStats{Hits: 90, Misses: 10, Evictions: 4, CurrentSizeBytes: 2 << 20, MaxSizeBytes: 4 << 20}
	e.vlogs = []storage.VLogStats{{DiskIdx: 3, FileBytes: 10 << 20, LiveBytes: 4 << 20, GarbageRatio: 0.55}}
	srv := newServer(t, e, "", 200, "ready")

	stdout, _, _ := capture(t, srv, "cache")
	for _, want := range []string{"2.0 MiB / 4.0 MiB", "90.0%", "90 hits / 10 misses", "Evictions:           4"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("cache missing %q:\n%s", want, stdout)
		}
	}
	stdout, _, _ = capture(t, srv, "compaction")
	for _, want := range []string{"10.0 MiB", "4.0 MiB", "6.0 MiB", "CRITICAL 55.0%"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("compaction missing %q:\n%s", want, stdout)
		}
	}
	if !strings.Contains(stdout, "\n3 ") {
		t.Errorf("compaction should label disk by DiskIdx 3:\n%s", stdout)
	}
	stdout, _, _ = capture(t, srv, "status")
	for _, want := range []string{"Writes total:        1,234", "2.0 MiB / 4.0 MiB", "CRITICAL 55.0%", "Flushes:             5"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("status missing %q:\n%s", want, stdout)
		}
	}
}

func TestQuotas(t *testing.T) {
	e := newMockEngine()
	e.quotas = []storage.QuotaSnapshot{
		{Namespace: "tenant_42", WritesPerSec: 1000, BurstWrites: 1000, MaxKeys: 5000000, KeyCount: 12, TokensLeft: 0.2},
		{Namespace: "open", KeyCount: 3},
	}
	srv := newServer(t, e, "", 200, "ready")
	stdout, stderr, code := capture(t, srv, "quotas")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	for _, want := range []string{"tenant_42", "1000", "5,000,000", "throttled", "open", "unlimited"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("quotas missing %q:\n%s", want, stdout)
		}
	}
}

func TestReplicationUsesClusterEndpoint(t *testing.T) {
	srv := newServer(t, newMockEngine(), "", 200, "ready")
	stdout, stderr, code := capture(t, srv, "replication")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	for _, want := range []string{"n2", "LAG", "quorum", "250.0ms", "2.0 KiB", "99"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("replication missing %q:\n%s", want, stdout)
		}
	}
}

func TestTracesTimestamp(t *testing.T) {
	srv := newServer(t, newMockEngine(), "", 200, "ready")
	stdout, _, _ := capture(t, srv, "traces")
	if !strings.Contains(stdout, "storage.Put") || !strings.Contains(stdout, "60.0ms") {
		t.Errorf("traces:\n%s", stdout)
	}
}
