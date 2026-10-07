package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/VeltrixDB/veltrixdb/adminapi"
	"github.com/VeltrixDB/veltrixdb/storage"
)

// mockEngine backs the real adminapi handlers; only quotas and CDC matter here.
type mockEngine struct {
	limits map[string]storage.QuotaLimit
	events []storage.CDCEvent
	subPfx string
	m      *storage.StorageMetrics
}

func newMockEngine() *mockEngine {
	m := &storage.StorageMetrics{}
	m.Writes, m.Reads, m.Deletes = &atomic.Uint64{}, &storage.StripedCounter{}, &atomic.Uint64{}
	m.AtomicOps, m.AuditDropped = &atomic.Uint64{}, &atomic.Uint64{}
	return &mockEngine{limits: map[string]storage.QuotaLimit{}, m: m}
}

func (e *mockEngine) GetIndexSize() int                                 { return 1 }
func (e *mockEngine) GetCacheStats() storage.CacheStats                 { return storage.CacheStats{} }
func (e *mockEngine) GetVLogStats() []storage.VLogStats                 { return nil }
func (e *mockEngine) GetMetrics() *storage.StorageMetrics               { return e.m }
func (e *mockEngine) GetWALTotals() (uint64, uint64)                    { return 0, 0 }
func (e *mockEngine) Checkpoint() error                                 { return nil }
func (e *mockEngine) ListNamespaces() []storage.NSInfo                  { return nil }
func (e *mockEngine) MigrateAll() (int, int)                            { return 0, 0 }
func (e *mockEngine) CDCStats() (uint64, uint64, int)                   { return 0, 0, 0 }
func (e *mockEngine) SetNamespaceLimit(ns string, l storage.QuotaLimit) { e.limits[ns] = l }
func (e *mockEngine) ChangesSince(int64, int) storage.ChangesSinceResult {
	return storage.ChangesSinceResult{}
}
func (e *mockEngine) QuotaStats() []storage.QuotaSnapshot {
	out := []storage.QuotaSnapshot{}
	for ns, l := range e.limits {
		out = append(out, storage.QuotaSnapshot{Namespace: ns, WritesPerSec: l.WritesPerSec, BurstWrites: l.BurstWrites, MaxKeys: l.MaxKeys})
	}
	return out
}
func (e *mockEngine) Subscribe(_ int, prefix string) (<-chan storage.CDCEvent, func()) {
	e.subPfx = prefix
	ch := make(chan storage.CDCEvent, len(e.events))
	for _, ev := range e.events {
		ch <- ev
	}
	close(ch)
	return ch, func() {}
}

func newServer(t *testing.T, e *mockEngine, token string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "veltrixdb_vlog_gc_runs_total{node=\"n1\"} 4\nveltrixdb_storage_writes_total{node=\"n1\"} 9")
	})
	adminMux := http.NewServeMux()
	adminapi.Register(adminMux, e, "/admin", nil)
	mux.Handle("/admin/", adminapi.Guard(token, adminMux))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// capture runs the plugin with discovery stubbed to srv.
func capture(t *testing.T, srv *httptest.Server, argv ...string) (string, string, int) {
	t.Helper()
	var o, e bytes.Buffer
	oldOut, oldErr, oldDisc := out, errOut, discover
	out, errOut = &o, &e
	discover = func(opts) (string, func(), error) { return srv.URL, func() {}, nil }
	defer func() { out, errOut, discover = oldOut, oldErr, oldDisc }()
	code := run(argv)
	return o.String(), e.String(), code
}

func TestQuotaSetBurst(t *testing.T) {
	cases := []struct {
		name      string
		argv      []string
		wantLimit storage.QuotaLimit
	}{
		{"default burst = WPS", []string{"quota-set", "tenant_42", "1000", "5000000"},
			storage.QuotaLimit{WritesPerSec: 1000, BurstWrites: 1000, MaxKeys: 5000000}},
		{"explicit burst after args", []string{"quota-set", "tenant_42", "1000", "5000000", "--burst", "4000"},
			storage.QuotaLimit{WritesPerSec: 1000, BurstWrites: 4000, MaxKeys: 5000000}},
		{"flags first", []string{"--burst=50", "quota-set", "t", "10", "0"},
			storage.QuotaLimit{WritesPerSec: 10, BurstWrites: 50}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newMockEngine()
			srv := newServer(t, e, "")
			stdout, stderr, code := capture(t, srv, tc.argv...)
			if code != 0 {
				t.Fatalf("exit %d: %s", code, stderr)
			}
			ns := tc.argv[len(tc.argv)-3]
			if tc.name == "explicit burst after args" {
				ns = "tenant_42"
			}
			if got := e.limits[ns]; got != tc.wantLimit {
				t.Errorf("limit = %+v, want %+v", got, tc.wantLimit)
			}
			if !strings.Contains(stdout, `"status": "ok"`) {
				t.Errorf("response: %s", stdout)
			}
		})
	}
}

func TestQuotaSetValidationAndEscaping(t *testing.T) {
	e := newMockEngine()
	srv := newServer(t, e, "")
	if _, _, code := capture(t, srv, "quota-set", "ns", "abc", "1"); code != 1 {
		t.Errorf("bad WPS accepted (code %d)", code)
	}
	if _, _, code := capture(t, srv, "quota-set", "ns", "1"); code != 2 {
		t.Errorf("missing args code %d", code)
	}
	if _, stderr, code := capture(t, srv, "quota-set", "a&max_keys=9", "5", "7"); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if l, ok := e.limits["a&max_keys=9"]; !ok || l.MaxKeys != 7 {
		t.Errorf("namespace not form-escaped: %+v", e.limits)
	}
}

func TestAdminToken(t *testing.T) {
	e := newMockEngine()
	srv := newServer(t, e, "s3cret")
	_, stderr, code := capture(t, srv, "quotas")
	if code != 1 || !strings.Contains(stderr, "HTTP 401") || !strings.Contains(stderr, "--admin-token") {
		t.Errorf("no token: code=%d %s", code, stderr)
	}
	stdout, stderr, code := capture(t, srv, "quotas", "--admin-token", "s3cret")
	if code != 0 || strings.TrimSpace(stdout) != "[]" {
		t.Errorf("flag token: code=%d out=%q err=%s", code, stdout, stderr)
	}
	t.Setenv("VELTRIX_ADMIN_TOKEN", "s3cret")
	if _, stderr, code := capture(t, srv, "quota-set", "x", "1", "2"); code != 0 {
		t.Errorf("env token: code=%d %s", code, stderr)
	}
	if _, stderr, code := capture(t, srv, "checkpoint"); code != 0 {
		t.Errorf("env token checkpoint: code=%d %s", code, stderr)
	}
}

func TestCDCTailAndGCStatus(t *testing.T) {
	e := newMockEngine()
	e.events = []storage.CDCEvent{{Op: "PUT", Key: "orders/1", Value: []byte("v"), Timestamp: 5}}
	srv := newServer(t, e, "")
	stdout, stderr, code := capture(t, srv, "cdc-tail", "--prefix", "orders/")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if e.subPfx != "orders/" {
		t.Errorf("prefix = %q", e.subPfx)
	}
	var ev struct{ Op, Key string }
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &ev); err != nil || ev.Op != "PUT" || ev.Key != "orders/1" {
		t.Errorf("cdc line %q (%v)", stdout, err)
	}

	stdout, _, _ = capture(t, srv, "gc-status")
	if !strings.Contains(stdout, "vlog_gc_runs_total") || strings.Contains(stdout, "writes_total") {
		t.Errorf("gc-status:\n%s", stdout)
	}
}

func TestParseArgs(t *testing.T) {
	t.Setenv("VELTRIX_ADMIN_TOKEN", "")
	cases := []struct {
		argv   []string
		ns     string
		prefix string
		pos    string
	}{
		{[]string{"cdc-tail", "--prefix=orders/"}, "veltrixdb", "orders/", "cdc-tail"},
		{[]string{"-n", "prod", "stats"}, "prod", "", "stats"},
		{[]string{"stats", "-n", "prod"}, "prod", "", "stats"},
		{[]string{"quota-set", "a", "--", "-1", "2"}, "veltrixdb", "", "quota-set a -1 2"},
	}
	for _, tc := range cases {
		o, pos, err := parseArgs(tc.argv, &bytes.Buffer{})
		if err != nil || o.namespace != tc.ns || o.prefix != tc.prefix || strings.Join(pos, " ") != tc.pos {
			t.Errorf("%v: o=%+v pos=%v err=%v", tc.argv, o, pos, err)
		}
	}
}

func TestUnknownCommandSkipsDiscovery(t *testing.T) {
	called := false
	oldDisc, oldErr := discover, errOut
	discover = func(opts) (string, func(), error) { called = true; return "", func() {}, nil }
	errOut = &bytes.Buffer{}
	defer func() { discover, errOut = oldDisc, oldErr }()
	if code := run([]string{"bogus"}); code != 2 || called {
		t.Errorf("code=%d discover called=%v", code, called)
	}
}
