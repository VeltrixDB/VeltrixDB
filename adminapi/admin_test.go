package adminapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// ── mockEngine ────────────────────────────────────────────────────────────────

type mockEngine struct {
	indexSize      int
	cacheStats     storage.CacheStats
	vlogStats      []storage.VLogStats
	metrics        *storage.StorageMetrics
	walBytes       uint64
	walEntries     uint64
	checkpointErr  error
	namespaces     []storage.NSInfo
	quotaSnapshots []storage.QuotaSnapshot
	migratedCount  int
	migrateErrCnt  int
	cdcTotal       uint64
	cdcDropped     uint64
	cdcSubs        int
	subscribeCh    chan storage.CDCEvent
}

func newMockEngine() *mockEngine {
	m := &mockEngine{
		indexSize: 42,
		metrics:   &storage.StorageMetrics{},
		subscribeCh: make(chan storage.CDCEvent, 4),
	}
	// Initialise all atomic fields so nil-pointer panics can't happen.
	m.metrics.Writes = &atomic.Uint64{}
	m.metrics.Reads = &atomic.Uint64{}
	m.metrics.Deletes = &atomic.Uint64{}
	m.metrics.AtomicOps = &atomic.Uint64{}
	m.metrics.AuditDropped = &atomic.Uint64{}
	return m
}

func (me *mockEngine) GetIndexSize() int                     { return me.indexSize }
func (me *mockEngine) GetCacheStats() storage.CacheStats     { return me.cacheStats }
func (me *mockEngine) GetVLogStats() []storage.VLogStats     { return me.vlogStats }
func (me *mockEngine) GetMetrics() *storage.StorageMetrics   { return me.metrics }
func (me *mockEngine) GetWALTotals() (uint64, uint64)        { return me.walBytes, me.walEntries }
func (me *mockEngine) Checkpoint() error                     { return me.checkpointErr }
func (me *mockEngine) ListNamespaces() []storage.NSInfo      { return me.namespaces }
func (me *mockEngine) QuotaStats() []storage.QuotaSnapshot   { return me.quotaSnapshots }
func (me *mockEngine) SetNamespaceLimit(ns string, limit storage.QuotaLimit) {}
func (me *mockEngine) MigrateAll() (int, int)                { return me.migratedCount, me.migrateErrCnt }
func (me *mockEngine) CDCStats() (uint64, uint64, int)       { return me.cdcTotal, me.cdcDropped, me.cdcSubs }
func (me *mockEngine) ChangesSince(since int64, limit int) storage.ChangesSinceResult {
	return storage.ChangesSinceResult{Cursor: since}
}
func (me *mockEngine) Subscribe(buf int, prefix string) (<-chan storage.CDCEvent, func()) {
	return me.subscribeCh, func() {}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newRouter(e Engine) *http.ServeMux {
	mux := http.NewServeMux()
	Register(mux, e, "/admin", nil)
	return mux
}

func do(t *testing.T, mux http.Handler, method, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody io.Reader
	if body != "" {
		reqBody = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reqBody)
	if body != "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&m); err != nil {
		t.Fatalf("decode JSON body: %v\nbody: %s", err, rec.Body.String())
	}
	return m
}

// ── /admin/stats ──────────────────────────────────────────────────────────────

func TestStats_GET_OK(t *testing.T) {
	me := newMockEngine()
	me.indexSize = 77
	me.metrics.Writes.Store(10)
	me.metrics.Reads.Store(20)

	mux := newRouter(me)
	rec := do(t, mux, http.MethodGet, "/admin/stats", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	m := decodeJSON(t, rec)
	if int(m["index_keys"].(float64)) != 77 {
		t.Fatalf("index_keys mismatch: %v", m["index_keys"])
	}
	if int(m["writes_total"].(float64)) != 10 {
		t.Fatalf("writes_total mismatch: %v", m["writes_total"])
	}
}

func TestStats_WrongMethod(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodPost, "/admin/stats", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// ── /admin/checkpoint ─────────────────────────────────────────────────────────

func TestCheckpoint_POST_OK(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodPost, "/admin/checkpoint", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	m := decodeJSON(t, rec)
	if m["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", m["status"])
	}
}

func TestCheckpoint_Error(t *testing.T) {
	me := newMockEngine()
	me.checkpointErr = errors.New("disk full")
	rec := do(t, newRouter(me), http.MethodPost, "/admin/checkpoint", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestCheckpoint_WrongMethod(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodGet, "/admin/checkpoint", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// ── /admin/quotas ─────────────────────────────────────────────────────────────

func TestQuotas_GET(t *testing.T) {
	me := newMockEngine()
	me.quotaSnapshots = []storage.QuotaSnapshot{
		{Namespace: "ns1", WritesPerSec: 100},
	}
	rec := do(t, newRouter(me), http.MethodGet, "/admin/quotas", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var arr []interface{}
	if err := json.NewDecoder(rec.Body).Decode(&arr); err != nil {
		t.Fatalf("decode array: %v", err)
	}
	if len(arr) != 1 {
		t.Fatalf("expected 1 quota entry, got %d", len(arr))
	}
}

func TestQuotas_POST(t *testing.T) {
	form := url.Values{
		"ns":            []string{"myns"},
		"writes_per_sec": []string{"50"},
		"burst":         []string{"100"},
		"max_keys":      []string{"1000"},
	}
	rec := do(t, newRouter(newMockEngine()), http.MethodPost, "/admin/quotas", form.Encode())
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	m := decodeJSON(t, rec)
	if m["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", m["status"])
	}
	if m["ns"] != "myns" {
		t.Fatalf("expected ns=myns, got %v", m["ns"])
	}
}

func TestQuotas_WrongMethod(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodPut, "/admin/quotas", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// ── /admin/migrate ────────────────────────────────────────────────────────────

func TestMigrate_POST_OK(t *testing.T) {
	me := newMockEngine()
	me.migratedCount = 5
	me.migrateErrCnt = 0
	rec := do(t, newRouter(me), http.MethodPost, "/admin/migrate", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	m := decodeJSON(t, rec)
	if int(m["migrated"].(float64)) != 5 {
		t.Fatalf("migrated count mismatch: %v", m["migrated"])
	}
	if int(m["errors"].(float64)) != 0 {
		t.Fatalf("expected 0 errors, got %v", m["errors"])
	}
}

func TestMigrate_WrongMethod(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodGet, "/admin/migrate", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

// ── /admin/version ────────────────────────────────────────────────────────────

func TestVersion_GET(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodGet, "/admin/version", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	m := decodeJSON(t, rec)
	if _, ok := m["current_schema_version"]; !ok {
		t.Fatal("current_schema_version missing from response")
	}
}

// ── /admin/cdc ────────────────────────────────────────────────────────────────

func TestCDC_WrongMethod(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodPost, "/admin/cdc", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestCDC_StreamsEventsWithTimeout(t *testing.T) {
	me := newMockEngine()

	// Pre-fill two events; CDC handler will drain them then hit the timer.
	me.subscribeCh <- storage.CDCEvent{Op: "PUT", Key: "k1", Value: []byte("v1"), Timestamp: time.Now().UnixMicro()}
	me.subscribeCh <- storage.CDCEvent{Op: "PUT", Key: "k2", Value: []byte("v2"), Timestamp: time.Now().UnixMicro()}

	mux := newRouter(me)
	req := httptest.NewRequest(http.MethodGet, "/admin/cdc?duration_seconds=1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "k1") || !strings.Contains(body, "k2") {
		t.Fatalf("expected CDC events in response, got: %s", body)
	}
}

// ── /admin/ui (smoke test) ────────────────────────────────────────────────────

func TestUI_Reachable(t *testing.T) {
	rec := do(t, newRouter(newMockEngine()), http.MethodGet, "/admin/ui", "")
	// UI returns HTML; just verify it doesn't 404.
	if rec.Code == http.StatusNotFound {
		t.Fatalf("expected non-404 from /admin/ui, got %d", rec.Code)
	}
}

// ── Register with custom prefix ───────────────────────────────────────────────

func TestRegister_CustomPrefix(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, newMockEngine(), "/ops", nil)

	rec := do(t, mux, http.MethodGet, "/ops/stats", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 at custom prefix /ops/stats, got %d", rec.Code)
	}
}

func TestRegister_TrailingSlashStripped(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux, newMockEngine(), "/admin/", nil)

	rec := do(t, mux, http.MethodGet, "/admin/stats", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after trailing-slash strip, got %d", rec.Code)
	}
}
