package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func resetTracing(t *testing.T) {
	t.Helper()
	Configure(Configuration{
		Sampler:       AlwaysSample,
		SlowThreshold: 50 * time.Millisecond,
		Retain:        64,
		ServiceName:   "test",
	})
}

// ── Start / span basics ───────────────────────────────────────────────────────

func TestStart_NewTrace(t *testing.T) {
	resetTracing(t)
	ctx, span := Start(context.Background(), "op.test")
	defer span.End()

	if span.TraceID() == "" {
		t.Fatal("TraceID should not be empty for a new trace")
	}
	if span.SpanID() == "" {
		t.Fatal("SpanID should not be empty")
	}
	if ctx == context.Background() {
		t.Fatal("Start must return a new context carrying the span")
	}
}

func TestStart_ChildInheritsTraceID(t *testing.T) {
	resetTracing(t)
	parentCtx, parent := Start(context.Background(), "parent")
	defer parent.End()

	_, child := Start(parentCtx, "child")
	defer child.End()

	if child.TraceID() != parent.TraceID() {
		t.Fatalf("child traceID %q != parent traceID %q", child.TraceID(), parent.TraceID())
	}
	if child.SpanID() == parent.SpanID() {
		t.Fatal("child and parent must have distinct span IDs")
	}
}

func TestSpan_SetAttribute(t *testing.T) {
	resetTracing(t)
	_, span := Start(context.Background(), "attr.test")
	span.SetAttribute("key.size", 42)
	span.End()

	// attribute must appear in the ring buffer's last entry
	rec := lastRecordedSpan(t)
	if v, ok := rec["attrs"].(map[string]interface{}); !ok {
		t.Fatal("attrs field missing or wrong type")
	} else if v["key.size"] == nil {
		t.Fatal("key.size attribute not stored")
	}
}

func TestSpan_SetError(t *testing.T) {
	resetTracing(t)
	_, span := Start(context.Background(), "err.test")
	span.SetError(errors.New("disk full"))
	span.End()

	rec := lastRecordedSpan(t)
	errStr, _ := rec["error"].(string)
	if errStr != "disk full" {
		t.Fatalf("expected error %q, got %q", "disk full", errStr)
	}
}

func TestSpan_End_Idempotent(t *testing.T) {
	resetTracing(t)
	_, span := Start(context.Background(), "idem.test")
	span.End()
	span.End() // must not panic or double-record
}

func TestSpan_SetAttribute_Noop(t *testing.T) {
	resetTracing(t)
	// Use RateSampler(0) to produce a guaranteed noop span.
	Configure(Configuration{Sampler: RateSampler(0), Retain: 64, SlowThreshold: time.Hour})
	_, noopSpan := Start(context.Background(), "noop")
	noopSpan.SetAttribute("k", "v") // must not panic
	noopSpan.SetError(errors.New("ignored"))
	noopSpan.End()
}

// ── Sampler ───────────────────────────────────────────────────────────────────

func TestRateSampler_Zero_AlwaysNoop(t *testing.T) {
	Configure(Configuration{
		Sampler:       RateSampler(0),
		Retain:        64,
		SlowThreshold: time.Hour,
	})
	for i := 0; i < 100; i++ {
		_, sp := Start(context.Background(), "sampled")
		concrete := sp.(*span)
		if !concrete.noop {
			t.Fatal("RateSampler(0) should always produce noop spans")
		}
		sp.End()
	}
}

func TestRateSampler_One_AlwaysRecord(t *testing.T) {
	sampler := RateSampler(1)
	for i := 0; i < 20; i++ {
		if !sampler("x") {
			t.Fatal("RateSampler(1) must always return true")
		}
	}
}

func TestRateSampler_ChildOfNoop_AlsoNoop(t *testing.T) {
	// When the root is not sampled, the context carries a noop span.
	// A child of a noop span inherits the noop state (no parent recorded in ctx).
	Configure(Configuration{
		Sampler:       RateSampler(0),
		Retain:        64,
		SlowThreshold: time.Hour,
	})
	parentCtx, parent := Start(context.Background(), "root")
	defer parent.End()

	_, child := Start(parentCtx, "child")
	defer child.End()

	// Both should be noops — ring buffer should remain empty.
	ringMu.Lock()
	for _, r := range ringBuf {
		if r.Name != "" {
			ringMu.Unlock()
			t.Fatal("noop spans must not appear in ring buffer")
		}
	}
	ringMu.Unlock()
}

// ── Configure ────────────────────────────────────────────────────────────────

func TestConfigure_ReplacesRingBuffer(t *testing.T) {
	Configure(Configuration{Sampler: AlwaysSample, Retain: 8, SlowThreshold: time.Hour})
	_, span := Start(context.Background(), "before.reset")
	span.End()

	Configure(Configuration{Sampler: AlwaysSample, Retain: 8, SlowThreshold: time.Hour})

	// After reconfigure the ring is zeroed — no old entries.
	ringMu.Lock()
	for _, r := range ringBuf {
		if r.Name != "" {
			ringMu.Unlock()
			t.Fatal("ring buffer should be cleared after Configure")
		}
	}
	ringMu.Unlock()
}

// ── HTTP handler ──────────────────────────────────────────────────────────────

func TestHandleTracesHTTP_NoSpans(t *testing.T) {
	resetTracing(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces", nil)
	HandleTracesHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "no spans") {
		t.Fatalf("expected 'no spans' message, got: %s", body)
	}
}

func TestHandleTracesHTTP_WithSpans(t *testing.T) {
	resetTracing(t)
	_, s := Start(context.Background(), "http.op")
	s.SetAttribute("env", "test")
	s.End()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces", nil)
	HandleTracesHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "http.op") {
		t.Fatalf("span name missing from output: %s", body)
	}
}

func TestHandleTracesHTTP_LimitParam(t *testing.T) {
	resetTracing(t)
	for i := 0; i < 10; i++ {
		_, s := Start(context.Background(), "span")
		s.End()
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces?limit=3", nil)
	HandleTracesHTTP(rec, req)

	lines := countNonEmptyLines(rec.Body.String())
	if lines > 3 {
		t.Fatalf("limit=3 but got %d lines", lines)
	}
}

func TestHandleTracesHTTP_ErrorFilter(t *testing.T) {
	resetTracing(t)

	_, good := Start(context.Background(), "good")
	good.End()

	_, bad := Start(context.Background(), "bad")
	bad.SetError(errors.New("oops"))
	bad.End()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces?error=true", nil)
	HandleTracesHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, `"name":"good"`) {
		t.Fatal("?error=true should filter out non-error spans")
	}
	if !strings.Contains(body, `"name":"bad"`) {
		t.Fatal("?error=true should include error spans")
	}
}

func TestHandleTracesHTTP_ContentType(t *testing.T) {
	resetTracing(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces", nil)
	HandleTracesHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "ndjson") {
		t.Fatalf("expected ndjson content-type, got %q", ct)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func lastRecordedSpan(t *testing.T) map[string]interface{} {
	t.Helper()
	// Collect ring buffer via the HTTP handler and parse the last NDJSON line.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/traces", nil)
	HandleTracesHTTP(rec, req)

	body, _ := io.ReadAll(rec.Body)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" {
			continue
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(l), &m); err == nil {
			return m
		}
	}
	t.Fatal("no valid JSON span found in ring buffer")
	return nil
}

func countNonEmptyLines(s string) int {
	n := 0
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}
