// Package tracing provides a minimal OpenTelemetry-compatible tracing
// surface using only the Go standard library.
//
// Why not import go.opentelemetry.io/otel directly?  That dependency tree
// pulls in 30+ packages and bumps go.mod.  This package implements the
// 80% subset of OTel we actually need (request id, parent/child spans,
// duration, status, attributes) and exposes an interface that makes
// swapping in the real OTel SDK a one-file change once the dep lands.
//
// Mental model:
//
//	ctx, span := tracing.Start(ctx, "engine.Put")
//	defer span.End()
//	span.SetAttribute("key.size", len(key))
//	if err != nil { span.SetError(err) }
//
// Output: spans completed in the last `retain` window are queryable via
// HandleTracesHTTP, which returns them as JSON Lines.  Slow spans (above
// SlowThreshold) are also written to the standard log so operators see
// them without enabling the full trace stream.
package tracing

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Span is the interface callers hold. Compatible (signature-wise) with the
// subset of go.opentelemetry.io/otel/trace.Span we use; future drop-in
// replacement is straightforward.
type Span interface {
	End()
	SetAttribute(key string, value any)
	SetError(err error)
	TraceID() string
	SpanID() string
}

// Sampler decides whether a new root span is recorded. Returning false makes
// Start return a no-op span (zero allocation, no map writes).
type Sampler func(name string) bool

// AlwaysSample records every span. Default for dev.
func AlwaysSample(_ string) bool { return true }

// RateSampler returns a sampler that keeps roughly fraction of root spans
// (range [0,1]). Slow spans are still always recorded by SetSlowThreshold.
func RateSampler(fraction float64) Sampler {
	if fraction >= 1 {
		return AlwaysSample
	}
	if fraction <= 0 {
		return func(string) bool { return false }
	}
	return func(string) bool {
		return rand.Float64() < fraction
	}
}

// Configuration is read at process startup and immutable afterwards.
type Configuration struct {
	Sampler        Sampler
	SlowThreshold  time.Duration // spans above this are always recorded + logged
	Retain         int           // recent-span ring buffer size for /traces
	ServiceName    string
	ServiceVersion string
}

var (
	cfgMu     sync.RWMutex
	cfg       = Configuration{Sampler: AlwaysSample, SlowThreshold: 50 * time.Millisecond, Retain: 1024}
	ringMu    sync.Mutex
	ringBuf   []completedSpan
	ringWrite int
)

// Configure replaces the global tracing config. Safe to call at startup.
func Configure(c Configuration) {
	if c.Sampler == nil {
		c.Sampler = AlwaysSample
	}
	if c.Retain <= 0 {
		c.Retain = 1024
	}
	cfgMu.Lock()
	cfg = c
	ringBuf = make([]completedSpan, c.Retain)
	ringWrite = 0
	cfgMu.Unlock()
}

// completedSpan is the on-record-buffer struct.
type completedSpan struct {
	Service        string         `json:"service"`
	Name           string         `json:"name"`
	TraceID        string         `json:"trace_id"`
	SpanID         string         `json:"span_id"`
	ParentSpanID   string         `json:"parent_span_id,omitempty"`
	StartUnixNano  int64          `json:"start_unix_ns"`
	DurationNs     int64          `json:"duration_ns"`
	StatusError    string         `json:"error,omitempty"`
	Attributes     map[string]any `json:"attrs,omitempty"`
}

// span is the concrete in-flight implementation.
type span struct {
	name      string
	traceID   [16]byte
	spanID    [8]byte
	parentID  [8]byte
	hasParent bool
	startTime time.Time
	attrs     map[string]any
	errStr    string
	noop      bool
	mu        sync.Mutex
	ended     bool
}

type ctxKey struct{}

// Start opens a new span. If parent context already carries a span, the new
// span links to it as child; otherwise a new trace begins.
func Start(ctx context.Context, name string) (context.Context, Span) {
	cfgMu.RLock()
	curCfg := cfg
	cfgMu.RUnlock()

	parent, hasParent := ctx.Value(ctxKey{}).(*span)

	// Sampling decision: unsampled becomes a fast no-op span.
	sampled := hasParent || curCfg.Sampler(name)
	if !sampled {
		return ctx, &span{name: name, noop: true, startTime: time.Now()}
	}

	s := &span{name: name, startTime: time.Now()}
	if hasParent && parent != nil {
		s.traceID = parent.traceID
		s.parentID = parent.spanID
		s.hasParent = true
	} else {
		// New trace: fresh 128-bit trace ID, 64-bit span ID.
		// math/rand is fine for trace IDs — they don't need to be unguessable.
		for i := 0; i < 16; i++ {
			s.traceID[i] = byte(rand.Uint32())
		}
	}
	for i := 0; i < 8; i++ {
		s.spanID[i] = byte(rand.Uint32())
	}
	return context.WithValue(ctx, ctxKey{}, s), s
}

// End records the span. Idempotent.
func (s *span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	dur := time.Since(s.startTime)
	attrs := s.attrs
	errStr := s.errStr
	s.mu.Unlock()

	if s.noop {
		return
	}

	cfgMu.RLock()
	slowThresh := cfg.SlowThreshold
	service := cfg.ServiceName
	cfgMu.RUnlock()

	// Always-record: slow spans + spans with errors.
	if dur >= slowThresh || errStr != "" {
		log.Printf("[trace] %s duration=%s trace_id=%s span_id=%s%s",
			s.name, dur, hex.EncodeToString(s.traceID[:]),
			hex.EncodeToString(s.spanID[:]),
			func() string {
				if errStr != "" {
					return " err=" + errStr
				}
				return ""
			}())
	}

	rec := completedSpan{
		Service:       service,
		Name:          s.name,
		TraceID:       hex.EncodeToString(s.traceID[:]),
		SpanID:        hex.EncodeToString(s.spanID[:]),
		StartUnixNano: s.startTime.UnixNano(),
		DurationNs:    dur.Nanoseconds(),
		StatusError:   errStr,
		Attributes:    attrs,
	}
	if s.hasParent {
		rec.ParentSpanID = hex.EncodeToString(s.parentID[:])
	}
	ringMu.Lock()
	if len(ringBuf) > 0 {
		ringBuf[ringWrite] = rec
		ringWrite = (ringWrite + 1) % len(ringBuf)
	}
	ringMu.Unlock()
}

// SetAttribute records a key/value on the span. No-op on no-op spans.
func (s *span) SetAttribute(key string, value any) {
	if s.noop {
		return
	}
	s.mu.Lock()
	if s.attrs == nil {
		s.attrs = map[string]any{}
	}
	s.attrs[key] = value
	s.mu.Unlock()
}

// SetError marks the span as failed. No-op on no-op spans.
func (s *span) SetError(err error) {
	if s.noop || err == nil {
		return
	}
	s.mu.Lock()
	s.errStr = err.Error()
	s.mu.Unlock()
}

// TraceID returns the hex-encoded 128-bit trace ID.
func (s *span) TraceID() string {
	return hex.EncodeToString(s.traceID[:])
}

// SpanID returns the hex-encoded 64-bit span ID.
func (s *span) SpanID() string {
	return hex.EncodeToString(s.spanID[:])
}

// HandleTracesHTTP serves the recent-span ring buffer as JSON Lines. Optional
// ?limit=N caps the output. ?error=true filters to error spans only.
func HandleTracesHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")

	limit := -1
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	errOnly := r.URL.Query().Get("error") == "true"

	ringMu.Lock()
	cp := make([]completedSpan, len(ringBuf))
	copy(cp, ringBuf)
	ringMu.Unlock()

	enc := json.NewEncoder(w)
	written := 0
	for _, rec := range cp {
		if rec.Name == "" {
			continue
		}
		if errOnly && rec.StatusError == "" {
			continue
		}
		if err := enc.Encode(rec); err != nil {
			return
		}
		written++
		if limit > 0 && written >= limit {
			break
		}
	}
	if written == 0 {
		fmt.Fprintln(w, `{"info":"no spans recorded yet"}`)
	}
}
