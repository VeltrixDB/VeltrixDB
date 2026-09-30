package storage

// group_commit.go — adaptive group-commit pacing shared by the WAL and VLog
// flushers.
//
// Fixed-window group commit (the original behaviour, GroupCommitFixed) opens
// a batch on its first request and flushes when the configured window
// (--wal-flush-window-ms, default 15 ms) expires. Every durable single-key
// Put therefore waited the whole window even when nobody else was writing:
// a lone writer's latency was ~15 ms + fdatasync, and a single client
// managed ~65 writes/s. That window was the main reason durable single-key
// writes trailed Aerospike.
//
// Adaptive pacing (GroupCommitAdaptive, the default) keeps the window only as
// an upper bound and decides per batch:
//
//   - Alone: the batch holds one request and recent batches held ~one
//     request each (EWMA < adaptiveAloneBatch) → flush immediately. Latency
//     is one fdatasync.
//   - Concurrent: keep the batch open while requests keep arriving, and
//     flush once none has arrived for an idle gap (≈ the recent fdatasync
//     cost, clamped to [adaptiveMinGap, adaptiveMaxGap]), or when the window
//     deadline or the batch cap is reached. Waiting longer than one sync to
//     collect stragglers buys nothing: requests that arrive during a sync
//     queue up and form the next batch anyway (natural batching).
//
// Durability is identical in both modes: every waiter is answered only after
// the fdatasync covering its bytes. Only WHEN the sync is issued changes.

import (
	"sync/atomic"
	"time"
)

// GroupCommit modes for StorageConfig.GroupCommit.
const (
	GroupCommitAdaptive = "adaptive"
	GroupCommitFixed    = "fixed"
)

const (
	// adaptiveAloneBatch: below this EWMA of requests per flush the writer
	// is treated as alone and flushed at once.
	adaptiveAloneBatch = 1.5
	adaptiveMinGap     = 20 * time.Microsecond
	adaptiveMaxGap     = 2 * time.Millisecond
	adaptiveEWMAWeight = 0.2
)

// groupCommitSync is the fdatasync the group-commit flushers call. A var
// only so tests can emulate a device's sync latency (group_commit_test.go).
var groupCommitSync = fdatasync

// commitPacer holds one flusher's adaptive state. Owned by the flusher
// goroutine; the atomics are only for stats readers.
type commitPacer struct {
	adaptive bool
	window   time.Duration

	ewmaBatch float64       // requests per flush
	ewmaSync  time.Duration // fdatasync duration

	immediate atomic.Uint64 // flushes issued without waiting (alone)
	gapped    atomic.Uint64 // flushes issued after an idle gap / deadline
}

func newCommitPacer(mode string, window time.Duration) *commitPacer {
	return &commitPacer{adaptive: mode != GroupCommitFixed, window: window, ewmaBatch: 1}
}

// alone reports whether a batch of n requests should flush right away.
func (p *commitPacer) alone(n int) bool {
	return p.adaptive && n <= 1 && p.ewmaBatch < adaptiveAloneBatch
}

// gap is how long to wait for the next request before flushing.
func (p *commitPacer) gap() time.Duration {
	g := p.ewmaSync
	if g < adaptiveMinGap {
		g = adaptiveMinGap
	}
	if g > adaptiveMaxGap {
		g = adaptiveMaxGap
	}
	if p.window > 0 && g > p.window {
		g = p.window
	}
	return g
}

// observe records one completed flush of n requests whose sync took d.
func (p *commitPacer) observe(n int, d time.Duration) {
	p.ewmaBatch = (1-adaptiveEWMAWeight)*p.ewmaBatch + adaptiveEWMAWeight*float64(n)
	if p.ewmaSync == 0 {
		p.ewmaSync = d
	} else {
		p.ewmaSync = time.Duration((1-adaptiveEWMAWeight)*float64(p.ewmaSync) + adaptiveEWMAWeight*float64(d))
	}
}

// batchTimer is the one timer a flusher uses: in fixed mode it fires at the
// window deadline; in adaptive mode at min(now+gap, deadline), re-armed on
// every arrival.
type batchTimer struct {
	t        *time.Timer
	C        <-chan time.Time
	deadline time.Time // zero = no open batch
}

// arm (re)schedules the timer to fire after d. Safe with both the pre- and
// post-Go 1.23 timer channel semantics.
func (bt *batchTimer) arm(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if bt.t == nil {
		bt.t = time.NewTimer(d)
		bt.C = bt.t.C
		return
	}
	if !bt.t.Stop() {
		select {
		case <-bt.t.C:
		default:
		}
	}
	bt.t.Reset(d)
	bt.C = bt.t.C
}

// stop cancels the timer and closes the batch.
func (bt *batchTimer) stop() {
	if bt.t != nil && !bt.t.Stop() {
		select {
		case <-bt.t.C:
		default:
		}
	}
	bt.C = nil
	bt.deadline = time.Time{}
}

// schedule decides what to do after new requests joined a batch that now
// holds n. It returns true when the caller must flush now; otherwise it has
// (re)armed the timer.
func (p *commitPacer) schedule(bt *batchTimer, n int, full bool) bool {
	if p.window <= 0 || full {
		return true
	}
	now := time.Now()
	if bt.deadline.IsZero() {
		if p.alone(n) {
			p.immediate.Add(1)
			return true
		}
		bt.deadline = now.Add(p.window)
		if !p.adaptive {
			bt.arm(p.window)
			return false
		}
	} else if !p.adaptive {
		return false // fixed: the window timer is already running
	}
	rest := bt.deadline.Sub(now)
	if rest <= 0 {
		return true
	}
	g := p.gap()
	if g > rest {
		g = rest
	}
	bt.arm(g)
	return false
}
