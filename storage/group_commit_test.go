package storage

// group_commit_test.go — adaptive vs fixed group commit.
//
// The device's fdatasync cost is emulated (groupCommitSync sleeps first), so
// the comparison means the same thing on a laptop, whose fsync returns at the
// drive cache, as on Linux NVMe. Run the table with:
//
//	VELTRIX_GC_TABLE=1 go test ./storage -run TestGroupCommit_LatencyTable -v

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withSyncLatency makes every group-commit fdatasync take at least d.
func withSyncLatency(t *testing.T, d time.Duration) {
	t.Helper()
	old := groupCommitSync
	groupCommitSync = func(fd int) error {
		time.Sleep(d)
		return fdatasync(fd)
	}
	t.Cleanup(func() { groupCommitSync = old })
}

type putStats struct {
	n         int
	p50, p99  time.Duration
	perSec    float64
	flushes   uint64
	wal, vlog pacerStats // flush decisions during the run
}

// enginePacerStats sums the flush decisions of every WAL and VLog pacer.
func enginePacerStats(se *StorageEngine) (wal, vlog pacerStats) {
	for _, w := range se.wals {
		wal = wal.add(w.pacer.stats())
	}
	for _, v := range se.vlogs {
		if v != nil {
			vlog = vlog.add(v.pacer.stats())
		}
	}
	return wal, vlog
}

func (a pacerStats) sub(b pacerStats) pacerStats {
	return pacerStats{a.Immediate - b.Immediate, a.Gap - b.Gap, a.Deadline - b.Deadline, a.Full - b.Full}
}

// measurePuts runs `writers` goroutines doing durable single-key Puts for dur.
func measurePuts(t *testing.T, mode string, window, dur time.Duration, writers int) putStats {
	t.Helper()
	cfg := testStorageConfig(t.TempDir())
	cfg.GroupCommit = mode
	cfg.WALFlushWindowMs = int(window / time.Millisecond)
	cfg.VLogFlushWindowMs = cfg.WALFlushWindowMs
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()
	<-se.ReplayDone
	flushes0 := se.metrics.WALFlushes.Load()
	wal0, vlog0 := enginePacerStats(se)

	var mu sync.Mutex
	var lats []time.Duration
	var stop atomic.Bool
	var wg sync.WaitGroup
	val := make([]byte, 128)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var local []time.Duration
			for i := 0; !stop.Load(); i++ {
				t0 := time.Now()
				if err := se.Put(fmt.Sprintf("gc-%d-%d", w, i), val, -1); err != nil {
					t.Error(err)
					return
				}
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			lats = append(lats, local...)
			mu.Unlock()
		}(w)
	}
	time.Sleep(dur)
	stop.Store(true)
	wg.Wait()
	wal1, vlog1 := enginePacerStats(se)
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	if len(lats) == 0 {
		t.Fatal("no writes completed")
	}
	return putStats{
		n:       len(lats),
		p50:     lats[len(lats)/2],
		p99:     lats[len(lats)*99/100],
		perSec:  float64(len(lats)) / dur.Seconds(),
		flushes: se.metrics.WALFlushes.Load() - flushes0,
		wal:     wal1.sub(wal0),
		vlog:    vlog1.sub(vlog0),
	}
}

// TestGroupCommit_AdaptiveGates is the regression gate: with a 300 µs
// device sync and the default 15 ms window,
//   - a lone writer must not wait for the window, and
//   - 64 concurrent writers must still share syncs (far fewer flushes than
//     writes), flushing on idle gaps rather than at the window deadline, at
//     no less throughput than fixed-window commit.
//
// The gates are built to hold on a loaded machine (go test ./... running
// other packages, CI neighbours). There the emulated 300 µs sync sleeps for
// milliseconds, the real fsync queues behind other processes' I/O, and writer
// goroutines wait for a CPU, so absolute latencies mean nothing and one
// fixed-then-adaptive pair can straddle a load spike. The old gates
// (lone-writer P50 < 3 ms absolute; one 1 s fixed run, then one 1 s adaptive
// run) failed that way 2/4 under load while every flush decision was correct.
// So:
//   - what the pacer DECIDED is gated exactly: pacer counters, immune to
//     timing (a lone writer's flushes are all "immediate"; concurrent flushes
//     are gap-driven, not deadline-driven);
//   - wall-clock claims are relative (adaptive vs fixed measured back to
//     back) and retried: a real regression fails every attempt, a load spike
//     does not.
func TestGroupCommit_AdaptiveGates(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	withSyncLatency(t, 300*time.Microsecond)
	const window = 15 * time.Millisecond
	const attempts = 5

	// Lone writer — decisions. Every WAL and VLog flush must have been issued
	// at once; any gap or deadline flush means a lone writer waited.
	alone := measurePuts(t, GroupCommitAdaptive, window, 500*time.Millisecond, 1)
	t.Logf("adaptive, 1 writer:   p50=%v p99=%v %.0f writes/s  wal=%+v vlog=%+v", alone.p50, alone.p99, alone.perSec, alone.wal, alone.vlog)
	for _, d := range []struct {
		name string
		s    pacerStats
	}{{"WAL", alone.wal}, {"VLog", alone.vlog}} {
		if waited := d.s.Gap + d.s.Deadline + d.s.Full; d.s.Immediate == 0 || waited > d.s.Immediate/100 {
			t.Errorf("lone writer: %s flushes %+v — adaptive commit made a lone writer wait", d.name, d.s)
		}
	}

	// Lone writer — wall clock. A fixed-window lone writer waits the whole
	// window; adaptive must take under half of that AND under half the window
	// (catches a wait the pacer does not see, e.g. a sleep in the flusher,
	// which would slow both modes alike).
	lonePass := false
	for a := 1; a <= attempts && !lonePass; a++ {
		fixed1 := measurePuts(t, GroupCommitFixed, window, 300*time.Millisecond, 1)
		adapt1 := measurePuts(t, GroupCommitAdaptive, window, 300*time.Millisecond, 1)
		lonePass = adapt1.p50 < fixed1.p50/2 && adapt1.p50 < window/2
		t.Logf("attempt %d, 1 writer: fixed p50=%v adaptive p50=%v pass=%v", a, fixed1.p50, adapt1.p50, lonePass)
	}
	if !lonePass {
		t.Errorf("lone writer: adaptive P50 not under half of fixed-window P50 and of the window in %d attempts: adaptive commit is waiting for the %v window", attempts, window)
	}

	// 64 writers. An attempt passes when adaptive batches (≥ 4 writes per
	// flush), flushes on idle gaps (deadline flushes are a minority), and
	// keeps ≥ 90% of fixed-window throughput measured right before it.
	var why string
	concPass := false
	for a := 1; a <= attempts && !concPass; a++ {
		fixed := measurePuts(t, GroupCommitFixed, window, 500*time.Millisecond, 64)
		adapt := measurePuts(t, GroupCommitAdaptive, window, 500*time.Millisecond, 64)
		perFlush := float64(adapt.n) / float64(adapt.flushes)
		flushes := adapt.wal.add(adapt.vlog)
		total := flushes.Immediate + flushes.Gap + flushes.Deadline + flushes.Full
		t.Logf("attempt %d, 64 writers: fixed %.0f writes/s (%.1f/flush, p50=%v)  adaptive %.0f writes/s (%.1f/flush, p50=%v)  wal=%+v vlog=%+v",
			a, fixed.perSec, float64(fixed.n)/float64(fixed.flushes), fixed.p50, adapt.perSec, perFlush, adapt.p50, adapt.wal, adapt.vlog)
		switch {
		case perFlush < 4:
			why = fmt.Sprintf("adaptive commit stopped batching: %.1f writes per flush at 64 writers", perFlush)
		case flushes.Deadline*4 > total:
			why = fmt.Sprintf("adaptive commit waits for the window: %d of %d flushes at the deadline", flushes.Deadline, total)
		case adapt.perSec < 0.9*fixed.perSec:
			why = fmt.Sprintf("adaptive throughput %.0f/s < 90%% of fixed %.0f/s at 64 writers", adapt.perSec, fixed.perSec)
		default:
			concPass = true
		}
	}
	if !concPass {
		t.Errorf("64 writers, all %d attempts failed; last: %s", attempts, why)
	}
}

// TestGroupCommit_LatencyTable prints the comparison table (no gates).
func TestGroupCommit_LatencyTable(t *testing.T) {
	if os.Getenv("VELTRIX_GC_TABLE") == "" {
		t.Skip("set VELTRIX_GC_TABLE=1 (≈25 s timing table; the nightly workflow runs it)")
	}
	const window = 15 * time.Millisecond
	for _, sync := range []time.Duration{300 * time.Microsecond, 2 * time.Millisecond} {
		t.Run(fmt.Sprintf("sync=%v", sync), func(t *testing.T) {
			withSyncLatency(t, sync)
			t.Logf("%-9s %7s %10s %10s %12s %10s", "mode", "writers", "p50", "p99", "writes/s", "per flush")
			for _, writers := range []int{1, 8, 64, 256} {
				for _, mode := range []string{GroupCommitFixed, GroupCommitAdaptive} {
					s := measurePuts(t, mode, window, 1500*time.Millisecond, writers)
					t.Logf("%-9s %7d %10v %10v %12.0f %10.1f", mode, writers,
						s.p50.Round(10*time.Microsecond), s.p99.Round(10*time.Microsecond), s.perSec, float64(s.n)/float64(s.flushes))
				}
			}
		})
	}
}

func TestCommitPacer_Decisions(t *testing.T) {
	var bt batchTimer
	p := newCommitPacer(GroupCommitAdaptive, 15*time.Millisecond)
	if !p.schedule(&bt, 1, false) {
		t.Fatal("adaptive: a lone first request must flush at once")
	}
	for i := 0; i < 20; i++ {
		p.observe(8, 300*time.Microsecond) // concurrent history
	}
	if p.schedule(&bt, 1, false) {
		t.Fatal("adaptive with concurrent history must open a batch")
	}
	if bt.deadline.IsZero() || bt.C == nil {
		t.Fatal("batch timer not armed")
	}
	if g := p.gap(); g < 250*time.Microsecond || g > 350*time.Microsecond {
		t.Fatalf("gap %v, want ≈ the 300 µs sync", g)
	}
	if !p.schedule(&bt, 5, true) {
		t.Fatal("a full batch must flush")
	}
	bt.stop()

	f := newCommitPacer(GroupCommitFixed, 15*time.Millisecond)
	if f.schedule(&bt, 1, false) {
		t.Fatal("fixed mode must wait for the window even when alone")
	}
	bt.stop()
	if !newCommitPacer(GroupCommitFixed, 0).schedule(&bt, 1, false) {
		t.Fatal("window 0 flushes immediately")
	}
}
