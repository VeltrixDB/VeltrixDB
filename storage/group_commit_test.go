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
	n        int
	p50, p99 time.Duration
	perSec   float64
	flushes  uint64
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
	}
}

// TestGroupCommit_AdaptiveGates is the regression gate: with a 300 µs
// device sync and the default 15 ms window,
//   - a lone writer must not wait for the window (P50 well under it), and
//   - 64 concurrent writers must still share syncs (far fewer flushes than
//     writes), at no less throughput than fixed-window commit.
func TestGroupCommit_AdaptiveGates(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	withSyncLatency(t, 300*time.Microsecond)
	const window = 15 * time.Millisecond

	alone := measurePuts(t, GroupCommitAdaptive, window, 700*time.Millisecond, 1)
	t.Logf("adaptive, 1 writer:   p50=%v p99=%v %.0f writes/s", alone.p50, alone.p99, alone.perSec)
	if alone.p50 > window/5 {
		t.Errorf("lone writer P50 %v: adaptive commit is waiting for the %v window", alone.p50, window)
	}

	fixed := measurePuts(t, GroupCommitFixed, window, time.Second, 64)
	adapt := measurePuts(t, GroupCommitAdaptive, window, time.Second, 64)
	t.Logf("fixed,    64 writers: p50=%v p99=%v %.0f writes/s, %.1f writes/flush", fixed.p50, fixed.p99, fixed.perSec, float64(fixed.n)/float64(fixed.flushes))
	t.Logf("adaptive, 64 writers: p50=%v p99=%v %.0f writes/s, %.1f writes/flush", adapt.p50, adapt.p99, adapt.perSec, float64(adapt.n)/float64(adapt.flushes))
	if float64(adapt.n)/float64(adapt.flushes) < 4 {
		t.Errorf("adaptive commit stopped batching: %.1f writes per flush at 64 writers", float64(adapt.n)/float64(adapt.flushes))
	}
	if adapt.perSec < 0.9*fixed.perSec {
		t.Errorf("adaptive throughput %.0f/s < 90%% of fixed %.0f/s at 64 writers", adapt.perSec, fixed.perSec)
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
