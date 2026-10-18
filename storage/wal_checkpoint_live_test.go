package storage

// wal_checkpoint_live_test.go — runtime (online) WAL checkpoints: every write
// acknowledged before, during or after a checkpoint taken while the engine
// serves traffic survives a restart, crash or clean (wal_checkpoint_live.go,
// CLAUDE.md invariant 21).

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ckptOracle is the expected state: key → value, "" = absent.
type ckptOracle struct {
	mu   sync.Mutex
	want map[string]string
	ttl  map[string]bool // keys written with a (long) TTL
}

func newCkptOracle() *ckptOracle {
	return &ckptOracle{want: map[string]string{}, ttl: map[string]bool{}}
}

func (o *ckptOracle) put(t testing.TB, se *StorageEngine, k, v string, ttl int32) {
	t.Helper()
	if err := se.Put(k, []byte(v), ttl); err != nil {
		t.Errorf("put %s: %v", k, err)
		return
	}
	o.mu.Lock()
	o.want[k] = v
	o.ttl[k] = ttl > 0
	o.mu.Unlock()
}

func (o *ckptOracle) del(t testing.TB, se *StorageEngine, k string) {
	t.Helper()
	if err := se.Delete(k); err != nil {
		t.Errorf("delete %s: %v", k, err)
		return
	}
	o.mu.Lock()
	o.want[k] = ""
	delete(o.ttl, k)
	o.mu.Unlock()
}

func (o *ckptOracle) multiPut(t testing.TB, se *StorageEngine, prefix string, n int) {
	t.Helper()
	reqs := make([]MultiPutRequest, n)
	for i := range reqs {
		reqs[i] = MultiPutRequest{Key: fmt.Sprintf("%s-%03d", prefix, i), Value: []byte("mp-" + prefix), TTL: -1}
	}
	for i, err := range se.MultiPut(reqs) {
		if err != nil {
			t.Errorf("multiput %s: %v", reqs[i].Key, err)
			return
		}
	}
	o.mu.Lock()
	for _, r := range reqs {
		o.want[r.Key] = string(r.Value)
		o.ttl[r.Key] = false
	}
	o.mu.Unlock()
}

func (o *ckptOracle) snapshot() (map[string]string, map[string]bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	w := make(map[string]string, len(o.want))
	for k, v := range o.want {
		w[k] = v
	}
	tt := make(map[string]bool, len(o.ttl))
	for k, v := range o.ttl {
		tt[k] = v
	}
	return w, tt
}

// verifyOracle checks every expected key; it reports at most 10 mismatches.
func verifyOracle(t testing.TB, se *StorageEngine, want map[string]string, ttl map[string]bool) {
	t.Helper()
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	bad := 0
	for _, k := range keys {
		v := want[k]
		got, err := se.Get(k)
		switch {
		case v == "" && err == nil:
			bad++
			if bad <= 10 {
				t.Errorf("%s = %q after restart, want absent (deleted)", k, got)
			}
		case v != "" && (err != nil || string(got) != v):
			bad++
			if bad <= 10 {
				t.Errorf("%s = %q (%v) after restart, want %q — acknowledged write lost", k, got, err, v)
			}
		case v != "" && ttl[k]:
			if r := se.GetTTLForKey(k); r <= 0 {
				bad++
				if bad <= 10 {
					t.Errorf("%s: TTL %d after restart, want a positive remaining TTL", k, r)
				}
			}
		case v != "" && !ttl[k]:
			if r := se.GetTTLForKey(k); r != -1 {
				bad++
				if bad <= 10 {
					t.Errorf("%s: TTL %d after restart, want immortal (-1)", k, r)
				}
			}
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d keys wrong after restart", bad, len(want))
	}
}

func ckptDirs(t testing.TB, n int) []string {
	root := t.TempDir()
	dirs := make([]string, n)
	for i := range dirs {
		dirs[i] = filepath.Join(root, fmt.Sprint("disk", i))
	}
	return dirs
}

func openCkptEngine(t testing.TB, mut func(*StorageConfig), dirs ...string) *StorageEngine {
	t.Helper()
	cfg := testStorageConfig(dirs...)
	if mut != nil {
		mut(cfg)
	}
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se.ReplayDone
	return se
}

// restartEngine restarts se either cleanly (Close) or crash-style: the old
// engine is abandoned with its goroutines and fds still live (invariant 59's
// helper), and a second engine replays whatever the files hold.
func restartEngine(t testing.TB, se *StorageEngine, crash bool, mut func(*StorageConfig), dirs ...string) *StorageEngine {
	t.Helper()
	if crash {
		abandonEngineForCrashTest(se)
	} else if err := se.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return openCkptEngine(t, mut, dirs...)
}

// snapshotDataDirs copies every file in dirs to fresh directories — the
// on-disk state a power cut at this instant would leave (each file read once,
// in full). The LOCK file is skipped so the copy can be opened.
func snapshotDataDirs(t testing.TB, dirs []string) []string {
	t.Helper()
	root := t.TempDir()
	out := make([]string, len(dirs))
	for i, d := range dirs {
		out[i] = filepath.Join(root, fmt.Sprint("disk", i))
		if err := filepath.Walk(d, func(p string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(d, p)
			dst := filepath.Join(out[i], rel)
			if fi.IsDir() {
				return os.MkdirAll(dst, 0755)
			}
			if fi.Name() == "LOCK" {
				return nil
			}
			src, err := os.Open(p)
			if os.IsNotExist(err) {
				return nil // a temp file renamed away mid-walk
			}
			if err != nil {
				return err
			}
			defer src.Close()
			f, err := os.Create(dst)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(f, src)
			return err
		}); err != nil {
			t.Fatalf("snapshot %s: %v", d, err)
		}
	}
	return out
}

// writeAfterCheckpoint is the mix every reproduction writes after the
// checkpoint: new keys, overwrites, deletes of old and new keys, TTL keys,
// a MultiPut batch and an atomic op.
func writeAfterCheckpoint(t testing.TB, se *StorageEngine, o *ckptOracle, tag string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		o.put(t, se, fmt.Sprintf("%s-new-%03d", tag, i), "after-"+tag, -1)
	}
	for i := 0; i < 20; i++ {
		o.put(t, se, fmt.Sprintf("base-%03d", i), "overwritten-"+tag, -1)
	}
	for i := 20; i < 40; i++ {
		o.del(t, se, fmt.Sprintf("base-%03d", i))
	}
	for i := 0; i < 10; i++ {
		o.del(t, se, fmt.Sprintf("%s-new-%03d", tag, i))
	}
	for i := 0; i < 20; i++ {
		o.put(t, se, fmt.Sprintf("%s-ttl-%03d", tag, i), "ttl-"+tag, 3600)
	}
	o.multiPut(t, se, tag+"-mp", 64)
	if res, err := se.SetIfNotExists(tag+"-setnx", []byte("nx"), -1); err != nil || res != SetNXCreated {
		t.Fatalf("SetIfNotExists: %v %v", res, err)
	}
	o.mu.Lock()
	o.want[tag+"-setnx"] = "nx"
	o.ttl[tag+"-setnx"] = false
	o.mu.Unlock()
}

func writeBase(t testing.TB, se *StorageEngine, o *ckptOracle) {
	t.Helper()
	for i := 0; i < 100; i++ {
		o.put(t, se, fmt.Sprintf("base-%03d", i), "base", -1)
	}
	for i := 0; i < 10; i++ {
		o.put(t, se, fmt.Sprintf("base-ttl-%03d", i), "base-ttl", 3600)
	}
}

// TestLiveCheckpoint_WritesAfterCheckpointSurviveRestart is the reproduction
// of the data-loss bug: a runtime checkpoint (admin /checkpoint, an online
// backup, CheckpointManager) renamed the compacted file over wal.log while
// the flusher kept appending to the old, now unlinked inode — every later
// write was gone after a crash restart.
func TestLiveCheckpoint_WritesAfterCheckpointSurviveRestart(t *testing.T) {
	triggers := []struct {
		name string
		run  func(t *testing.T, se *StorageEngine)
	}{
		{"checkpoint", func(t *testing.T, se *StorageEngine) {
			if err := se.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
		}},
		{"full-backup", func(t *testing.T, se *StorageEngine) {
			if _, err := NewBackupEngine(se).FullBackup(t.TempDir()); err != nil {
				t.Fatalf("FullBackup: %v", err)
			}
		}},
		{"checkpoint-manager", func(t *testing.T, se *StorageEngine) {
			if err := NewCheckpointManager(se, time.Hour).ForceCheckpoint(); err != nil {
				t.Fatalf("ForceCheckpoint: %v", err)
			}
		}},
	}
	for _, disks := range []int{1, 3} {
		for _, tr := range triggers {
			for _, crash := range []bool{true, false} {
				mode := "clean"
				if crash {
					mode = "crash"
				}
				t.Run(fmt.Sprintf("%ddisk/%s/%s", disks, tr.name, mode), func(t *testing.T) {
					dirs := ckptDirs(t, disks)
					se := openCkptEngine(t, nil, dirs...)
					o := newCkptOracle()
					writeBase(t, se, o)
					tr.run(t, se)
					writeAfterCheckpoint(t, se, o, "a1")
					// A second checkpoint after more writes: the tail of the
					// first one must carry through.
					tr.run(t, se)
					writeAfterCheckpoint(t, se, o, "a2")

					se = restartEngine(t, se, crash, nil, dirs...)
					want, ttl := o.snapshot()
					verifyOracle(t, se, want, ttl)
					// And once more: the replayed state is itself durable.
					se = restartEngine(t, se, crash, nil, dirs...)
					verifyOracle(t, se, want, ttl)
					se.Close()
				})
			}
		}
	}
}

// liveCkptSteps are the test-hook points of a runtime checkpoint, in order
// (wal_checkpoint_live.go). The first three run with the flusher serving
// writes; the rest run inside the flusher, between batches.
var liveCkptSteps = []string{
	ckptStepFenced, ckptStepBuilt, ckptStepTailCopied,
	ckptStepSynced, ckptStepRenamed, ckptStepSwapped,
}

// TestLiveCheckpoint_CrashAtEachStep: at every step of a runtime checkpoint
// the on-disk state is copied (a power cut at that instant) and the copy must
// replay every write acknowledged before it. Writes made from inside the
// hook at the steps where the flusher still runs land in the tail the
// checkpoint has to carry. Then the checkpoint finishes, more writes follow,
// and the live engine is crash-restarted too.
func TestLiveCheckpoint_CrashAtEachStep(t *testing.T) {
	for _, disks := range []int{1, 3} {
		for _, step := range liveCkptSteps {
			t.Run(fmt.Sprintf("%ddisk/%s", disks, step), func(t *testing.T) {
				dirs := ckptDirs(t, disks)
				se := openCkptEngine(t, nil, dirs...)
				o := newCkptOracle()
				writeBase(t, se, o)

				var snapDirs []string
				var snapWant map[string]string
				var snapTTL map[string]bool
				var calls atomic.Int32
				se.setCheckpointTestHook(func(diskIdx int, s string) error {
					calls.Add(1)
					switch s {
					case ckptStepFenced, ckptStepBuilt, ckptStepTailCopied:
						// The flusher is running: these writes are acknowledged
						// and land after the checkpoint's fence.
						tag := fmt.Sprintf("in-%s-d%d", s, diskIdx)
						for i := 0; i < 20; i++ {
							o.put(t, se, fmt.Sprintf("%s-%02d", tag, i), tag, -1)
						}
						o.put(t, se, "base-050", tag, -1)
						o.del(t, se, fmt.Sprintf("base-%03d", 60+len(s)%10))
					}
					if s == step && diskIdx == disks-1 {
						snapWant, snapTTL = o.snapshot()
						snapDirs = snapshotDataDirs(t, dirs)
					}
					return nil
				})
				if err := se.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
				se.setCheckpointTestHook(nil)
				if got := calls.Load(); got != int32(len(liveCkptSteps)*disks) {
					t.Fatalf("hook ran %d times, want %d", got, len(liveCkptSteps)*disks)
				}
				if snapDirs == nil {
					t.Fatal("no snapshot taken")
				}

				// The power cut at the step.
				crashed := openCkptEngine(t, nil, snapDirs...)
				verifyOracle(t, crashed, snapWant, snapTTL)
				crashed.Close()

				// The live engine: more writes, then a crash restart.
				writeAfterCheckpoint(t, se, o, "post")
				se = restartEngine(t, se, true, nil, dirs...)
				want, ttl := o.snapshot()
				verifyOracle(t, se, want, ttl)
				se.Close()
			})
		}
	}
}

// TestLiveCheckpoint_FailureAtEachStepKeepsOldWAL: a checkpoint that fails
// before its rename leaves the old wal.log in place and the engine writing
// to it; nothing is lost, and the next checkpoint succeeds.
func TestLiveCheckpoint_FailureAtEachStepKeepsOldWAL(t *testing.T) {
	for _, step := range []string{ckptStepFenced, ckptStepBuilt, ckptStepTailCopied, ckptStepSynced} {
		t.Run(step, func(t *testing.T) {
			dirs := ckptDirs(t, 2)
			se := openCkptEngine(t, nil, dirs...)
			o := newCkptOracle()
			writeBase(t, se, o)
			injected := errors.New("injected checkpoint failure")
			se.setCheckpointTestHook(func(diskIdx int, s string) error {
				if s == step && diskIdx == 1 {
					return injected
				}
				return nil
			})
			if err := se.Checkpoint(); !errors.Is(err, injected) {
				t.Fatalf("Checkpoint error = %v, want the injected one", err)
			}
			se.setCheckpointTestHook(nil)
			for _, d := range dirs {
				if _, err := os.Stat(filepath.Join(d, "wal.log.ckpt")); !os.IsNotExist(err) {
					t.Fatalf("%s: temp checkpoint left behind (%v)", d, err)
				}
			}
			writeAfterCheckpoint(t, se, o, "after-fail")
			se = restartEngine(t, se, true, nil, dirs...)
			want, ttl := o.snapshot()
			verifyOracle(t, se, want, ttl)
			if err := se.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint after a failed one: %v", err)
			}
			writeAfterCheckpoint(t, se, o, "after-ok")
			se = restartEngine(t, se, true, nil, dirs...)
			want, ttl = o.snapshot()
			verifyOracle(t, se, want, ttl)
			se.Close()
		})
	}
}

// TestLiveCheckpoint_ConcurrentWritersStress: writers hammer the engine
// (puts, overwrites, deletes, TTL puts, MultiPut, INCR) while checkpoints and
// full backups run in a loop; then a crash restart must reproduce the last
// acknowledged state of every key. Each writer owns its keys, so the last
// acknowledged operation per key is well defined. It also measures the write
// stall a checkpoint causes.
func TestLiveCheckpoint_ConcurrentWritersStress(t *testing.T) {
	for _, disks := range []int{1, 3} {
		t.Run(fmt.Sprintf("%ddisk", disks), func(t *testing.T) {
			dirs := ckptDirs(t, disks)
			se := openCkptEngine(t, nil, dirs...)
			o := newCkptOracle()
			writeBase(t, se, o)

			const writers = 8
			dur := 1500 * time.Millisecond
			if testing.Short() {
				dur = 500 * time.Millisecond
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			var ops atomic.Int64
			var maxLatNs atomic.Int64
			var counters [writers]int64
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(w)))
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						k := fmt.Sprintf("w%d-%04d", w, rng.Intn(400))
						t0 := time.Now()
						switch r := rng.Intn(20); {
						case r < 11:
							o.put(t, se, k, fmt.Sprintf("v%d-%d", w, n), -1)
						case r < 14:
							o.del(t, se, k)
						case r < 16:
							o.put(t, se, k, fmt.Sprintf("t%d-%d", w, n), 3600)
						case r < 18:
							o.multiPut(t, se, fmt.Sprintf("w%d-mp%02d", w, rng.Intn(20)), 16)
						default:
							ck := fmt.Sprintf("w%d-ctr", w)
							v, err := se.Increment(ck, 1, -1)
							if err != nil {
								t.Errorf("Increment: %v", err)
								return
							}
							counters[w]++
							if v != counters[w] {
								t.Errorf("Increment = %d, want %d", v, counters[w])
								return
							}
							o.mu.Lock()
							o.want[ck] = fmt.Sprint(v)
							o.ttl[ck] = false
							o.mu.Unlock()
						}
						if lat := time.Since(t0).Nanoseconds(); lat > maxLatNs.Load() {
							maxLatNs.Store(lat)
						}
						ops.Add(1)
					}
				}(w)
			}

			var ckpts, backups int
			var stalls []time.Duration
			deadline := time.Now().Add(dur)
			for time.Now().Before(deadline) {
				if ckpts%4 == 3 {
					if _, err := NewBackupEngine(se).FullBackup(t.TempDir()); err != nil {
						t.Fatalf("FullBackup: %v", err)
					}
					backups++
				} else if err := se.Checkpoint(); err != nil {
					t.Fatalf("Checkpoint: %v", err)
				}
				ckpts++
				stalls = append(stalls, se.LastCheckpointStats().MaxStall)
				time.Sleep(20 * time.Millisecond)
			}
			close(stop)
			wg.Wait()
			if t.Failed() {
				return
			}
			sort.Slice(stalls, func(i, j int) bool { return stalls[i] < stalls[j] })
			t.Logf("%d ops, %d checkpoints (%d via backup); per-checkpoint flusher pause p50=%v max=%v; max write latency %v",
				ops.Load(), ckpts, backups, stalls[len(stalls)/2], stalls[len(stalls)-1], time.Duration(maxLatNs.Load()))

			se = restartEngine(t, se, true, nil, dirs...)
			want, ttl := o.snapshot()
			verifyOracle(t, se, want, ttl)
			se = restartEngine(t, se, false, nil, dirs...)
			verifyOracle(t, se, want, ttl)
			se.Close()
		})
	}
}

// TestLiveCheckpoint_RaftMarkerCut: a runtime checkpoint in raft mode keeps
// the applied-index cut exact: records after the newest marker are still cut
// after the checkpoint, records before it survive, and a checkpoint taken
// right after a crash-restart that cut records (dead ranges) does not cut its
// own records.
func TestLiveCheckpoint_RaftMarkerCut(t *testing.T) {
	for _, disks := range []int{1, 3} {
		t.Run(fmt.Sprintf("%ddisk", disks), func(t *testing.T) {
			dirs := ckptDirs(t, disks)
			mut := func(c *StorageConfig) { c.RaftAppliedIndex = true }
			se := openCkptEngine(t, mut, dirs...)
			se.SetCheckpointGate(func(fn func()) { fn() })
			mustPut(t, se, "a", "1")
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 1, Term: 1}, false); err != nil {
				t.Fatal(err)
			}
			if err := se.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			mustPut(t, se, "b", "2")
			if err := se.SetRaftAppliedState(AppliedIndexState{Index: 2, Term: 1}, true); err != nil {
				t.Fatal(err)
			}
			mustPut(t, se, "c", "3") // entry 3: no marker yet — cut at a crash
			if err := se.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			mustPut(t, se, "d", "4") // entry 4: no marker either

			se = restartEngine(t, se, true, mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 2, Term: 1})
			wantValue(t, se, "a", "1")
			wantValue(t, se, "b", "2")
			wantValue(t, se, "d", "")

			// A checkpoint straight after the cut, with no write in between,
			// must not land its records inside the cut's dead range.
			se.SetCheckpointGate(func(fn func()) { fn() })
			if err := se.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			se = restartEngine(t, se, true, mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 2, Term: 1})
			wantValue(t, se, "a", "1")
			wantValue(t, se, "b", "2")
			wantValue(t, se, "d", "")

			// The same for the clean-shutdown checkpoint: a crash that cuts,
			// then Close with no write in between (it used to write every
			// record at the dead range's upper bound — all dropped next open).
			mustPut(t, se, "e", "5") // no marker: cut at the crash
			se = restartEngine(t, se, true, mut, dirs...)
			wantValue(t, se, "e", "")
			se = restartEngine(t, se, false, mut, dirs...)
			wantApplied(t, se, AppliedIndexState{Index: 2, Term: 1})
			wantValue(t, se, "a", "1")
			wantValue(t, se, "b", "2")
			se.Close()
		})
	}
}

// TestLiveCheckpoint_ReplicatedTombstonesKept: in replicated mode a runtime
// checkpoint keeps the tombstones a replica has not acknowledged, and deletes
// made after it reach the catch-up feed after a crash restart.
func TestLiveCheckpoint_ReplicatedTombstonesKept(t *testing.T) {
	dirs := ckptDirs(t, 1)
	se := openCkptEngine(t, nil, dirs...)
	se.SetReplicaWatermark("r1", time.Now().UnixMicro()-1)
	mustPut(t, se, "x", "1")
	mustPut(t, se, "y", "1")
	if err := se.Delete("x"); err != nil {
		t.Fatal(err)
	}
	if err := se.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := se.Delete("y"); err != nil {
		t.Fatal(err)
	}
	se = restartEngine(t, se, true, nil, dirs...)
	defer se.Close()
	dels := map[string]bool{}
	for _, ev := range se.ChangesSince(0, 0).Events {
		if ev.Op == "DEL" {
			dels[ev.Key] = true
		}
	}
	if !dels["x"] || !dels["y"] {
		t.Fatalf("catch-up feed after restart has deletes %v, want x and y", dels)
	}
}

// TestLiveBackup_ConsistentUnderWrites: a full backup taken while writers run
// restores to an engine in which every key it holds is readable (no WAL
// record points past the copied VLog), which holds every write acknowledged
// before the backup started, and whose manifest version covers them.
func TestLiveBackup_ConsistentUnderWrites(t *testing.T) {
	for _, disks := range []int{1, 3} {
		t.Run(fmt.Sprintf("%ddisk", disks), func(t *testing.T) {
			dirs := ckptDirs(t, disks)
			se := openCkptEngine(t, nil, dirs...)
			o := newCkptOracle()
			writeBase(t, se, o)

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for w := 0; w < 4; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for n := 0; ; n++ {
						select {
						case <-stop:
							return
						default:
						}
						o.put(t, se, fmt.Sprintf("bw%d-%04d", w, n%500), fmt.Sprintf("v%d", n), -1)
						if n%7 == 0 {
							o.multiPut(t, se, fmt.Sprintf("bw%d-mp", w), 32)
						}
					}
				}(w)
			}
			time.Sleep(50 * time.Millisecond)
			before, beforeTTL := o.snapshot()
			backupDir := t.TempDir()
			m, err := NewBackupEngine(se).FullBackup(backupDir)
			if err != nil {
				t.Fatalf("FullBackup: %v", err)
			}
			time.Sleep(50 * time.Millisecond)
			close(stop)
			wg.Wait()

			restoreDirs := ckptDirs(t, disks)
			if err := Restore([]*BackupManifest{m}, []string{backupDir}, restoreDirs); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			rs := openCkptEngine(t, nil, restoreDirs...)
			defer rs.Close()
			for _, k := range rs.ScanKeys() {
				if _, err := rs.Get(k); err != nil {
					t.Fatalf("restored key %s unreadable: %v", k, err)
				}
			}
			// Every write acknowledged before the backup began is in it —
			// either with that value or a later one from the same writer.
			for k, v := range before {
				got, err := rs.Get(k)
				if v == "" {
					continue
				}
				if err != nil {
					t.Fatalf("restored backup lost %s (acknowledged before the backup started): %v", k, err)
				}
				if strings.HasPrefix(k, "base-") && string(got) != v {
					t.Fatalf("restored %s = %q, want %q", k, got, v)
				}
				_ = beforeTTL
			}
			if m.EngineVersion == 0 || m.EngineVersion > se.GetVersion() {
				t.Fatalf("manifest EngineVersion %d outside (0, %d]", m.EngineVersion, se.GetVersion())
			}

			// And the live engine kept every write, backup or not.
			se = restartEngine(t, se, true, nil, dirs...)
			want, ttl := o.snapshot()
			verifyOracle(t, se, want, ttl)
			se.Close()
		})
	}
}

// TestLiveCheckpoint_PITRArchiverFollowsSwap: the WAL archiver keeps
// archiving across runtime checkpoints — every write is archived exactly
// once, and the checkpoint's own compacted records are not archived.
func TestLiveCheckpoint_PITRArchiverFollowsSwap(t *testing.T) {
	dirs := ckptDirs(t, 2)
	archiveDir := t.TempDir()
	se := openCkptEngine(t, func(c *StorageConfig) {
		c.ArchiveDir = archiveDir
		c.ArchiveIntervalMs = 5
	}, dirs...)
	defer se.Close()
	arch, err := StartWALArchiver(se)
	if err != nil {
		t.Fatal(err)
	}
	written := map[string]int{}
	put := func(k string) {
		mustPut(t, se, k, "v")
		written[k]++
	}
	for round := 0; round < 4; round++ {
		for i := 0; i < 50; i++ {
			put(fmt.Sprintf("r%d-%02d", round, i))
		}
		if round == 1 {
			time.Sleep(30 * time.Millisecond) // let a pass run mid-way
		}
		if err := se.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if round == 2 {
			if err := se.Checkpoint(); err != nil { // two swaps between passes
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < 50; i++ {
		put(fmt.Sprintf("tail-%02d", i))
	}
	arch.Stop()

	segs, err := ListArchiveSegments(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	archived := map[string]int{}
	for _, s := range segs {
		ents, err := replayWAL(s.SegPath)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			archived[e.key]++
		}
	}
	for k, n := range written {
		if archived[k] != n {
			t.Errorf("%s archived %d times, written %d times", k, archived[k], n)
		}
	}
	for k := range archived {
		if _, ok := written[k]; !ok {
			t.Errorf("unexpected archived key %q", k)
		}
	}
}
