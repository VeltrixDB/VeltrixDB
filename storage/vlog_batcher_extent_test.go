package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// The batcher reserves its whole extent with ONE vl.end.Add at Commit, after
// staging, instead of one Add per 4 KB block during staging. That is what
// makes a batch's blocks contiguous on disk and lets Flush write them with a
// single pwrite — but it also widens the window in which a batch holds
// unreserved offsets, so concurrent batches overlapping would now corrupt
// whole extents rather than single blocks.
//
// Each key gets a value that encodes its own identity, so an overlap shows up
// as a value belonging to some other key rather than as a read error.
func writeConcurrentBatches(t *testing.T, se *StorageEngine, workers, perBatch int) map[string][]byte {
	t.Helper()
	want := make(map[string][]byte, workers*perBatch)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			reqs := make([]MultiPutRequest, perBatch)
			for j := range reqs {
				k := fmt.Sprintf("extent-%d-%d", w, j)
				// Long enough that a wrong offset lands mid-value rather than
				// on a header, and varied in length so records pack at
				// different intra-block positions.
				v := bytes.Repeat([]byte(k+"|"), 3+j%17)
				reqs[j] = MultiPutRequest{Key: k, Value: v, TTL: -1}
			}
			errs := se.MultiPut(reqs)
			mu.Lock()
			defer mu.Unlock()
			for j, err := range errs {
				if err != nil {
					t.Errorf("MultiPut w=%d j=%d: %v", w, j, err)
					continue
				}
				want[reqs[j].Key] = reqs[j].Value
			}
		}(w)
	}
	wg.Wait()
	return want
}

func verifyAll(t *testing.T, se *StorageEngine, want map[string][]byte, when string) {
	t.Helper()
	for k, v := range want {
		got, err := se.Get(k)
		if err != nil {
			t.Fatalf("Get %s %s: %v", k, when, err)
		}
		if !bytes.Equal(got, v) {
			t.Fatalf("key %s %s: got %q, want %q — concurrent batch extents overlap",
				k, when, got, v)
		}
	}
}

func TestMultiPut_ConcurrentBatchExtentsDoNotOverlap(t *testing.T) {
	cfg := testStorageConfig(t.TempDir())
	se, err := NewStorageEngine(cfg)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	<-se.ReplayDone

	want := writeConcurrentBatches(t, se, 16, 256)
	if len(want) != 16*256 {
		t.Fatalf("wrote %d keys, want %d", len(want), 16*256)
	}
	verifyAll(t, se, want, "from the live engine")
}

// Same writes, then a dirty restart: the engine is never closed, so the WAL is
// replayed rather than read back from a checkpoint and every value is served
// from the VLog with a cold cache. This is what catches a vl.end that is
// re-seeded too low after restart — new writes would land on top of live
// records (invariant 27) — and a packed flag lost across replay (invariant 28).
func TestMultiPut_ConcurrentBatchExtentsSurviveDirtyRestart(t *testing.T) {
	dir := t.TempDir()

	// Deliberately not closed: simulates a process kill. MultiPut has already
	// returned, so WAL and VLog are both fdatasync'd and no checkpoint has
	// been written over the WAL.
	se1, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	<-se1.ReplayDone
	want := writeConcurrentBatches(t, se1, 8, 256)

	se2, err := NewStorageEngine(testStorageConfig(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { se2.Close() })
	<-se2.ReplayDone

	verifyAll(t, se2, want, "after dirty restart")

	// vl.end must have been seeded past every live record: a fresh write that
	// landed on top of one would corrupt it.
	fresh := map[string][]byte{}
	for i := 0; i < 512; i++ {
		k := fmt.Sprintf("post-restart-%d", i)
		v := bytes.Repeat([]byte(k+"|"), 5)
		if err := se2.Put(k, v, -1); err != nil {
			t.Fatalf("Put %s after restart: %v", k, err)
		}
		fresh[k] = v
	}
	verifyAll(t, se2, want, "after post-restart writes")
	verifyAll(t, se2, fresh, "post-restart write readback")
}
