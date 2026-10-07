package consensus

// pipeline_mode_test.go — what differs between Options.Pipeline off (window 1,
// paced sending) and on (window N, eager sending).

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// inflightView wraps a chaosView and records, per (sender, peer), the largest
// number of entry-carrying AppendEntries in flight at once.
type inflightView struct {
	*chaosView
	mu  sync.Mutex
	cur map[string]int
	max map[string]int
}

func (v *inflightView) SendAppendEntriesAsync(peer string, args AppendEntriesArgs, done func(AppendEntriesReply, error)) {
	if len(args.Entries) == 0 {
		v.chaosView.SendAppendEntriesAsync(peer, args, done)
		return
	}
	v.mu.Lock()
	v.cur[peer]++
	if v.cur[peer] > v.max[peer] {
		v.max[peer] = v.cur[peer]
	}
	v.mu.Unlock()
	v.chaosView.SendAppendEntriesAsync(peer, args, func(r AppendEntriesReply, err error) {
		v.mu.Lock()
		v.cur[peer]--
		v.mu.Unlock()
		done(r, err)
	})
}

func (v *inflightView) maxInflight() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	m := 0
	for _, n := range v.max {
		if n > m {
			m = n
		}
	}
	return m
}

// TestPipeline_InFlightPerPeer: with the pipeline off a leader never has more
// than one entry-carrying AppendEntries in flight to a follower; with it on
// (window 4) it uses the window, and never exceeds it.
func TestPipeline_InFlightPerPeer(t *testing.T) {
	forEachPipelineMode(t, func(t *testing.T, pipeline bool) {
		ct := &chaosTransport{
			nodes: make(map[string]*RaftNode), cut: make(map[string]bool),
			rng: rand.New(rand.NewSource(7)), maxDelay: 4 * time.Millisecond,
		}
		ids := []string{fmt.Sprintf("w%v-a", pipeline), fmt.Sprintf("w%v-b", pipeline), fmt.Sprintf("w%v-c", pipeline)}
		nodes := make([]*RaftNode, len(ids))
		views := make([]*inflightView, len(ids))
		for i, id := range ids {
			var peers []string
			for _, p := range ids {
				if p != id {
					peers = append(peers, p)
				}
			}
			views[i] = &inflightView{chaosView: &chaosView{c: ct, self: id}, cur: map[string]int{}, max: map[string]int{}}
			n, err := NewRaftNodeWithOptions(id, peers, t.TempDir(), &mockSM{}, views[i],
				Options{Pipeline: pipeline, PipelineWindow: 4})
			if err != nil {
				t.Fatal(err)
			}
			nodes[i] = n
			ct.mu.Lock()
			ct.nodes[id] = n
			ct.mu.Unlock()
		}
		defer func() {
			ct.closed.Store(true)
			for _, n := range nodes {
				n.Stop()
			}
			ct.wg.Wait()
		}()
		lead := waitForLeader(nodes, 10*time.Second)
		if lead < 0 {
			t.Fatal("no leader elected")
		}
		var acked atomic.Int64
		var wg sync.WaitGroup
		deadline := time.Now().Add(700 * time.Millisecond)
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; time.Now().Before(deadline); i++ {
					if nodes[lead].Submit([]byte(fmt.Sprintf("w%d-%d", w, i))) == nil {
						acked.Add(1)
					}
				}
			}(w)
		}
		wg.Wait()
		if acked.Load() == 0 {
			t.Fatal("no command acknowledged")
		}
		got := views[lead].maxInflight()
		t.Logf("pipeline=%v: %d acknowledged, max in flight per peer %d", pipeline, acked.Load(), got)
		switch {
		case !pipeline && got != 1:
			t.Fatalf("pipeline off: max entry AppendEntries in flight per peer = %d, want 1", got)
		case pipeline && got > 4:
			t.Fatalf("pipeline on (window 4): %d in flight", got)
		case pipeline && got < 2:
			t.Fatalf("pipeline on (window 4): never more than %d in flight — not pipelining", got)
		}
	})
}

// TestPipeline_LoneSubmitNotHeartbeatPaced: with nothing else going on, a
// Submit commits in about two fsyncs plus a round trip — never by waiting for
// the 50 ms heartbeat.  Pipeline off sends only when a leader fsync completes
// (or a reply arrives while no fsync is pending); a missing wake-up there
// would show up as heartbeat-paced commits.
func TestPipeline_LoneSubmitNotHeartbeatPaced(t *testing.T) {
	forEachPipelineMode(t, func(t *testing.T, pipeline bool) {
		hub := newMockTransport()
		ids := []string{fmt.Sprintf("hb%v-a", pipeline), fmt.Sprintf("hb%v-b", pipeline), fmt.Sprintf("hb%v-c", pipeline)}
		nodes := make([]*RaftNode, len(ids))
		for i, id := range ids {
			var peers []string
			for _, p := range ids {
				if p != id {
					peers = append(peers, p)
				}
			}
			n, err := NewRaftNodeWithOptions(id, peers, t.TempDir(), &mockSM{}, hub.forNode(id), Options{Pipeline: pipeline})
			if err != nil {
				t.Fatal(err)
			}
			nodes[i] = n
			hub.register(id, n)
		}
		defer func() {
			for _, n := range nodes {
				n.Stop()
			}
		}()
		lead := waitForLeader(nodes, 10*time.Second)
		if lead < 0 {
			t.Fatal("no leader elected")
		}
		if err := nodes[lead].Submit([]byte("warm-up")); err != nil {
			t.Fatal(err)
		}
		var took []time.Duration
		for i := 0; i < 21; i++ {
			start := time.Now()
			if err := nodes[lead].Submit([]byte(fmt.Sprintf("lone-%d", i))); err != nil {
				t.Fatal(err)
			}
			took = append(took, time.Since(start))
		}
		sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
		median := took[len(took)/2]
		t.Logf("pipeline=%v: lone Submit median %v (min %v, max %v)", pipeline, median, took[0], took[len(took)-1])
		if median >= heartbeatInterval/2 {
			t.Fatalf("lone Submit median %v — commits are waiting for heartbeats (%v)", median, heartbeatInterval)
		}
	})
}
