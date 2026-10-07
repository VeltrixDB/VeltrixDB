package consensus

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestRaftPipeline_TCPThroughput measures Submit throughput of a 3-node
// cluster over the real TCP transport (loopback), with every node's raft log
// fsync replaced by a fixed delay + fsync(2) so the three nodes behave like
// three machines with independent disks (on one laptop they would otherwise
// share one drive's cache flushes; VELTRIX_RAFT_FSYNC_US=-1 keeps the real
// os.File.Sync).  Env-gated (VELTRIX_RAFT_BENCH=1):
//
//	VELTRIX_RAFT_BENCH=1 VELTRIX_RAFT_FSYNC_US=1000 VELTRIX_RAFT_CLIENTS=16 \
//	  go test ./consensus/ -run TestRaftPipeline_TCPThroughput -v
func TestRaftPipeline_TCPThroughput(t *testing.T) {
	if os.Getenv("VELTRIX_RAFT_BENCH") != "1" {
		t.Skip("set VELTRIX_RAFT_BENCH=1")
	}
	fsyncUS := envInt("VELTRIX_RAFT_FSYNC_US", 1000)
	clients := envInt("VELTRIX_RAFT_CLIENTS", 16)
	window := envInt("VELTRIX_RAFT_WINDOW", 0)
	dur := time.Duration(envInt("VELTRIX_RAFT_SECONDS", 3)) * time.Second

	ids := []string{"b1", "b2", "b3"}
	servers := make([]*RPCServer, 3)
	addrs := map[string]string{}
	handlers := make([]*lateHandler, 3)
	for i, id := range ids {
		handlers[i] = &lateHandler{}
		s, err := NewRPCServer("127.0.0.1:0", handlers[i])
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = s
		addrs[id] = s.Addr()
		go s.ListenAndServe()
	}
	nodes := make([]*RaftNode, 3)
	for i, id := range ids {
		peers := map[string]string{}
		var pids []string
		for _, p := range ids {
			if p != id {
				peers[p] = addrs[p]
				pids = append(pids, p)
			}
		}
		n, err := NewRaftNodeWithOptions(id, pids, t.TempDir(), &mockSM{}, NewTCPTransport(peers),
			Options{PipelineWindow: window})
		if err != nil {
			t.Fatal(err)
		}
		if fsyncUS >= 0 { // < 0: keep the real os.File.Sync (F_FULLFSYNC on darwin)
			d := time.Duration(fsyncUS) * time.Microsecond
			n.store.setSyncHook(func(f *os.File) error {
				time.Sleep(d)
				return syscall.Fsync(int(f.Fd()))
			})
		}
		handlers[i].set(n)
		nodes[i] = n
	}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
		for _, n := range nodes {
			n.Stop()
		}
	}()
	lead := waitForStableLeader(nodes, 10*time.Second)
	if lead < 0 {
		t.Fatal("no leader")
	}
	var ops atomic.Int64
	var mu sync.Mutex
	var lats []time.Duration
	stop := time.Now().Add(dur)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			var mine []time.Duration
			for i := 0; time.Now().Before(stop); i++ {
				t0 := time.Now()
				if err := nodes[lead].Submit([]byte(fmt.Sprintf("c%d-%d-%s", c, i, string(make([]byte, 1000))))); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
				mine = append(mine, time.Since(t0))
				ops.Add(1)
			}
			mu.Lock()
			lats = append(lats, mine...)
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	n := len(lats)
	if n == 0 {
		t.Fatal("no ops")
	}
	t.Logf("fsync=%dµs clients=%d window=%d: %.0f commits/s  avg=%v p50=%v p99=%v  leader fsyncs=%d",
		fsyncUS, clients, nodes[lead].pipelineWindow, float64(ops.Load())/dur.Seconds(),
		sum/time.Duration(n), lats[n/2], lats[n*99/100], nodes[lead].store.syncCalls.Load())
}

// lateHandler forwards to a RaftNode installed after the server started.
type lateHandler struct {
	mu sync.Mutex
	n  *RaftNode
}

func (h *lateHandler) set(n *RaftNode) { h.mu.Lock(); h.n = n; h.mu.Unlock() }
func (h *lateHandler) get() *RaftNode  { h.mu.Lock(); defer h.mu.Unlock(); return h.n }
func (h *lateHandler) HandleRequestVote(a RequestVoteArgs) RequestVoteReply {
	if n := h.get(); n != nil {
		return n.HandleRequestVote(a)
	}
	return RequestVoteReply{}
}
func (h *lateHandler) HandleAppendEntries(a AppendEntriesArgs) AppendEntriesReply {
	if n := h.get(); n != nil {
		return n.HandleAppendEntries(a)
	}
	return AppendEntriesReply{}
}
func (h *lateHandler) HandleAppendEntriesAsync(a AppendEntriesArgs, respond func(AppendEntriesReply)) {
	if n := h.get(); n != nil {
		n.HandleAppendEntriesAsync(a, respond)
		return
	}
	respond(AppendEntriesReply{})
}
func (h *lateHandler) HandleInstallSnapshot(a InstallSnapshotArgs) InstallSnapshotReply {
	if n := h.get(); n != nil {
		return n.HandleInstallSnapshot(a)
	}
	return InstallSnapshotReply{}
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}
