package integration_test

// raft_applied_test.go — exactly-once apply across crashes on a real
// 3-process raft cluster (CLAUDE.md invariant 65): nodes are SIGKILLed at
// random points of a heavy INCR load, restarted, and every node's counters
// must equal the leader's, which must lie within [acknowledged,
// acknowledged + unknown].  Before the applied index, a restarted node
// re-applied its retained raft log over its engine and its counters drifted
// up from the leader's (and a restarted leader's from the acknowledged
// count).

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// textCmd sends one text-protocol command and returns the reply line.
func textCmd(c net.Conn, r *bufio.Reader, cmd string) (string, error) {
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(cmd + "\n")); err != nil {
		return "", err
	}
	line, err := r.ReadString('\n')
	return strings.TrimRight(line, "\r\n"), err
}

func getCounter(addr, key string) (int64, error) {
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	line, err := textCmd(c, bufio.NewReader(c), "GET "+key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(line, 10, 64)
}

func TestRaftApplied_CrashDuringIncrLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("distributed cluster test skipped in -short mode")
	}
	procs := startSnapCluster(t)
	const workers = 8
	var acked, unknown [workers]atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("ctr%d", w)
			var c net.Conn
			var r *bufio.Reader
			for {
				select {
				case <-stop:
					if c != nil {
						c.Close()
					}
					return
				default:
				}
				if c == nil {
					addrs := addrsOf(procs)
					a := addrs[rand.Intn(len(addrs))]
					cc, err := net.DialTimeout("tcp", a, time.Second)
					if err != nil {
						time.Sleep(50 * time.Millisecond)
						continue
					}
					c, r = cc, bufio.NewReader(cc)
				}
				line, err := textCmd(c, r, "INCR "+key)
				switch {
				case err != nil:
					unknown[w].Add(1) // may or may not have committed
					c.Close()
					c = nil
				case strings.HasPrefix(line, "ERR"):
					// MOVED / not leader / no quorum: rejected before the
					// log, except a timeout, which may still commit.
					if strings.Contains(line, "MOVED") || strings.Contains(line, "not the Raft leader") {
						if i := strings.Index(line, "MOVED "); i >= 0 {
							// "MOVED <addr> <leaderID>" or "MOVED - (leader unknown…)"
							c.Close()
							c = nil
							if f := strings.Fields(line[i+6:]); len(f) > 0 && f[0] != "-" {
								if cc, err := net.DialTimeout("tcp", f[0], time.Second); err == nil {
									c, r = cc, bufio.NewReader(cc)
								}
							}
						}
					} else {
						unknown[w].Add(1)
					}
					time.Sleep(5 * time.Millisecond)
				default:
					if _, err := strconv.ParseInt(line, 10, 64); err != nil {
						unknown[w].Add(1)
					} else {
						acked[w].Add(1)
					}
				}
			}
		}(w)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	const rounds = 6
	for round := 0; round < rounds; round++ {
		time.Sleep(time.Duration(300+rng.Intn(1500)) * time.Millisecond)
		p := procs[rng.Intn(len(procs))]
		t.Logf("round %d: SIGKILL %s", round+1, p.id)
		p.kill()
		time.Sleep(time.Duration(200+rng.Intn(800)) * time.Millisecond)
		p.start()
		p.waitReady()
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()

	// Every node must converge to the leader's counters.
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, leaderAddr := waitForRaftLeader(t, addrsOf(procs), 20*time.Second)
		var want [workers]int64
		ok := true
		var diff []string
		for w := 0; w < workers; w++ {
			v, err := getCounter(leaderAddr, fmt.Sprintf("ctr%d", w))
			if err != nil {
				ok = false
				diff = append(diff, fmt.Sprintf("leader ctr%d: %v", w, err))
				continue
			}
			want[w] = v
			for _, a := range addrsOf(procs) {
				got, err := getCounter(a, fmt.Sprintf("ctr%d", w))
				if err != nil || got != v {
					ok = false
					diff = append(diff, fmt.Sprintf("%s ctr%d=%d (%v), leader %d", a, w, got, err, v))
				}
			}
		}
		if ok {
			var total, totalAcked, totalUnknown int64
			for w := 0; w < workers; w++ {
				a, u := acked[w].Load(), unknown[w].Load()
				total += want[w]
				totalAcked += a
				totalUnknown += u
				if want[w] < a || want[w] > a+u {
					t.Errorf("ctr%d = %d on every node, want within [%d acknowledged, %d acknowledged + unknown]", w, want[w], a, a+u)
				}
			}
			t.Logf("%d rounds of SIGKILL: every node holds the leader's counters, total %d (acknowledged %d, unknown %d)",
				rounds, total, totalAcked, totalUnknown)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes do not agree with the leader after the crashes:\n%s", strings.Join(diff, "\n"))
		}
		time.Sleep(500 * time.Millisecond)
	}
}
