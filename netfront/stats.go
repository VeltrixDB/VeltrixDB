//go:build cgo && go1.21

package netfront

/*
#include "netfront.h"
*/
import "C"

import (
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"unsafe"
)

// hist is a log2 latency histogram with the same buckets as the C++ side
// (netfront.h VXNF_HIST_*): bucket i counts [2^(i-1), 2^i) µs.
type hist struct {
	b [C.VXNF_HIST_BUCKETS]atomic.Uint64
}

func (h *hist) addNS(ns uint64) {
	i := bits.Len64(ns / 1000)
	if i >= len(h.b) {
		i = len(h.b) - 1
	}
	h.b[i].Add(1)
}

func (h *hist) snapshot() (out [C.VXNF_HIST_BUCKETS]uint64) {
	for i := range h.b {
		out[i] = h.b[i].Load()
	}
	return out
}

// stageStats times each hop a request takes through the front-end, so a tail
// can be attributed to a stage instead of guessed at:
//
//	cbEnter    C++ calling vxnfExec → Go body running (waiting for a P)
//	exec       one vxnfExec: every inline answer of a loop iteration
//	putSched   PUT goroutine created → running
//	putExec    engine Put/MultiPut of one PUT group
//	deferSched deferred-connection goroutine created → running
//	deferExec  one deferred connection's chain (disk reads + anything behind them)
//
// plus, from C++, wake (goroutine vxnf_respond → loop appending the answer)
// and iter (one whole loop iteration).
type stageStats struct {
	cbEnter, exec, putSched, putExec, deferSched, deferExec hist
}

// upperUS is bucket i's exclusive upper bound in µs.
func upperUS(i int) uint64 {
	if i == 0 {
		return 1
	}
	return 1 << uint(i)
}

// quantiles returns n and the bucket upper bounds of p50/p99/p99.9/max.
func quantiles(b [C.VXNF_HIST_BUCKETS]uint64) (n uint64, q [4]uint64) {
	for _, c := range b {
		n += c
	}
	if n == 0 {
		return 0, q
	}
	targets := [3]float64{0.50, 0.99, 0.999}
	var cum uint64
	t := 0
	for i, c := range b {
		cum += c
		for t < 3 && float64(cum) >= targets[t]*float64(n) {
			q[t] = upperUS(i)
			t++
		}
		if c > 0 {
			q[3] = upperUS(i)
		}
	}
	return n, q
}

func fmtUS(us uint64) string {
	if us >= 1000 {
		return fmt.Sprintf("%.3gms", float64(us)/1000)
	}
	return fmt.Sprintf("%dµs", us)
}

// LatencyReport is one line per stage (safe before or after Close): count and the upper bounds of the
// log2 buckets holding p50 / p99 / p99.9 / max. Buckets are powers of two,
// so "p99<=8ms" means the 99th percentile is in [4, 8) ms.
func (s *Server) LatencyReport() string {
	var wake, iter [C.VXNF_HIST_BUCKETS]uint64
	if s.closed.Load() {
		wake, iter = s.finalWake, s.finalIter // s.c is freed
	} else {
		C.vxnf_hist(s.c, C.VXNF_HIST_WAKE, (*C.uint64_t)(unsafe.Pointer(&wake[0])))
		C.vxnf_hist(s.c, C.VXNF_HIST_ITER, (*C.uint64_t)(unsafe.Pointer(&iter[0])))
	}
	rows := []struct {
		name string
		b    [C.VXNF_HIST_BUCKETS]uint64
	}{
		{"iter", iter},
		{"cb_enter", s.st.cbEnter.snapshot()},
		{"exec", s.st.exec.snapshot()},
		{"put_sched", s.st.putSched.snapshot()},
		{"put_exec", s.st.putExec.snapshot()},
		{"wake", wake},
		{"defer_sched", s.st.deferSched.snapshot()},
		{"defer_exec", s.st.deferExec.snapshot()},
	}
	var sb strings.Builder
	for _, r := range rows {
		n, q := quantiles(r.b)
		fmt.Fprintf(&sb, "[netfront] stage=%-11s n=%-9d p50<=%-7s p99<=%-7s p999<=%-7s max<=%s\n",
			r.name, n, fmtUS(q[0]), fmtUS(q[1]), fmtUS(q[2]), fmtUS(q[3]))
	}
	return sb.String()
}
