//go:build cgo && go1.21

// Package netfront is VeltrixDB's C++ network front-end: per-core event
// loops (io_uring on Linux, poll() elsewhere) serving the binary protocol's
// hot commands — PUT GET DEL PING MPUT MGET — and handing every request that
// arrived in one loop iteration to Go in a single call. See netfront.h for
// the model and netfront.cpp for the memory-safety rules.
//
// It is opt-in (cmd/server --net). The Go server remains the default and the
// only one that serves the text protocol, AUTH, TLS and the extended binary
// commands.
package netfront

/*
#cgo CXXFLAGS: -std=c++17 -O2
#cgo linux LDFLAGS: -lstdc++ -lpthread
#include <stdlib.h>
#include "netfront.h"
*/
import "C"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime/cgo"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/VeltrixDB/veltrixdb/storage"
)

// Available reports whether this build contains the front-end (it needs cgo).
const Available = true

// Backend is what the front-end executes requests against. *storage.StorageEngine
// satisfies it; standalone mode only (no Raft or replication routing).
type Backend interface {
	Get(key string) ([]byte, error)
	MultiGet(keys []string) []storage.MultiGetResult
	Put(key string, value []byte, ttl int32) error
	MultiPut(reqs []storage.MultiPutRequest) []error
	Delete(key string) error
}

// Config for Start.
type Config struct {
	Addr      string // host:port, e.g. ":9000"; port 0 picks one
	Threads   int    // event loops; <= 0 means 1
	IOBackend string // "auto" (io_uring if available, else poll), "uring", "poll"
	RingDepth int    // io_uring SQ depth per loop; <= 0 means 4096
}

// Server is a running front-end.
type Server struct {
	c      *C.vxnf_server
	h      cgo.Handle
	be     Backend
	writes sync.WaitGroup // in-flight write and deferred-read goroutines; Close waits for them
	once   sync.Once
	st     stageStats

	deferReads bool // answer disk-bound GET/MGET from a goroutine (DeferReadsEnv)

	closed               atomic.Bool
	finalWake, finalIter [C.VXNF_HIST_BUCKETS]uint64 // C++ histograms, captured before vxnf_free
}

// DeferReadsEnv=0 answers every GET and MGET inline on the loop thread, disk
// reads included (the behaviour before deferral existed) — for A/B runs.
const DeferReadsEnv = "VELTRIXDB_NET_DEFER_READS"

// Start binds cfg.Addr and starts the event loops.
func Start(cfg Config, be Backend) (*Server, error) {
	host, portStr, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("netfront: addr %q: %w", cfg.Addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("netfront: port %q: %w", portStr, err)
	}
	var backend C.int
	switch cfg.IOBackend {
	case "", "auto":
		backend = C.VXNF_BACKEND_AUTO
	case "uring":
		backend = C.VXNF_BACKEND_URING
	case "poll":
		backend = C.VXNF_BACKEND_POLL
	default:
		return nil, fmt.Errorf("netfront: unknown I/O backend %q (auto, uring, poll)", cfg.IOBackend)
	}

	s := &Server{be: be, deferReads: os.Getenv(DeferReadsEnv) != "0"}
	s.h = cgo.NewHandle(s)
	chost := C.CString(host)
	defer C.free(unsafe.Pointer(chost))
	var errbuf [256]C.char
	c := C.vxnf_start(C.vxnf_config{
		host:       chost,
		port:       C.int(port),
		threads:    C.int(cfg.Threads),
		backend:    backend,
		ring_depth: C.int(cfg.RingDepth),
		handle:     C.uintptr_t(s.h),
	}, &errbuf[0], C.size_t(len(errbuf)))
	if c == nil {
		s.h.Delete()
		return nil, errors.New("netfront: " + C.GoString(&errbuf[0]))
	}
	s.c = c
	return s, nil
}

// Port is the bound TCP port.
func (s *Server) Port() int { return int(C.vxnf_port(s.c)) }

// IOBackend is the backend actually running: "io_uring" or "poll".
func (s *Server) IOBackend() string { return C.GoString(C.vxnf_backend(s.c)) }

// Stats returns loop iterations, requests served and Go batches (vxnfExec
// calls). requests/batches is the batching factor the front-end achieves.
func (s *Server) Stats() (iterations, requests, batches uint64) {
	return uint64(C.vxnf_stat_iterations(s.c)), uint64(C.vxnf_stat_requests(s.c)), uint64(C.vxnf_stat_batches(s.c))
}

// Close stops the loops, closes every connection, waits for in-flight writes
// to finish (their answers are dropped), and frees the server.
func (s *Server) Close() {
	s.once.Do(func() {
		C.vxnf_stop(s.c)
		s.writes.Wait() // no goroutine may call vxnf_respond after vxnf_free
		C.vxnf_hist(s.c, C.VXNF_HIST_WAKE, (*C.uint64_t)(unsafe.Pointer(&s.finalWake[0])))
		C.vxnf_hist(s.c, C.VXNF_HIST_ITER, (*C.uint64_t)(unsafe.Pointer(&s.finalIter[0])))
		s.closed.Store(true)
		C.vxnf_free(s.c)
		s.h.Delete()
	})
}

const (
	statusOK       = 0x00
	statusErr      = 0x01
	statusNotFound = 0x02
)

// appendFrame appends [status][4B len LE][payload].
func appendFrame(b []byte, status byte, payload []byte) []byte {
	b = append(b, status)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func (s *Server) respond(loop C.uint32_t, conn C.uint64_t, frame []byte) {
	C.vxnf_respond(s.c, loop, conn, unsafe.Pointer(unsafe.SliceData(frame)), C.size_t(len(frame)))
}

// cbytes views C memory owned by the request's connection buffer; valid only
// inside vxnfExec, so anything kept must be copied out.
func cbytes(p *C.uint8_t, n C.uint32_t) []byte {
	if n == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}

// noIOGetter is the optional Backend extension that lets reads which need
// disk leave the loop thread. *storage.StorageEngine implements it. Without
// it every GET and MGET is answered inline, as before.
type noIOGetter interface {
	GetNoIO(key string) (value []byte, needIO bool, err error)
	GetAfterNoIO(key string) ([]byte, error)
}

// chain is one connection's requests that must be answered from a goroutine,
// in arrival order. Once a connection has deferred a request in a batch,
// every later request of that connection in the same batch joins its chain:
// answering one of them inline would overtake the deferred answer.
type chain struct {
	loop C.uint32_t
	conn C.uint64_t
	jobs []func(respond func([]byte))
}

func getFrame(b []byte, v []byte, err error) []byte {
	if err != nil {
		return appendFrame(b, statusNotFound, nil)
	}
	return appendFrame(b, statusOK, v)
}

//export vxnfExec
func vxnfExec(h C.uintptr_t, reqs *C.vxnf_req, n C.int, tCall C.uint64_t) {
	s := cgo.Handle(h).Value().(*Server)
	tEnter := uint64(C.vxnf_now_ns())
	s.st.cbEnter.addNS(tEnter - uint64(tCall))
	defer func() { s.st.exec.addNS(uint64(C.vxnf_now_ns()) - tEnter) }()

	rs := unsafe.Slice(reqs, int(n))
	var frame []byte // reused for every inline answer: vxnf_respond copies it
	var chains map[C.uint64_t]*chain
	deferTo := func(r *C.vxnf_req) *chain {
		if ch := chains[r.conn]; ch != nil {
			return ch
		}
		if chains == nil {
			chains = make(map[C.uint64_t]*chain)
		}
		ch := &chain{loop: r.loop, conn: r.conn}
		chains[r.conn] = ch
		return ch
	}
	var probe noIOGetter
	if s.deferReads {
		probe, _ = s.be.(noIOGetter)
	}

	for i := 0; i < len(rs); i++ {
		r := &rs[i]
		ch := chains[r.conn] // non-nil: this connection is already deferred

		switch r.op {
		case C.VXNF_GET:
			key := string(cbytes(r.key, r.klen))
			if ch == nil {
				if probe == nil {
					v, err := s.be.Get(key)
					frame = getFrame(frame[:0], v, err)
					s.respond(r.loop, r.conn, frame)
					continue
				}
				v, needIO, err := probe.GetNoIO(key)
				if !needIO {
					frame = getFrame(frame[:0], v, err)
					s.respond(r.loop, r.conn, frame)
					continue
				}
				ch = deferTo(r)
				ch.jobs = append(ch.jobs, func(respond func([]byte)) {
					v, err := probe.GetAfterNoIO(key)
					respond(getFrame(nil, v, err))
				})
				continue
			}
			ch.jobs = append(ch.jobs, func(respond func([]byte)) {
				v, err := s.be.Get(key)
				respond(getFrame(nil, v, err))
			})

		case C.VXNF_PING:
			if ch != nil {
				ch.jobs = append(ch.jobs, func(respond func([]byte)) {
					respond(appendFrame(nil, statusOK, []byte("PONG")))
				})
				continue
			}
			frame = appendFrame(frame[:0], statusOK, []byte("PONG"))
			s.respond(r.loop, r.conn, frame)

		case C.VXNF_MGET:
			keys, ok := parseMGet(cbytes(r.key, r.klen), int(r.vlen))
			if !ok {
				if ch != nil {
					ch.jobs = append(ch.jobs, func(respond func([]byte)) {
						respond(appendFrame(nil, statusErr, []byte("malformed MGET frame")))
					})
					continue
				}
				frame = appendFrame(frame[:0], statusErr, []byte("malformed MGET frame"))
				s.respond(r.loop, r.conn, frame)
				continue
			}
			if ch == nil && probe != nil {
				// Answer every key that needs no disk now; if any does, the
				// goroutine reads only those, and the answer stays one frame.
				results, pending := mgetNoIO(probe, keys)
				if len(pending) == 0 {
					frame = appendMGetResponse(frame[:0], results)
					s.respond(r.loop, r.conn, frame)
					continue
				}
				ch = deferTo(r)
				ch.jobs = append(ch.jobs, func(respond func([]byte)) {
					for _, i := range pending {
						v, err := probe.GetAfterNoIO(keys[i])
						results[i] = storage.MultiGetResult{Key: keys[i], Value: v, Found: err == nil && v != nil, Err: err}
					}
					respond(appendMGetResponse(nil, results))
				})
				continue
			}
			if ch == nil {
				frame = appendMGetResponse(frame[:0], s.be.MultiGet(keys))
				s.respond(r.loop, r.conn, frame)
				continue
			}
			ch.jobs = append(ch.jobs, func(respond func([]byte)) {
				respond(appendMGetResponse(nil, s.be.MultiGet(keys)))
			})

		case C.VXNF_PUT:
			// Back-to-back PUTs of one connection arrive adjacent (the parser
			// groups them); coalesce them into one MultiPut exactly as the Go
			// server's tryCoalescePuts does.
			j := i + 1
			for j < len(rs) && rs[j].op == C.VXNF_PUT && rs[j].conn == r.conn {
				j++
			}
			group := make([]storage.MultiPutRequest, 0, j-i)
			for k := i; k < j; k++ {
				group = append(group, storage.MultiPutRequest{
					Key:   string(cbytes(rs[k].key, rs[k].klen)),
					Value: append([]byte(nil), cbytes(rs[k].val, rs[k].vlen)...),
					TTL:   -1,
				})
			}
			i = j - 1
			job := func(respond func([]byte)) {
				t0 := uint64(C.vxnf_now_ns())
				var errs []error
				if len(group) == 1 {
					// Single PUT keeps the single-key path (write-through cache).
					errs = []error{s.be.Put(group[0].Key, group[0].Value, -1)}
				} else {
					errs = s.be.MultiPut(group)
				}
				s.st.putExec.addNS(uint64(C.vxnf_now_ns()) - t0)
				var f []byte
				for _, err := range errs {
					if err != nil {
						f = appendFrame(f[:0], statusErr, []byte(err.Error()))
					} else {
						f = appendFrame(f[:0], statusOK, nil)
					}
					respond(f)
				}
			}
			s.runOrChain(ch, r.loop, r.conn, job, &s.st.putSched)

		case C.VXNF_DEL:
			key := string(cbytes(r.key, r.klen))
			s.runOrChain(ch, r.loop, r.conn, func(respond func([]byte)) {
				var f []byte
				if err := s.be.Delete(key); err != nil {
					f = appendFrame(f, statusErr, []byte(err.Error()))
				} else {
					f = appendFrame(f, statusOK, nil)
				}
				respond(f)
			}, nil)

		case C.VXNF_MPUT:
			entries, ok := parseMPut(cbytes(r.key, r.klen), int(r.vlen))
			if !ok {
				if ch != nil {
					ch.jobs = append(ch.jobs, func(respond func([]byte)) {
						respond(appendFrame(nil, statusErr, []byte("malformed MPUT frame")))
					})
					continue
				}
				frame = appendFrame(frame[:0], statusErr, []byte("malformed MPUT frame"))
				s.respond(r.loop, r.conn, frame)
				continue
			}
			s.runOrChain(ch, r.loop, r.conn, func(respond func([]byte)) {
				errs := s.be.MultiPut(entries)
				f := make([]byte, 5, 5+len(errs))
				f[0] = statusOK
				binary.LittleEndian.PutUint32(f[1:], uint32(len(errs)))
				for _, err := range errs {
					if err != nil {
						f = append(f, statusErr)
					} else {
						f = append(f, statusOK)
					}
				}
				respond(f)
			}, nil)

		default:
			// The C++ parser only emits the ops above.
			if ch != nil {
				ch.jobs = append(ch.jobs, func(respond func([]byte)) {
					respond(appendFrame(nil, statusErr, []byte("unsupported op")))
				})
				continue
			}
			frame = appendFrame(frame[:0], statusErr, []byte("unsupported op"))
			s.respond(r.loop, r.conn, frame)
		}
	}

	// Block each deferred connection BEFORE its goroutine can answer, then
	// run its chain in order.
	for _, ch := range chains {
		C.vxnf_defer(s.c, ch.loop, ch.conn)
		ch := ch
		tq := uint64(C.vxnf_now_ns())
		s.writes.Add(1)
		go func() {
			defer s.writes.Done()
			s.st.deferSched.addNS(uint64(C.vxnf_now_ns()) - tq)
			t0 := uint64(C.vxnf_now_ns())
			respond := func(f []byte) { s.respond(ch.loop, ch.conn, f) }
			for _, job := range ch.jobs {
				job(respond)
			}
			s.st.deferExec.addNS(uint64(C.vxnf_now_ns()) - t0)
		}()
	}
}

// runOrChain runs a write job in its own goroutine, or appends it to the
// connection's chain when an earlier request of the connection was deferred
// in this batch. sched, when set, records the goroutine start delay.
func (s *Server) runOrChain(ch *chain, loop C.uint32_t, conn C.uint64_t, job func(func([]byte)), sched *hist) {
	if ch != nil {
		ch.jobs = append(ch.jobs, job)
		return
	}
	tq := uint64(C.vxnf_now_ns())
	s.writes.Add(1)
	go func() {
		defer s.writes.Done()
		if sched != nil {
			sched.addNS(uint64(C.vxnf_now_ns()) - tq)
		}
		job(func(f []byte) { s.respond(loop, conn, f) })
	}()
}

// mgetNoIO resolves every key of an MGET that needs no disk and returns the
// indices of those that do (their results are left for GetAfterNoIO).
// Mirrors MultiGet's per-key Get, so each key is counted as one read.
func mgetNoIO(p noIOGetter, keys []string) (results []storage.MultiGetResult, pending []int) {
	results = make([]storage.MultiGetResult, len(keys))
	for i, k := range keys {
		v, needIO, err := p.GetNoIO(k)
		if needIO {
			pending = append(pending, i)
			continue
		}
		results[i] = storage.MultiGetResult{Key: k, Value: v, Found: err == nil && v != nil, Err: err}
	}
	return results, pending
}

// parseMGet decodes count × [2B keyLen LE][key]. The C++ parser has already
// bounds-checked the frame; this re-checks rather than trust it.
func parseMGet(p []byte, count int) ([]string, bool) {
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if len(p) < 2 {
			return nil, false
		}
		kl := int(binary.LittleEndian.Uint16(p))
		if len(p) < 2+kl {
			return nil, false
		}
		keys = append(keys, string(p[2:2+kl]))
		p = p[2+kl:]
	}
	return keys, len(p) == 0
}

// parseMPut decodes count × [2B keyLen][4B valLen][4B ttl][key][value],
// copying everything out of the C buffer.
func parseMPut(p []byte, count int) ([]storage.MultiPutRequest, bool) {
	reqs := make([]storage.MultiPutRequest, 0, count)
	for i := 0; i < count; i++ {
		if len(p) < 10 {
			return nil, false
		}
		kl := int(binary.LittleEndian.Uint16(p))
		vl := int(binary.LittleEndian.Uint32(p[2:]))
		ttl := int32(binary.LittleEndian.Uint32(p[6:]))
		if len(p) < 10+kl+vl {
			return nil, false
		}
		reqs = append(reqs, storage.MultiPutRequest{
			Key:   string(p[10 : 10+kl]),
			Value: append([]byte(nil), p[10+kl:10+kl+vl]...),
			TTL:   ttl,
		})
		p = p[10+kl+vl:]
	}
	return reqs, len(p) == 0
}

// appendMGetResponse: [OK][4B count] + count × [status][4B len][value],
// byte-for-byte what cmd/server handleMGet writes.
func appendMGetResponse(b []byte, results []storage.MultiGetResult) []byte {
	b = append(b, statusOK)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(results)))
	for _, r := range results {
		if !r.Found || r.Value == nil {
			b = append(b, statusNotFound, 0, 0, 0, 0)
			continue
		}
		b = append(b, statusOK)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Value)))
		b = append(b, r.Value...)
	}
	return b
}
