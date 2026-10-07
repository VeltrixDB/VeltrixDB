package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"time"
)

// servePprof exposes CPU and runtime profiles on a dedicated listener
// (--pprof-addr, off by default).
//
// Built on runtime/pprof rather than net/http/pprof on purpose: importing
// net/http/pprof — even without using its handlers — runs an init that
// registers them on http.DefaultServeMux, and any listener that ever serves
// DefaultServeMux (a nil Handler) would then leak profiles. Nothing in this
// process does today; this keeps it that way by construction.
//
//	/debug/pprof/profile?seconds=N   CPU profile (default 30 s)
//	/debug/pprof/trace?seconds=N     execution trace (default 5 s) — `go tool trace`
//	/debug/pprof/<name>              heap, goroutine, allocs, block, mutex, threadcreate
//
// block and mutex stay empty unless --pprof-block-rate / --pprof-mutex-fraction
// turn them on (enableContentionProfiles): the runtime records neither by default.
//
// Both are `go tool pprof`-compatible.
func servePprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		secs, _ := strconv.Atoi(r.URL.Query().Get("seconds"))
		if secs <= 0 {
			secs = 30
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if err := pprof.StartCPUProfile(w); err != nil {
			http.Error(w, err.Error(), http.StatusConflict) // one CPU profile at a time
			return
		}
		select {
		case <-time.After(time.Duration(secs) * time.Second):
		case <-r.Context().Done():
		}
		pprof.StopCPUProfile()
	})
	mux.HandleFunc("/debug/pprof/trace", func(w http.ResponseWriter, r *http.Request) {
		secs, _ := strconv.Atoi(r.URL.Query().Get("seconds"))
		if secs <= 0 {
			secs = 5
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if err := trace.Start(w); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		select {
		case <-time.After(time.Duration(secs) * time.Second):
		case <-r.Context().Done():
		}
		trace.Stop()
	})
	mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[len("/debug/pprof/"):]
		p := pprof.Lookup(name)
		if p == nil {
			http.Error(w, fmt.Sprintf("unknown profile %q", name), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_ = p.WriteTo(w, 0)
	})
	log.Printf("[pprof] serving on http://%s/debug/pprof/", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("[pprof] listener stopped: %v", err)
	}
}

// enableContentionProfiles turns on the runtime's mutex and block profiling,
// which are off by default (so /debug/pprof/mutex and /block are empty).
// Values <= 0 leave a profile off.
func enableContentionProfiles(mutexFraction, blockRateNs int) {
	if mutexFraction > 0 {
		runtime.SetMutexProfileFraction(mutexFraction)
		log.Printf("[pprof] mutex profile on: sampling 1 in %d contention events", mutexFraction)
	}
	if blockRateNs > 0 {
		runtime.SetBlockProfileRate(blockRateNs)
		log.Printf("[pprof] block profile on: events >= %d ns", blockRateNs)
	}
}
