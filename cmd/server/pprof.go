package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime/pprof"
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
//	/debug/pprof/<name>              heap, goroutine, allocs, block, mutex, threadcreate
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
