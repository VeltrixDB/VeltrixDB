/*
 * netfront.h — pure-C API of the C++ network front-end (netfront.cpp).
 *
 * Included by the cgo preamble in netfront.go, so it must stay C99.
 *
 * Model
 * ─────
 * N event-loop threads, one per core. Each owns its own listening socket
 * (SO_REUSEPORT: the kernel spreads connections across them, and a
 * connection lives on one loop for its whole life), its own I/O backend
 * (io_uring on Linux, poll() elsewhere or on request) and its connections.
 *
 * One loop iteration: reap every completed I/O, parse every complete request
 * frame out of every connection's input, hand the WHOLE set to Go in one
 * call (vxnfExec), then submit every resulting send and re-armed receive in
 * one go. Syscalls and Go transitions are paid per iteration, not per
 * request — that amortisation is the point of the front-end.
 *
 * Responses: Go answers every request exactly once with vxnf_respond, either
 * inside vxnfExec (reads that need no disk) or later from any goroutine
 * (writes, which wait on fdatasync, and reads that need a VLog read — neither
 * may block a loop). A connection with a write or deferred read in flight is
 * not parsed further until it is answered, so each connection is served
 * strictly in order — the same semantics as the Go server.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Binary protocol commands the front-end serves (same bytes as cmd/server). */
enum {
    VXNF_PUT  = 0x01,
    VXNF_GET  = 0x02,
    VXNF_DEL  = 0x03,
    VXNF_PING = 0x04,
    VXNF_MPUT = 0x06,
    VXNF_MGET = 0x07,
};

enum {
    VXNF_BACKEND_AUTO  = 0, /* io_uring when available, else poll */
    VXNF_BACKEND_URING = 1,
    VXNF_BACKEND_POLL  = 2,
};

typedef struct vxnf_server vxnf_server;

/* One parsed request. Pointers reference the connection's input buffer and
 * are valid only until vxnfExec returns: Go copies whatever it keeps. */
typedef struct {
    uint64_t       conn;  /* opaque connection token */
    const uint8_t* key;   /* PUT/GET/DEL: key. MPUT/MGET: the entry payload */
    const uint8_t* val;   /* PUT: value */
    uint32_t       klen;  /* PUT/GET/DEL: key length. MPUT/MGET: payload length */
    uint32_t       vlen;  /* PUT: value length. MPUT/MGET: entry count */
    uint32_t       loop;  /* owning loop index */
    uint8_t        op;    /* VXNF_* */
    uint8_t        _pad[3];
} vxnf_req;

typedef struct {
    const char* host;       /* bind address, e.g. "0.0.0.0" */
    int         port;       /* 0 = pick one (tests); see vxnf_port */
    int         threads;    /* event loops; <= 0 means 1 */
    int         backend;    /* VXNF_BACKEND_* */
    int         ring_depth; /* io_uring SQ depth per loop; <= 0 means 4096 */
    uintptr_t   handle;     /* passed back to vxnfExec (a cgo.Handle) */
} vxnf_config;

/* Bind, listen and start the loops. NULL on failure with a message in err. */
vxnf_server* vxnf_start(vxnf_config cfg, char* err, size_t errlen);

/* The bound port (useful with port 0). */
int vxnf_port(const vxnf_server* s);

/* Name of the backend actually running: "io_uring" or "poll". */
const char* vxnf_backend(const vxnf_server* s);

/* Stop accepting, close every connection and join the loop threads. The
 * server stays allocated: vxnf_respond remains safe to call (and is a no-op)
 * until vxnf_free, so late write completions cannot touch freed memory. */
void vxnf_stop(vxnf_server* s);

/* Release the server. Only after vxnf_stop AND after the last vxnf_respond. */
void vxnf_free(vxnf_server* s);

/* Answer request (loop, conn) with a complete binary response frame. Safe
 * from any thread; a stale token (the connection has closed) is ignored. */
void vxnf_respond(vxnf_server* s, uint32_t loop, uint64_t conn, const void* data, size_t len);

/* Called from inside vxnfExec, on the loop's own thread only: the answer to
 * this connection's requests will come later from a goroutine (a read that
 * needs disk). Blocks the connection like a write — nothing more is parsed
 * from it until every outstanding request is answered — so per-connection
 * order holds. */
void vxnf_defer(vxnf_server* s, uint32_t loop, uint64_t conn);

/* CLOCK_MONOTONIC in ns — the clock vxnfExec's t_call_ns is taken on. */
uint64_t vxnf_now_ns(void);

/* Latency histograms, VXNF_HIST_BUCKETS buckets: bucket i counts durations of
 * [2^(i-1), 2^i) µs, bucket 0 is sub-microsecond. */
enum {
    VXNF_HIST_BUCKETS = 32,
    VXNF_HIST_WAKE = 0, /* vxnf_respond from a goroutine → answer appended on the loop */
    VXNF_HIST_ITER = 1, /* one loop iteration: event handling + vxnfExec + post */
};
void vxnf_hist(const vxnf_server* s, int which, uint64_t* out);

/* Counters for /metrics: loop iterations, requests, Go batches. */
uint64_t vxnf_stat_iterations(const vxnf_server* s);
uint64_t vxnf_stat_requests(const vxnf_server* s);
uint64_t vxnf_stat_batches(const vxnf_server* s);

#ifdef __cplusplus
}
#endif
