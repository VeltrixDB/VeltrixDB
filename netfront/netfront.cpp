/*
 * netfront.cpp — C++ network front-end: per-core event loops, io_uring on
 * Linux (poll() as the portable fallback), one Go call per loop iteration.
 * API and model: netfront.h.
 *
 * Lives in the Go package directory (not cpp/src behind an #include shim) so
 * the go build cache tracks it — see storage/native_index.cpp for the
 * measurement that made that a rule.
 *
 * Memory-safety rules this file is built around
 * ─────────────────────────────────────────────
 * 1. A vxnf_req points into its connection's input buffer. Between parsing a
 *    request and the end of the vxnfExec call that consumes it, that buffer
 *    is never grown, compacted or freed. All three happen only in
 *    Loop::post(), after exec.
 * 2. A buffer handed to the kernel (armed recv / send) is never moved until
 *    its completion is reaped: input is only grown or compacted while no
 *    recv is armed; output is double-buffered (out_inflight is what the
 *    kernel sees, responses append to out_pending).
 * 3. A connection slot is released only in post(), only once no recv or send
 *    is armed, and its generation is bumped: a write answered after the
 *    client went away carries the old generation and is dropped.
 */

#include "netfront.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__linux__) && defined(VXNF_HAVE_URING)
#include <liburing.h>
#endif

// Exported from netfront.go. Declared by hand: _cgo_export.h is C and its
// complex-number typedefs do not compile as C++.
extern "C" void vxnfExec(uintptr_t handle, vxnf_req* reqs, int n);

namespace {

constexpr uint8_t  kStatusErr  = 0x01;
constexpr uint32_t kMaxKey     = 1u << 20;   // cmd/server: keyLen > 1<<20 is rejected
constexpr uint32_t kMaxVal     = 64u << 20;  // cmd/server: valLen > 64<<20 is rejected
constexpr uint32_t kMaxBatch   = 100000;     // cmd/server maxBatchCount
constexpr size_t   kPutGroup   = 256;        // cmd/server maxPipelineBatch
constexpr size_t   kReadChunk  = 64 * 1024;
constexpr size_t   kInHighMark = 16u << 20;  // stop reading ahead past this much unparsed input
constexpr size_t   kOutHighMark = 64u << 20; // stop reading from a client that is not reading its answers
constexpr uint8_t  kMaxCmd     = 0x29;       // highest binary command byte (cmd/server binCmdGetVer)

inline uint32_t le32(const uint8_t* p) {
    return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}
inline uint16_t le16(const uint8_t* p) { return uint16_t(p[0] | p[1] << 8); }

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL; // a C thread must never take SIGPIPE
#else
constexpr int kSendFlags = 0;            // macOS: SO_NOSIGPIPE is set per socket instead
#endif

inline uint64_t token(uint32_t idx, uint32_t gen) { return uint64_t(gen) << 32 | idx; }
inline uint32_t token_idx(uint64_t t) { return uint32_t(t); }
inline uint32_t token_gen(uint64_t t) { return uint32_t(t >> 32); }

void set_err(char* err, size_t n, const std::string& msg) {
    if (err && n) std::snprintf(err, n, "%s", msg.c_str());
}

struct Conn {
    int      fd  = -1;
    uint32_t gen = 1;
    bool     live = false;

    std::vector<uint8_t> in;   // [0, in_len) received; [in_pos, in_len) not yet parsed
    size_t   in_len = 0;
    size_t   in_pos = 0;
    size_t   in_need = 0;      // bytes the next frame needs in total from in_pos

    std::vector<uint8_t> out_pending;   // responses not yet handed to the kernel
    std::vector<uint8_t> out_inflight;  // what the armed send is writing
    size_t   inflight_off = 0;

    bool recv_armed = false;
    bool send_armed = false;
    bool blocked    = false;  // a write is in flight: parse nothing more until answered
    bool closing    = false;
    uint32_t outstanding = 0; // requests handed to Go, not yet answered
    std::string fail_msg;     // protocol error to send once earlier answers are queued
};

struct Event {
    enum Type : uint8_t { Accept, Recv, Send, Wake } type;
    uint32_t conn;
    int      res; // Accept: new fd or -errno. Recv/Send: bytes or -errno (0 = EOF).
};

struct Loop;

// I/O backend. Every operation is one-shot: arming queues it, and it
// completes as exactly one Event returned by wait().
struct Backend {
    virtual ~Backend() = default;
    virtual const char* name() const = 0;
    virtual bool init(Loop& l, int depth, std::string& err) = 0;
    virtual void arm_accept() = 0;
    virtual void arm_wake() = 0;
    virtual void arm_recv(uint32_t c, uint8_t* p, size_t n) = 0;
    virtual void arm_send(uint32_t c, const uint8_t* p, size_t n) = 0;
    virtual void forget(uint32_t c) = 0; // slot released
    // Withdraw the armed accept and wake reads (shutdown). Their completions
    // still arrive through wait(), after which nothing is lent to the kernel.
    virtual void cancel_listen() = 0;
    // True when armed operations hold buffers inside the kernel (io_uring),
    // so shutdown must reap every completion before freeing anything.
    virtual bool lends_buffers() const = 0;
    // Submit everything armed, block for at least one completion, return all.
    virtual void wait(std::vector<Event>& out) = 0;
};

struct Completion {
    uint64_t conn;
    std::vector<uint8_t> data;
};

} // namespace

struct vxnf_server;

namespace {

struct Loop {
    vxnf_server* srv = nullptr;
    uint32_t     idx = 0;
    int          lfd = -1;
    int          wake_r = -1, wake_w = -1;
    std::unique_ptr<Backend> be;
    std::thread  th;

    std::vector<Conn>     conns;
    std::vector<uint32_t> free_slots;
    std::vector<uint32_t> touched;     // conns to revisit in post()
    std::vector<vxnf_req> batch;
    std::vector<Event>    events;

    std::mutex              cmu;       // guards completions
    std::vector<Completion> completions;
    std::vector<Completion> drained;
    std::atomic<bool>       wake_pending{false};
    std::atomic<bool>       stop_requested{false}; // set by vxnf_stop, from any thread
    bool accept_armed = false;
    bool wake_armed   = false;
    bool stopping     = false;                     // loop thread only

    void run();
    void handle(const Event& e);
    void on_accept(int fd);
    void on_recv(uint32_t c, int res);
    void on_send(uint32_t c, int res);
    void on_wake();
    void parse(uint32_t c);
    void exec();
    void post();
    void touch(uint32_t c) { touched.push_back(c); }
    void begin_close(uint32_t c);
    void fail(uint32_t c, const char* msg);
    void append_response(Conn& c, const void* data, size_t len);
    void maybe_send(uint32_t c);
    void release(uint32_t c);
    void start_stopping();
    bool can_exit() const;
    void wake();
};

thread_local Loop* tl_loop = nullptr; // set while this thread runs a loop

// ── poll() backend ───────────────────────────────────────────────────────────

struct PollBackend final : Backend {
    Loop* l = nullptr;
    struct Want {
        uint8_t*       rp = nullptr; size_t rn = 0; bool r = false;
        const uint8_t* sp = nullptr; size_t sn = 0; bool s = false;
    };
    std::vector<Want>  want;
    bool accept = false, wake = false;
    std::vector<pollfd>   pfds;
    std::vector<uint32_t> pconn;

    const char* name() const override { return "poll"; }
    bool init(Loop& lp, int, std::string&) override { l = &lp; return true; }
    Want& w(uint32_t c) { if (want.size() <= c) want.resize(c + 1); return want[c]; }
    void arm_accept() override { accept = true; }
    void arm_wake() override { wake = true; }
    void arm_recv(uint32_t c, uint8_t* p, size_t n) override { auto& x = w(c); x.rp = p; x.rn = n; x.r = true; }
    void arm_send(uint32_t c, const uint8_t* p, size_t n) override { auto& x = w(c); x.sp = p; x.sn = n; x.s = true; }
    void forget(uint32_t c) override { if (c < want.size()) want[c] = Want{}; }
    void cancel_listen() override { accept = false; wake = false; }
    bool lends_buffers() const override { return false; }

    void wait(std::vector<Event>& out) override {
        for (;;) {
            pfds.clear();
            pconn.clear();
            if (accept) { pfds.push_back({l->lfd, POLLIN, 0}); pconn.push_back(UINT32_MAX); }
            if (wake)   { pfds.push_back({l->wake_r, POLLIN, 0}); pconn.push_back(UINT32_MAX - 1); }
            for (uint32_t c = 0; c < want.size(); ++c) {
                short ev = (want[c].r ? POLLIN : 0) | (want[c].s ? POLLOUT : 0);
                if (ev) { pfds.push_back({l->conns[c].fd, ev, 0}); pconn.push_back(c); }
            }
            if (::poll(pfds.data(), pfds.size(), -1) < 0) {
                if (errno == EINTR) continue;
                return;
            }
            for (size_t i = 0; i < pfds.size(); ++i) {
                const short re = pfds[i].revents;
                if (!re) continue;
                const uint32_t c = pconn[i];
                if (c == UINT32_MAX) {
                    for (;;) {
                        int fd = ::accept(l->lfd, nullptr, nullptr);
                        if (fd < 0) {
                            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                                out.push_back({Event::Accept, 0, -errno});
                            break;
                        }
                        out.push_back({Event::Accept, 0, fd});
                    }
                    accept = false;
                    continue;
                }
                if (c == UINT32_MAX - 1) {
                    char buf[256];
                    while (::read(l->wake_r, buf, sizeof buf) > 0) {}
                    wake = false;
                    out.push_back({Event::Wake, 0, 0});
                    continue;
                }
                Want& x = want[c];
                const int fd = l->conns[c].fd;
                if (x.r && (re & (POLLIN | POLLHUP | POLLERR))) {
                    ssize_t n = ::recv(fd, x.rp, x.rn, 0);
                    if (n >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                        x.r = false;
                        out.push_back({Event::Recv, c, n >= 0 ? int(n) : -errno});
                    }
                }
                if (x.s && (re & (POLLOUT | POLLHUP | POLLERR))) {
                    ssize_t n = ::send(fd, x.sp, x.sn, kSendFlags);
                    if (n >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)) {
                        x.s = false;
                        out.push_back({Event::Send, c, n >= 0 ? int(n) : -errno});
                    }
                }
            }
            if (!out.empty()) return;
        }
    }
};

// ── io_uring backend ─────────────────────────────────────────────────────────

#if defined(__linux__) && defined(VXNF_HAVE_URING)

struct UringBackend final : Backend {
    Loop*    l = nullptr;
    io_uring ring{};
    bool     ok = false;
    uint64_t wakebuf = 0;
    char     wakepipe[256];

    enum : uint8_t { TAccept = 1, TRecv, TSend, TWake, TCancel };
    static uint64_t ud(uint8_t type, uint32_t c) { return uint64_t(type) << 56 | c; }

    const char* name() const override { return "io_uring"; }

    bool init(Loop& lp, int depth, std::string& err) override {
        l = &lp;
        io_uring_params p{};
        // No SQPOLL: a busy-polling kernel thread per loop is exactly what
        // starved the CI race job before (see VELTRIXDB_DISABLE_CGO_ENGINE),
        // and the loop already batches every submission into one enter.
        int r = io_uring_queue_init_params(unsigned(depth), &ring, &p);
        if (r < 0) {
            err = std::string("io_uring_queue_init: ") + std::strerror(-r);
            return false;
        }
        ok = true;
        return true;
    }
    ~UringBackend() override { if (ok) io_uring_queue_exit(&ring); }

    io_uring_sqe* sqe() {
        io_uring_sqe* s = io_uring_get_sqe(&ring);
        while (!s) { // SQ full: push what is queued and retry
            io_uring_submit(&ring);
            s = io_uring_get_sqe(&ring);
        }
        return s;
    }
    void arm_accept() override {
        auto* s = sqe();
        io_uring_prep_accept(s, l->lfd, nullptr, nullptr, 0);
        io_uring_sqe_set_data64(s, ud(TAccept, 0));
    }
    void arm_wake() override {
        auto* s = sqe();
        io_uring_prep_read(s, l->wake_r, wakepipe, sizeof wakepipe, 0);
        io_uring_sqe_set_data64(s, ud(TWake, 0));
    }
    void arm_recv(uint32_t c, uint8_t* p, size_t n) override {
        auto* s = sqe();
        io_uring_prep_recv(s, l->conns[c].fd, p, n, 0);
        io_uring_sqe_set_data64(s, ud(TRecv, c));
    }
    void arm_send(uint32_t c, const uint8_t* p, size_t n) override {
        auto* s = sqe();
        io_uring_prep_send(s, l->conns[c].fd, p, n, kSendFlags);
        io_uring_sqe_set_data64(s, ud(TSend, c));
    }
    void forget(uint32_t) override {}
    void cancel_listen() override {
        for (uint8_t t : {TAccept, TWake}) {
            auto* s = sqe();
            io_uring_prep_cancel64(s, ud(t, 0), 0);
            io_uring_sqe_set_data64(s, ud(TCancel, 0));
        }
    }
    bool lends_buffers() const override { return true; }

    void wait(std::vector<Event>& out) override {
        for (;;) {
            int r = io_uring_submit_and_wait(&ring, 1);
            if (r < 0 && r != -EINTR && r != -EAGAIN && r != -EBUSY) return;
            unsigned head, n = 0;
            io_uring_cqe* cqe;
            io_uring_for_each_cqe(&ring, head, cqe) {
                ++n;
                const uint64_t d = io_uring_cqe_get_data64(cqe);
                const uint8_t  t = uint8_t(d >> 56);
                const uint32_t c = uint32_t(d);
                switch (t) {
                case TAccept: out.push_back({Event::Accept, 0, cqe->res}); break;
                case TRecv:   out.push_back({Event::Recv, c, cqe->res}); break;
                case TSend:   out.push_back({Event::Send, c, cqe->res}); break;
                case TWake:   out.push_back({Event::Wake, 0, cqe->res}); break;
                default:      break; // TCancel: the cancelled op reports on its own
                }
            }
            io_uring_cq_advance(&ring, n);
            if (n) return; // a lone TCancel still returns, so run() re-checks its exit condition
        }
    }
};

#endif

} // namespace

struct vxnf_server {
    vxnf_config cfg{};
    std::string host;
    int         port = 0;
    std::vector<std::unique_ptr<Loop>> loops;
    std::atomic<bool>     stopped{false};
    std::atomic<uint64_t> iterations{0}, requests{0}, batches{0};
    const char* backend = "poll";
};

namespace {

void Loop::wake() {
    if (!wake_pending.exchange(true)) {
        char b = 1;
        (void)!::write(wake_w, &b, 1);
    }
}

void Loop::run() {
    tl_loop = this;
    be->arm_accept();
    accept_armed = true;
    be->arm_wake();
    wake_armed = true;
    while (!can_exit()) {
        events.clear();
        be->wait(events);
        srv->iterations.fetch_add(1, std::memory_order_relaxed);
        for (const Event& e : events) handle(e);
        if (!stopping && stop_requested.load()) start_stopping();
        // post() can parse more (a connection unblocked by an inline answer),
        // and those requests need another exec before their buffer may move.
        do {
            exec();
            post();
        } while (!batch.empty());
    }
    // Connections still flushing when the loop exits: nothing is armed any
    // more (can_exit), so their fds can simply be closed.
    for (Conn& k : conns) {
        if (k.live && k.fd >= 0) ::close(k.fd);
        k.fd = -1;
        k.live = false;
    }
    tl_loop = nullptr;
}

// Runs on the loop thread: stop listening and shut every connection down.
// Everything else — reaping, releasing — then happens through the normal
// event path, so no other thread ever touches `conns`.
void Loop::start_stopping() {
    stopping = true;
    be->cancel_listen();
    if (!be->lends_buffers()) accept_armed = wake_armed = false;
    for (uint32_t c = 0; c < conns.size(); ++c)
        if (conns[c].live) begin_close(c);
}

// Exit once stopping and the kernel holds none of our buffers.
bool Loop::can_exit() const {
    if (!stopping) return false;
    if (!be->lends_buffers()) return true;
    if (accept_armed || wake_armed) return false;
    for (const Conn& c : conns)
        if (c.live && (c.recv_armed || c.send_armed)) return false;
    return true;
}

void Loop::handle(const Event& e) {
    switch (e.type) {
    case Event::Accept:
        accept_armed = false;
        if (e.res >= 0) {
            if (stopping) ::close(e.res); else on_accept(e.res);
        }
        if (!stopping && !stop_requested.load()) { be->arm_accept(); accept_armed = true; }
        break;
    case Event::Recv: on_recv(e.conn, e.res); break;
    case Event::Send: on_send(e.conn, e.res); break;
    case Event::Wake:
        wake_armed = false;
        on_wake();
        if (!stopping && !stop_requested.load()) {
            be->arm_wake();
            wake_armed = true;
        }
        break;
    }
}

void Loop::on_accept(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
#ifdef SO_NOSIGPIPE
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
    uint32_t c;
    if (!free_slots.empty()) {
        c = free_slots.back();
        free_slots.pop_back();
    } else {
        c = uint32_t(conns.size());
        conns.emplace_back();
    }
    Conn& k = conns[c];
    k.fd = fd;
    k.live = true;
    touch(c); // post() arms the first recv
}

void Loop::on_recv(uint32_t c, int res) {
    Conn& k = conns[c];
    k.recv_armed = false;
    if (res <= 0) {
        begin_close(c); // 0 = peer closed; < 0 = error
        return;
    }
    k.in_len += size_t(res);
    parse(c);
    touch(c);
}

void Loop::on_send(uint32_t c, int res) {
    Conn& k = conns[c];
    k.send_armed = false;
    if (res <= 0) {
        begin_close(c);
        return;
    }
    k.inflight_off += size_t(res);
    if (k.inflight_off >= k.out_inflight.size()) {
        k.out_inflight.clear();
        k.inflight_off = 0;
    }
    touch(c); // post() sends the remainder or the next pending run
}

void Loop::on_wake() {
    wake_pending.store(false);
    {
        std::lock_guard<std::mutex> g(cmu);
        drained.swap(completions);
    }
    for (Completion& d : drained) {
        const uint32_t c = token_idx(d.conn);
        if (c >= conns.size()) continue;
        Conn& k = conns[c];
        if (!k.live || k.gen != token_gen(d.conn)) continue; // client went away
        append_response(k, d.data.data(), d.data.size());
        if (k.outstanding) --k.outstanding;
        if (k.outstanding == 0 && k.blocked) {
            k.blocked = false;
            parse(c); // resume the pipeline behind the write
        }
        touch(c);
    }
    drained.clear();
}

void Loop::append_response(Conn& k, const void* data, size_t len) {
    const auto* p = static_cast<const uint8_t*>(data);
    k.out_pending.insert(k.out_pending.end(), p, p + len);
}

// A frame the front-end cannot serve or cannot frame: answer it with an
// error and close, since the stream cannot be re-synchronised. The error is
// queued in post(), AFTER the answers to requests parsed before it, so the
// client sees responses in request order.
void Loop::fail(uint32_t c, const char* msg) {
    Conn& k = conns[c];
    k.fail_msg = msg;
    k.closing = true;
    touch(c);
}

// Hard close: the peer went away or I/O failed. Nothing more is sent.
void Loop::begin_close(uint32_t c) {
    Conn& k = conns[c];
    if (!k.live) return;
    if (k.fd >= 0) ::shutdown(k.fd, SHUT_RDWR);
    k.closing = true;
    k.fail_msg.clear();
    k.out_pending.clear();
    touch(c);
}

void Loop::release(uint32_t c) {
    Conn& k = conns[c];
    ::close(k.fd);
    be->forget(c);
    k.fd = -1;
    k.live = false;
    ++k.gen; // late write answers now miss
    k.in.clear(); k.in.shrink_to_fit();
    k.out_pending.clear(); k.out_pending.shrink_to_fit();
    k.out_inflight.clear(); k.out_inflight.shrink_to_fit();
    k.fail_msg.clear();
    k.in_len = k.in_pos = k.in_need = k.inflight_off = 0;
    k.blocked = false;
    k.closing = false;
    k.outstanding = 0;
    free_slots.push_back(c);
}

// Parse every complete frame in [in_pos, in_len) into batch. Stops at an
// incomplete frame (recording how much it needs), or after dispatching a
// write, which blocks the connection until Go answers it.
void Loop::parse(uint32_t c) {
    Conn& k = conns[c];
    size_t queued_puts = 0;
    while (!k.blocked && !k.closing) {
        const size_t avail = k.in_len - k.in_pos;
        const uint8_t* p = k.in.data() + k.in_pos;
        k.in_need = 0;
        if (avail < 1) break;
        if (p[0] < 0x01 || p[0] > kMaxCmd) {
            fail(c, "text protocol is not served by --net=uring; use the binary protocol");
            return;
        }
        if (avail < 7) { k.in_need = 7; break; }
        const uint8_t  cmd  = p[0];
        const uint32_t klen = le16(p + 1);
        const uint32_t vlen = le32(p + 3);

        // A group of back-to-back PUTs ends at the first frame that is not a
        // complete PUT — same coalescing rule as cmd/server tryCoalescePuts.
        if (queued_puts > 0 && (cmd != VXNF_PUT || queued_puts >= kPutGroup)) {
            k.blocked = true;
            break;
        }

        vxnf_req r{};
        r.conn = token(c, k.gen);
        r.loop = idx;
        r.op   = cmd;
        size_t need = 0;

        switch (cmd) {
        case VXNF_PUT: case VXNF_GET: case VXNF_DEL: case VXNF_PING: {
            if (klen > kMaxKey || vlen > kMaxVal) { fail(c, "frame too large"); return; }
            need = 7 + size_t(klen) + vlen;
            if (avail < need) { k.in_need = need; goto incomplete; }
            r.key = p + 7; r.klen = klen;
            r.val = p + 7 + klen; r.vlen = vlen;
            break;
        }
        case VXNF_MPUT: case VXNF_MGET: {
            const uint32_t count = vlen;
            if (count == 0 || count > kMaxBatch) { fail(c, "bad batch count"); return; }
            size_t off = 7;
            for (uint32_t i = 0; i < count; ++i) {
                const size_t eh = cmd == VXNF_MPUT ? 10 : 2;
                if (avail < off + eh) { k.in_need = off + eh; goto incomplete; }
                const uint32_t ek = le16(p + off);
                const uint32_t ev = cmd == VXNF_MPUT ? le32(p + off + 2) : 0;
                if (ek > kMaxKey || ev > kMaxVal) { fail(c, "frame too large"); return; }
                off += eh + ek + ev;
                if (off - 7 > UINT32_MAX) { fail(c, "frame too large"); return; }
            }
            if (avail < off) { k.in_need = off; goto incomplete; }
            need = off;
            r.key = p + 7; r.klen = uint32_t(off - 7); r.vlen = count;
            break;
        }
        default:
            fail(c, "command not served by --net=uring (supported: PUT GET DEL PING MPUT MGET)");
            return;
        }

        batch.push_back(r);
        ++k.outstanding;
        k.in_pos += need;
        srv->requests.fetch_add(1, std::memory_order_relaxed);
        if (cmd == VXNF_PUT) {
            ++queued_puts;
            continue;
        }
        if (cmd == VXNF_DEL || cmd == VXNF_MPUT) {
            k.blocked = true;
            break;
        }
        continue;
    incomplete:
        break;
    }
    if (queued_puts > 0) k.blocked = true;
}

void Loop::exec() {
    // Answers made inside vxnfExec (reads) land directly in out_pending; they
    // may add conns to `touched`, never to `batch`, so batch is stable here.
    while (!batch.empty()) {
        srv->batches.fetch_add(1, std::memory_order_relaxed);
        vxnfExec(srv->cfg.handle, batch.data(), int(batch.size()));
        batch.clear();
        // A read answered inline cannot unblock anything (only writes block),
        // so nothing new can have been parsed: one call drains the batch.
    }
}

void Loop::maybe_send(uint32_t c) {
    Conn& k = conns[c];
    if (k.send_armed || k.fd < 0) return;
    if (k.out_inflight.empty() && !k.out_pending.empty()) {
        k.out_inflight.swap(k.out_pending);
        k.inflight_off = 0;
    }
    if (k.out_inflight.empty()) return;
    be->arm_send(c, k.out_inflight.data() + k.inflight_off, k.out_inflight.size() - k.inflight_off);
    k.send_armed = true;
}

void Loop::post() {
    std::vector<uint32_t> work;
    work.swap(touched); // touch() during this pass queues for the next pass
    std::sort(work.begin(), work.end());
    work.erase(std::unique(work.begin(), work.end()), work.end());
    for (uint32_t c : work) {
        Conn& k = conns[c];
        if (!k.live) continue;

        if (!k.fail_msg.empty()) {
            const uint32_t n = uint32_t(k.fail_msg.size());
            uint8_t hdr[5] = {kStatusErr, uint8_t(n), uint8_t(n >> 8), uint8_t(n >> 16), uint8_t(n >> 24)};
            append_response(k, hdr, 5);
            append_response(k, k.fail_msg.data(), n);
            k.fail_msg.clear();
        }
        maybe_send(c);

        if (k.closing) {
            // Flush what is queued, then shut down; release only once the
            // kernel holds no buffer of this connection (rule 3).
            if (!k.send_armed && k.out_inflight.empty() && k.out_pending.empty()) {
                if (k.fd >= 0) ::shutdown(k.fd, SHUT_RDWR);
                if (!k.recv_armed) release(c);
            }
            continue;
        }

        // Normally only a write unblocks a connection, via on_wake(). Handle
        // an inline answer to a write too, rather than leave it wedged.
        if (k.blocked && k.outstanding == 0) {
            k.blocked = false;
            const size_t before = batch.size();
            parse(c);
            if (batch.size() != before) {
                touch(c); // its reqs point into k.in: no compaction until exec'd
                continue;
            }
        }

        if (k.recv_armed) continue; // buffer lent to the kernel: do not move it (rule 2)

        // Rule 1: every req parsed from this buffer has been exec'd.
        if (k.in_pos > 0) {
            std::memmove(k.in.data(), k.in.data() + k.in_pos, k.in_len - k.in_pos);
            k.in_len -= k.in_pos;
            k.in_pos = 0;
        }
        // Read ahead while blocked too (bounded) so pipelined requests keep
        // arriving behind a write; stop reading from a client that is not
        // reading its answers.
        if (k.in_len >= kInHighMark && k.in_need <= k.in_len) continue;
        if (k.out_pending.size() + k.out_inflight.size() > kOutHighMark) continue;
        const size_t want = std::max(k.in_len + kReadChunk, k.in_need);
        if (k.in.size() < want) k.in.resize(want);
        be->arm_recv(c, k.in.data() + k.in_len, k.in.size() - k.in_len);
        k.recv_armed = true;
    }
}

bool make_listener(const std::string& host, int port, bool reuseport, int& fd_out, int& port_out, std::string& err) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { err = std::string("socket: ") + std::strerror(errno); return false; }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
#ifdef SO_REUSEPORT
    if (reuseport) ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
#endif
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port   = htons(uint16_t(port));
    if (host.empty() || host == "0.0.0.0") {
        a.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (::inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) {
        ::close(fd);
        err = "bad bind address (IPv4 only): " + host;
        return false;
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof a) < 0 || ::listen(fd, 4096) < 0) {
        err = std::string("bind/listen ") + host + ":" + std::to_string(port) + ": " + std::strerror(errno);
        ::close(fd);
        return false;
    }
    socklen_t al = sizeof a;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &al);
    port_out = ntohs(a.sin_port);
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
    fd_out = fd;
    return true;
}

} // namespace

extern "C" {

vxnf_server* vxnf_start(vxnf_config cfg, char* err, size_t errlen) {
    auto s = std::make_unique<vxnf_server>();
    s->cfg  = cfg;
    s->host = cfg.host ? cfg.host : "";
    int threads = cfg.threads > 0 ? cfg.threads : 1;
#ifndef __linux__
    // SO_REUSEPORT only load-balances accepts on Linux; elsewhere one loop.
    threads = 1;
#endif
    const int depth = cfg.ring_depth > 0 ? cfg.ring_depth : 4096;

    int port = cfg.port;
    for (int i = 0; i < threads; ++i) {
        auto l = std::make_unique<Loop>();
        l->srv = s.get();
        l->idx = uint32_t(i);
        std::string e;
        int bound = 0;
        if (!make_listener(s->host, port, threads > 1, l->lfd, bound, e)) {
            set_err(err, errlen, e);
            return nullptr; // Loop destructors close nothing yet; fds leak only on this error path
        }
        port = bound; // later loops join the same port
        int p[2];
        if (::pipe(p) < 0) { set_err(err, errlen, std::string("pipe: ") + std::strerror(errno)); return nullptr; }
        ::fcntl(p[0], F_SETFL, ::fcntl(p[0], F_GETFL) | O_NONBLOCK);
        ::fcntl(p[1], F_SETFL, ::fcntl(p[1], F_GETFL) | O_NONBLOCK);
        l->wake_r = p[0];
        l->wake_w = p[1];

        bool want_uring = cfg.backend != VXNF_BACKEND_POLL;
#if defined(__linux__) && defined(VXNF_HAVE_URING)
        if (want_uring) {
            auto u = std::make_unique<UringBackend>();
            if (u->init(*l, depth, e)) {
                l->be = std::move(u);
            } else if (cfg.backend == VXNF_BACKEND_URING) {
                set_err(err, errlen, e);
                return nullptr;
            }
        }
#else
        if (want_uring && cfg.backend == VXNF_BACKEND_URING) {
            set_err(err, errlen, "io_uring backend not built into this binary (Linux + liburing only)");
            return nullptr;
        }
#endif
        if (!l->be) {
            auto pb = std::make_unique<PollBackend>();
            pb->init(*l, depth, e);
            l->be = std::move(pb);
        }
        s->loops.push_back(std::move(l));
    }
    s->port    = port;
    s->backend = s->loops[0]->be->name();
    for (auto& l : s->loops) {
        Loop* lp = l.get();
        lp->th = std::thread([lp] { lp->run(); });
    }
    return s.release();
}

int vxnf_port(const vxnf_server* s) { return s->port; }
const char* vxnf_backend(const vxnf_server* s) { return s->backend; }

void vxnf_stop(vxnf_server* s) {
    if (s->stopped.exchange(true)) return;
    // Only ever signal the loops: each one shuts its own connections down on
    // its own thread (Loop::start_stopping) — no other thread touches conns.
    for (auto& l : s->loops) {
        l->stop_requested.store(true);
        l->wake();
    }
    for (auto& l : s->loops) {
        if (l->th.joinable()) l->th.join();
        l->be.reset(); // nothing is armed any more: safe to tear the ring down
        if (l->lfd >= 0) ::close(l->lfd);
        l->lfd = -1;
    }
}

void vxnf_free(vxnf_server* s) {
    if (!s) return;
    for (auto& l : s->loops) {
        if (l->wake_r >= 0) ::close(l->wake_r);
        if (l->wake_w >= 0) ::close(l->wake_w);
    }
    delete s;
}

void vxnf_respond(vxnf_server* s, uint32_t loop, uint64_t conn, const void* data, size_t len) {
    if (s->stopped.load(std::memory_order_relaxed) || loop >= s->loops.size()) return;
    Loop* l = s->loops[loop].get();
    if (tl_loop == l) {
        // Inside vxnfExec on the loop's own thread: no locking needed.
        const uint32_t c = token_idx(conn);
        if (c >= l->conns.size()) return;
        Conn& k = l->conns[c];
        if (!k.live || k.gen != token_gen(conn)) return;
        l->append_response(k, data, len);
        if (k.outstanding) --k.outstanding;
        l->touch(c);
        return;
    }
    Completion comp;
    comp.conn = conn;
    comp.data.assign(static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + len);
    {
        std::lock_guard<std::mutex> g(l->cmu);
        l->completions.push_back(std::move(comp));
    }
    l->wake();
}

uint64_t vxnf_stat_iterations(const vxnf_server* s) { return s->iterations.load(std::memory_order_relaxed); }
uint64_t vxnf_stat_requests(const vxnf_server* s) { return s->requests.load(std::memory_order_relaxed); }
uint64_t vxnf_stat_batches(const vxnf_server* s) { return s->batches.load(std::memory_order_relaxed); }

} // extern "C"
