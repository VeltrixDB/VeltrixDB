#!/usr/bin/env bash
# net-bench.sh — compare network front-ends (--net) on one machine.
#
# For every ENGINE x NET x WINDOW combination: start a fresh server, run the three
# workloads the README's comparison table uses, and print one markdown table
# (also appended to $GITHUB_STEP_SUMMARY when set).
#
#   batch write   MPUT, 8 clients x 1024 keys, overwrites in a 1M-key space
#   read          single GET, 64 clients, binary protocol
#   mixed         70% GET / 30% PUT, 64 clients, binary protocol
#
# Env (defaults in brackets):
#   NETS        front-ends to compare                 [go uring]
#   ENGINES     storage engine: cgo (C++ layer on) and/or go
#               (VELTRIXDB_DISABLE_CGO_ENGINE=1)      [cgo]
#   WINDOWS     WAL/VLog group-commit windows, ms     [5]
#   DATA_ROOT   where data dirs go                    [mktemp -d]
#   DURATION    seconds per read/mixed workload       [15]
#   BATCH_DURATION seconds of batch write             [10]
#   WATCHDOG_SLACK seconds past a workload's duration before it counts as hung [60]
#   NUM_KEYS    keyspace                              [1000000]
#   NET_THREADS event loops for C++ front-ends        [nproc]
#   PORT        server port                           [9700]
#
# Every overwrite appends to the WAL and VLog and nothing reclaims space
# during a run, so batch write grows the data dir by several GB per second
# of load (~10 GB for one combination on an M-series Mac). BATCH_DURATION
# caps that, and each combination's data dir is deleted when it finishes —
# without both, a tmpfs DATA_ROOT fills and every later write fails ENOSPC.
#
# Client and server share the machine, so absolute numbers understate a real
# deployment; compare rows against each other, not against other hardware.

set -euo pipefail

NETS="${NETS:-go uring}"
ENGINES="${ENGINES:-cgo}"
WINDOWS="${WINDOWS:-5}"
DURATION="${DURATION:-15}"
BATCH_DURATION="${BATCH_DURATION:-10}"
WATCHDOG_SLACK="${WATCHDOG_SLACK:-60}"
NUM_KEYS="${NUM_KEYS:-1000000}"
NET_THREADS="${NET_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
PORT="${PORT:-9700}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
DATA_ROOT="${DATA_ROOT:-$WORK}"
trap 'pkill -f "$WORK/veltrixdb" 2>/dev/null || true; rm -rf "$WORK"' EXIT

echo "building (CGO_ENABLED=1)…"
(cd "$ROOT" && CGO_ENABLED=1 go build -o "$WORK/veltrixdb" ./cmd/server)
(cd "$ROOT" && CGO_ENABLED=1 go build -o "$WORK/loadtest" ./cmd/loadtest)

LT=("$WORK/loadtest" --addr "127.0.0.1:$PORT" --num-keys "$NUM_KEYS" --value-size 128)

# metric SECTION FIELD FILE — e.g. metric WRITES P99 out.txt → "4.160 ms"
metric() {
  python3 - "$1" "$2" "$3" <<'PY'
import re, sys
section, field, path = sys.argv[1], sys.argv[2], sys.argv[3]
cur = None
for line in open(path):
    m = re.search(r"── (WRITES|READS)", line)
    if m:
        cur = m.group(1)
        continue
    if cur != section:
        continue
    s = line.strip()
    if field == "Throughput" and s.startswith("Throughput:"):
        print(s.split(":", 1)[1].strip()); break
    if s.startswith(field + " ") and not s.startswith(field + "."):
        print(" ".join(s.split()[1:3])); break
PY
}

# ── One combination = one fresh server + three workloads ────────────────────
#
# A failure never aborts the run: it dumps diagnostics, records a FAILED row
# and moves on, so one CI run shows every combination. The script still exits
# non-zero at the end if anything failed.

FAILED=0

# diag — everything needed to diagnose a failed combination.
diag() {
  echo "::group::diagnostics: $1"
  for f in "$WORK"/w.txt "$WORK"/r.txt "$WORK"/m.txt; do
    [[ -s "$f" ]] && { echo "──── $(basename "$f") (last 40 lines)"; tail -n 40 "$f"; }
  done
  echo "──── df $DATA_ROOT"; df -h "$DATA_ROOT" || true
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    # Native stacks first (C++ threads: io_uring bridge, batch engine,
    # netfront loops) — SIGQUIT below kills the process.
    if command -v gdb >/dev/null; then
      echo "──── native thread backtraces (gdb)"
      sudo -n gdb -p "$pid" -batch -nx -ex "set pagination off" \
        -ex "thread apply all bt 25" 2>&1 | grep -vE '^\[New LWP|^warning:' | head -n 1500 || true
    fi
    echo "──── SIGQUIT → Go goroutine dump"
    kill -QUIT "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  fi
  if [[ -n "${pid:-}" ]]; then
    kill -KILL "$pid" 2>/dev/null || true
    local st=0; wait "$pid" 2>/dev/null || st=$?
    echo "server exit status: $st"
  fi
  if [[ -n "${log:-}" && -f "$log" ]]; then
    echo "──── server log ($log, last 4000 lines)"; tail -n 4000 "$log"
  fi
  command -v dmesg >/dev/null && { sudo -n dmesg 2>/dev/null | tail -n 20 || true; }
  echo "::endgroup::"
  pid=""
}

# fail MSG — mark this combination failed; the caller returns 1.
fail() {
  echo "::error::net-bench [$tag]: $1"
  diag "$tag"
  ROWS+=("| $engine | $net | ${w} | FAILED: $1 | | | | | | | | |")
  FAILED=1
}

# lt NAME LIMIT_S ARGS… — run one loadtest workload into $WORK/NAME.txt.
# LIMIT_S is a watchdog: loadtest has no request timeout, so a server that
# stops answering would otherwise hang the job until the Actions timeout
# (which is what happened: 45 min, no output).
lt() {
  local name=$1 limit=$2; shift 2
  "${LT[@]}" "$@" >"$WORK/$name.txt" 2>&1 &
  local lpid=$! waited=0
  while kill -0 "$lpid" 2>/dev/null; do
    if (( waited >= limit * 10 )); then
      echo "── [$tag] '$name' still running after ${limit}s — server looks hung"
      fail "'$name' hung (>${limit}s)"
      kill -KILL "$lpid" 2>/dev/null || true; wait "$lpid" 2>/dev/null || true
      return 1
    fi
    sleep 0.1; waited=$((waited + 1))
  done
  local rc=0; wait "$lpid" || rc=$?
  if (( rc != 0 )); then fail "loadtest '$name' exited $rc"; return 1; fi
  if ! kill -0 "$pid" 2>/dev/null; then fail "server died during '$name'"; return 1; fi
  if grep -qE 'Errors: +[1-9]' "$WORK/$name.txt"; then
    fail "loadtest '$name' reported errors"; return 1
  fi
}

wait_port() {
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    kill -0 "$pid" 2>/dev/null || return 1
    sleep 0.1
  done
  return 1
}

run_combo() {
  local data="$DATA_ROOT/vx-$engine-$net-$w"
  rm -rf "$data"
  log="$WORK/server-$engine-$net-$w.log"
  local disable=0
  [[ $engine == go ]] && disable=1
  VELTRIXDB_DISABLE_CGO_ENGINE=$disable GOTRACEBACK=all \
    "$WORK/veltrixdb" -addr "127.0.0.1:$PORT" -metrics-addr "127.0.0.1:0" -data "$data" -cache 2048 \
      --wal-flush-window-ms "$w" --vlog-flush-window-ms "$w" \
      --net "$net" --net-threads "$NET_THREADS" >"$log" 2>&1 &
  pid=$!
  if ! wait_port; then fail "server did not start"; rm -rf "$data"; return 1; fi
  grep -E 'front-end listening|plaintext listening' "$log" || true

  rm -f "$WORK"/w.txt "$WORK"/r.txt "$WORK"/m.txt
  local ok=1
  echo "── [$tag] batch write"
  lt w $((BATCH_DURATION + 2 + WATCHDOG_SLACK)) --duration "$BATCH_DURATION" --mode write --concurrency 8 --batch-size 1024 --warmup 2 || ok=0
  if (( ok )); then
    echo "── [$tag] read"
    lt r $((DURATION + WATCHDOG_SLACK)) --duration "$DURATION" --mode read --proto binary --concurrency 64 || ok=0
  fi
  if (( ok )); then
    echo "── [$tag] mixed"
    lt m $((DURATION + 5 + WATCHDOG_SLACK)) --duration "$DURATION" --mode mixed --proto binary --concurrency 64 --read-ratio 0.7 || ok=0
  fi

  if (( ok )); then
    ROWS+=("| $engine | $net | ${w} | $(metric WRITES Throughput "$WORK/w.txt") | $(metric WRITES P50 "$WORK/w.txt") | $(metric WRITES P99 "$WORK/w.txt") | $(metric READS Throughput "$WORK/r.txt") | $(metric READS P50 "$WORK/r.txt") | $(metric READS P99 "$WORK/r.txt") | $(metric READS P99 "$WORK/m.txt") | $(metric WRITES P50 "$WORK/m.txt") | $(metric WRITES P99 "$WORK/m.txt") |")
    kill -INT "$pid" 2>/dev/null || true
    wait "$pid" || true
    pid=""
  fi
  echo "── [$tag] data dir $(du -sh "$data" 2>/dev/null | cut -f1), removing"
  rm -rf "$data"
}

ROWS=()
for engine in $ENGINES; do
  for net in $NETS; do
    for w in $WINDOWS; do
      tag="engine=$engine net=$net window=${w}ms"
      run_combo || true
    done
  done
done

{
  echo
  echo "### Network front-end comparison ($(nproc 2>/dev/null || echo ?) CPUs, client on the same host, batch ${BATCH_DURATION}s, read/mixed ${DURATION}s)"
  echo
  echo "| storage engine | --net | window ms | batch write keys/s | batch P50 | batch P99 | read ops/s | read P50 | read P99 | mixed read P99 | mixed write P50 | mixed write P99 |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|---|"
  printf '%s\n' "${ROWS[@]}"
  echo
  echo "storage engine: cgo = C++ storage layer on (io_uring VLog bridge + batch engine, Linux only); go = VELTRIXDB_DISABLE_CGO_ENGINE=1. The index is the native C++ table in both."
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"

exit "$FAILED"
