#!/usr/bin/env bash
# net-bench.sh — compare network front-ends (--net) on one machine.
#
# For every NET x WINDOW combination: start a fresh server, run the three
# workloads the README's comparison table uses, and print one markdown table
# (also appended to $GITHUB_STEP_SUMMARY when set).
#
#   batch write   MPUT, 8 clients x 1024 keys, overwrites in a 1M-key space
#   read          single GET, 64 clients, binary protocol
#   mixed         70% GET / 30% PUT, 64 clients, binary protocol
#
# Env (defaults in brackets):
#   NETS        front-ends to compare                 [go uring]
#   WINDOWS     WAL/VLog group-commit windows, ms     [5]
#   DATA_ROOT   where data dirs go                    [mktemp -d]
#   DURATION    seconds per workload                  [15]
#   NUM_KEYS    keyspace                              [1000000]
#   NET_THREADS event loops for C++ front-ends        [nproc]
#   PORT        server port                           [9700]
#
# Client and server share the machine, so absolute numbers understate a real
# deployment; compare rows against each other, not against other hardware.

set -euo pipefail

NETS="${NETS:-go uring}"
WINDOWS="${WINDOWS:-5}"
DURATION="${DURATION:-15}"
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

LT=("$WORK/loadtest" --addr "127.0.0.1:$PORT" --num-keys "$NUM_KEYS" --value-size 128 --duration "$DURATION")

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

# die MSG — print everything needed to diagnose a failed combination, then exit.
# Loadtest output goes to files, so without this a failure under `set -e`
# leaves nothing in the CI log but "exit code 1".
die() {
  echo "::error::net-bench: $1"
  for f in "$WORK"/w.txt "$WORK"/r.txt "$WORK"/m.txt; do
    [[ -s "$f" ]] && { echo "──── $(basename "$f")"; tail -n 60 "$f"; }
  done
  if [[ -n "${log:-}" && -f "$log" ]]; then
    echo "──── server log ($log)"; tail -n 200 "$log"
  fi
  if [[ -n "${pid:-}" ]]; then
    if kill -0 "$pid" 2>/dev/null; then echo "server pid $pid still running"
    else wait "$pid"; echo "server pid $pid exited with status $?"; fi
  fi
  command -v dmesg >/dev/null && sudo -n dmesg 2>/dev/null | tail -n 20 || true
  exit 1
}

# lt NAME ARGS… — run one loadtest workload into $WORK/NAME.txt; fail loudly.
lt() {
  local name=$1; shift
  local rc=0
  "${LT[@]}" "$@" >"$WORK/$name.txt" 2>&1 || rc=$?
  (( rc == 0 )) || die "net=$net window=${w}ms: loadtest '$name' exited $rc"
  kill -0 "$pid" 2>/dev/null || die "net=$net window=${w}ms: server died during '$name'"
  if grep -qE 'Errors: +[1-9]' "$WORK/$name.txt"; then
    die "net=$net window=${w}ms: loadtest '$name' reported errors"
  fi
}

wait_port() {
  for _ in $(seq 1 100); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0
    sleep 0.1
  done
  echo "server did not open port $PORT" >&2
  return 1
}

ROWS=()
for net in $NETS; do
  for w in $WINDOWS; do
    data="$DATA_ROOT/vx-$net-$w"
    rm -rf "$data"
    log="$WORK/server-$net-$w.log"
    "$WORK/veltrixdb" -addr "127.0.0.1:$PORT" -metrics-addr "127.0.0.1:0" -data "$data" -cache 2048 \
      --wal-flush-window-ms "$w" --vlog-flush-window-ms "$w" \
      --net "$net" --net-threads "$NET_THREADS" >"$log" 2>&1 &
    pid=$!
    wait_port || die "net=$net window=${w}ms: server did not start"
    grep -E 'front-end listening|plaintext listening' "$log" || true

    rm -f "$WORK"/w.txt "$WORK"/r.txt "$WORK"/m.txt
    echo "── net=$net window=${w}ms: batch write"
    lt w --mode write --concurrency 8 --batch-size 1024 --warmup 2
    echo "── net=$net window=${w}ms: read"
    lt r --mode read --proto binary --concurrency 64
    echo "── net=$net window=${w}ms: mixed"
    lt m --mode mixed --proto binary --concurrency 64 --read-ratio 0.7

    ROWS+=("| $net | ${w} | $(metric WRITES Throughput "$WORK/w.txt") | $(metric WRITES P50 "$WORK/w.txt") | $(metric WRITES P99 "$WORK/w.txt") | $(metric READS Throughput "$WORK/r.txt") | $(metric READS P50 "$WORK/r.txt") | $(metric READS P99 "$WORK/r.txt") | $(metric READS P99 "$WORK/m.txt") | $(metric WRITES P50 "$WORK/m.txt") | $(metric WRITES P99 "$WORK/m.txt") |")

    kill -INT "$pid" 2>/dev/null || true
    wait "$pid" || true
    pid=""
  done
done

{
  echo
  echo "### Network front-end comparison ($(nproc 2>/dev/null || echo ?) CPUs, client on the same host, ${DURATION}s per run)"
  echo
  echo "| --net | window ms | batch write keys/s | batch P50 | batch P99 | read ops/s | read P50 | read P99 | mixed read P99 | mixed write P50 | mixed write P99 |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|"
  printf '%s\n' "${ROWS[@]}"
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
