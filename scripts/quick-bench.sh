#!/usr/bin/env bash
# quick-bench.sh — a 2-minute benchmark of one VeltrixDB node with nothing to
# set up: builds the server and loadtest, starts a throwaway server on a temp
# directory, runs four phases and prints one table.
#
#   ./scripts/quick-bench.sh
#   DURATION=30 CONCURRENCY=128 VALUE_SIZE=1024 ./scripts/quick-bench.sh
#   ADDR=10.0.0.5:9000 ./scripts/quick-bench.sh   # an already running server
#
# Phases (each DURATION seconds):
#   write   single-key PUTs, each acknowledged after fdatasync
#   batch   MPUT, BATCH_SIZE keys per request
#   read    GETs over the keys written above
#   mixed   READ_RATIO reads / the rest writes
#
# Env:
#   DURATION     15        CONCURRENCY  64       VALUE_SIZE  128
#   NUM_KEYS     200000    BATCH_SIZE   256      READ_RATIO  0.7
#   ADDR         empty = start a local server on 127.0.0.1:${PORT:-9300}
#   PROTO        binary (default) | text
#   CGO_ENABLED  build mode (default: Go's default — 1 where a C++ compiler
#                exists, which is what the Docker image runs)
#   OUT_DIR      temp dir — binaries, server.log, one log per phase
#   SERVER_FLAGS extra server flags, e.g. "-cache 4096"
#
# This is a smoke-level number on whatever machine you run it on. For the
# gated storage benchmark use scripts/bench.sh; for YCSB and the comparison
# with Aerospike / ScyllaDB use bench/compare (QUICKSTART.md has all three).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DURATION="${DURATION:-15}"
CONCURRENCY="${CONCURRENCY:-64}"
VALUE_SIZE="${VALUE_SIZE:-128}"
NUM_KEYS="${NUM_KEYS:-200000}"
BATCH_SIZE="${BATCH_SIZE:-256}"
READ_RATIO="${READ_RATIO:-0.7}"
PROTO="${PROTO:-binary}"
PORT="${PORT:-9300}"
ADDR="${ADDR:-}"
SERVER_FLAGS="${SERVER_FLAGS:-}"
OUT_DIR="${OUT_DIR:-$(mktemp -d "${TMPDIR%/}/veltrix-quickbench.XXXXXX")}"
mkdir -p "$OUT_DIR"

echo "building into $OUT_DIR"
(cd "$ROOT" && go build -o "$OUT_DIR/veltrixdb" ./cmd/server && go build -o "$OUT_DIR/loadtest" ./cmd/loadtest)

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    rm -rf "$OUT_DIR/data"
  fi
}
trap cleanup EXIT INT TERM

if [ -z "$ADDR" ]; then
  ADDR="127.0.0.1:$PORT"
  # shellcheck disable=SC2086
  "$OUT_DIR/veltrixdb" -addr "$ADDR" -metrics-addr "127.0.0.1:$((PORT + 4))" \
    -data "$OUT_DIR/data" $SERVER_FLAGS >"$OUT_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$((PORT + 4))/readyz" >/dev/null 2>&1 && break
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "server exited:" >&2; tail -20 "$OUT_DIR/server.log" >&2; exit 1; }
    sleep 0.5
  done
fi

run_phase() { # name, loadtest args...
  local name=$1; shift
  echo "── $name (${DURATION}s)"
  "$OUT_DIR/loadtest" --addr "$ADDR" --proto "$PROTO" --duration "$DURATION" \
    --concurrency "$CONCURRENCY" --num-keys "$NUM_KEYS" --value-size "$VALUE_SIZE" \
    --report-every 5 "$@" >"$OUT_DIR/$name.log" 2>&1 || {
    echo "   failed; see $OUT_DIR/$name.log" >&2; tail -5 "$OUT_DIR/$name.log" >&2; return 1; }
}

# The batch phase writes every key first so the read phase finds them.
run_phase batch --mode write --batch-size "$BATCH_SIZE" --warmup 0
run_phase write --mode write --warmup 2
run_phase read --mode read
run_phase mixed --mode mixed --read-ratio "$READ_RATIO"

# Pull throughput / P50 / P99 out of each phase's result block.
summary() { # file, section label (WRITES|READS)
  awk -v sec="$2" '
    /── / { insec = ($0 ~ sec) }
    insec && /Throughput:/ { tput = $2 }
    insec && /Errors:/     { errs = $2 }
    insec && $1 == "P50" && p50 == "" { p50 = $2 " " $3 }
    insec && $1 == "P99" && p99 == "" { p99 = $2 " " $3 }
    END { printf "%s|%s|%s|%s", tput, p50, p99, errs }' "$1"
}

echo
echo "VeltrixDB quick bench — $(uname -sm), $CONCURRENCY workers, ${VALUE_SIZE} B values, ${DURATION}s per phase"
printf '%-24s %14s %12s %12s %8s\n' phase "ops/s" P50 P99 errors
row() {
  IFS='|' read -r t p50 p99 e <<<"$(summary "$OUT_DIR/$2.log" "$3")"
  printf '%-24s %14s %12s %12s %8s\n' "$1" "${t:--}" "${p50:--}" "${p99:--}" "${e:--}"
}
row "MPUT x$BATCH_SIZE (keys/s)" batch WRITES
row "PUT (durable)" write WRITES
row "GET" read READS
row "mixed: reads" mixed READS
row "mixed: writes" mixed WRITES
echo
echo "MPUT latency is per request of $BATCH_SIZE keys. Full output: $OUT_DIR/*.log"
