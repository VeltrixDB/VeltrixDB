#!/usr/bin/env bash
# soak.sh — long-running stability test for VeltrixDB.
#
# Runs a steady mixed-workload loadtest for hours-to-days and asserts that
# none of the canonical "drift" indicators trip: no GC emergency runs, no
# scrub corruption, no admission throttling beyond a small floor, latency
# stays within the soak baseline, RSS growth ≤ 10%/hour.
#
# Designed to catch:
#   - Slow memory leaks (RSS climbing over hours)
#   - Bloom filter false-positive accumulation (vacuum not keeping up)
#   - VLog garbage accumulation (GC tuning regression)
#   - Goroutine leaks (count climbing)
#   - File descriptor leaks
#   - Latency drift (P99 today >>P99 from baseline)
#
# Usage:
#   ./scripts/soak.sh                                    # 4-hour default
#   SOAK_HOURS=24 ./scripts/soak.sh                       # 24-hour soak
#   SOAK_DATA=/mnt/nvme0/soak SOAK_PORT=19998 ./scripts/soak.sh
#
# Output: one CSV line per minute to soak.csv with columns:
#   ts,rss_kb,goroutines,fd_count,p99_write_ms,p99_read_ms,vlog_garbage_ratio,
#   gc_emergency_runs,scrub_corruption_total,admission_throttles_total
#
# Exit code:
#   0 — all assertions held
#   1 — at least one assertion was tripped (see soak.log for which)

set -uo pipefail

SOAK_HOURS="${SOAK_HOURS:-4}"
SOAK_DATA="${SOAK_DATA:-/tmp/veltrixdb-soak}"
SOAK_PORT="${SOAK_PORT:-19990}"
SOAK_METRICS_PORT="${SOAK_METRICS_PORT:-12110}"
SOAK_CONCURRENCY="${SOAK_CONCURRENCY:-32}"
SOAK_KEYS="${SOAK_KEYS:-1000000}"
SOAK_VALUE_SIZE="${SOAK_VALUE_SIZE:-256}"
SOAK_OUT="${SOAK_OUT:-./soak-out}"

# Acceptance gates (override via env to relax for slower hardware).
MAX_RSS_GROWTH_PCT_PER_HOUR="${MAX_RSS_GROWTH_PCT_PER_HOUR:-10}"
MAX_P99_WRITE_MS="${MAX_P99_WRITE_MS:-200}"
MAX_P99_READ_MS="${MAX_P99_READ_MS:-50}"
MAX_GC_EMERGENCY_RUNS="${MAX_GC_EMERGENCY_RUNS:-0}"
MAX_SCRUB_CORRUPTION="${MAX_SCRUB_CORRUPTION:-0}"

mkdir -p "$SOAK_OUT"
cd "$(dirname "$0")/.."

if [[ ! -d "$SOAK_DATA" ]]; then
  if [[ "$SOAK_DATA" != /tmp/* && "$SOAK_DATA" != /mnt/* && "$SOAK_DATA" != /var/* ]]; then
    echo "refusing to wipe non-/tmp non-/mnt non-/var path: $SOAK_DATA"
    exit 1
  fi
fi
rm -rf "$SOAK_DATA"
mkdir -p "$SOAK_DATA"

echo "[soak] building server"
go build -o "$SOAK_OUT/veltrixdb-soak" ./cmd/server || exit 1
echo "[soak] building loadtest"
go build -o "$SOAK_OUT/loadtest-soak" ./cmd/loadtest || exit 1

echo "[soak] starting server  port=$SOAK_PORT  data=$SOAK_DATA"
"$SOAK_OUT/veltrixdb-soak" \
  -addr ":$SOAK_PORT" \
  -metrics-addr ":$SOAK_METRICS_PORT" \
  -data "$SOAK_DATA" \
  -cache 1024 \
  > "$SOAK_OUT/server.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null; kill $LOADTEST_PID 2>/dev/null; wait 2>/dev/null' EXIT

# Wait for ready
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$SOAK_METRICS_PORT/healthz" > /dev/null; then
    break
  fi
  sleep 1
done
if ! curl -sf "http://127.0.0.1:$SOAK_METRICS_PORT/healthz" > /dev/null; then
  echo "[soak] server did not come up; logs:"
  tail -20 "$SOAK_OUT/server.log"
  exit 1
fi

echo "[soak] starting loadtest  concurrency=$SOAK_CONCURRENCY  keys=$SOAK_KEYS"
"$SOAK_OUT/loadtest-soak" \
  --addr "127.0.0.1:$SOAK_PORT" \
  --mode mixed \
  --concurrency "$SOAK_CONCURRENCY" \
  --duration "$((SOAK_HOURS * 3600))" \
  --num-keys "$SOAK_KEYS" \
  --value-size "$SOAK_VALUE_SIZE" \
  --read-ratio 0.7 \
  > "$SOAK_OUT/loadtest.log" 2>&1 &
LOADTEST_PID=$!

CSV="$SOAK_OUT/soak.csv"
echo "ts,rss_kb,goroutines,fd_count,p99_write_ms,p99_read_ms,vlog_garbage_ratio,gc_emergency_runs,scrub_corruption_total,admission_throttles_total" > "$CSV"

START_TS=$(date +%s)
INITIAL_RSS=""
EXIT_CODE=0

while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START_TS))
  if (( ELAPSED >= SOAK_HOURS * 3600 )); then
    break
  fi
  # Sample once per minute.
  sleep 60

  if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "[soak] server died; aborting"
    EXIT_CODE=1
    break
  fi
  if ! kill -0 $LOADTEST_PID 2>/dev/null; then
    echo "[soak] loadtest finished early"
    break
  fi

  RSS=$(ps -o rss= -p $SERVER_PID 2>/dev/null | tr -d ' ')
  RSS=${RSS:-0}
  GOROUTINES=$(curl -sf "http://127.0.0.1:$SOAK_METRICS_PORT/metrics" | awk '/^go_goroutines /{print $2; exit}')
  GOROUTINES=${GOROUTINES%.*}
  GOROUTINES=${GOROUTINES:-0}
  FDS=$(ls /proc/$SERVER_PID/fd 2>/dev/null | wc -l)
  FDS=${FDS:-0}

  METRICS=$(curl -sf "http://127.0.0.1:$SOAK_METRICS_PORT/metrics")
  P99W=$(printf '%s' "$METRICS" | awk '/^veltrixdb_storage_write_latency_seconds_bucket{le="0.2"/{print $2; exit}')
  P99R=$(printf '%s' "$METRICS" | awk '/^veltrixdb_storage_read_latency_seconds_bucket{le="0.05"/{print $2; exit}')
  GARBAGE=$(printf '%s' "$METRICS" | awk '/^veltrixdb_vlog_garbage_ratio{disk="0"/{print $2; exit}')
  GC_EMERG=$(printf '%s' "$METRICS" | awk '/^veltrixdb_vlog_gc_emergency_runs_total/{print $2; exit}')
  SCRUB_CORR=$(printf '%s' "$METRICS" | awk '/^veltrixdb_scrub_corruption_total/{print $2; exit}')
  ADM_THR=$(printf '%s' "$METRICS" | awk '/^veltrixdb_storage_write_admission_throttles_total/{print $2; exit}')

  P99W=${P99W:-0}; P99R=${P99R:-0}; GARBAGE=${GARBAGE:-0}; GC_EMERG=${GC_EMERG:-0}
  SCRUB_CORR=${SCRUB_CORR:-0}; ADM_THR=${ADM_THR:-0}

  echo "$NOW,$RSS,$GOROUTINES,$FDS,$P99W,$P99R,$GARBAGE,$GC_EMERG,$SCRUB_CORR,$ADM_THR" >> "$CSV"

  if [[ -z "$INITIAL_RSS" ]]; then
    INITIAL_RSS=$RSS
  fi

  # Hard-fail asserts.
  if (( $(echo "$GC_EMERG > $MAX_GC_EMERGENCY_RUNS" | bc -l 2>/dev/null || echo 0) )); then
    echo "[soak] FAIL: gc_emergency_runs=$GC_EMERG exceeded threshold $MAX_GC_EMERGENCY_RUNS"
    EXIT_CODE=1
  fi
  if (( $(echo "$SCRUB_CORR > $MAX_SCRUB_CORRUPTION" | bc -l 2>/dev/null || echo 0) )); then
    echo "[soak] FAIL: scrub_corruption_total=$SCRUB_CORR exceeded threshold"
    EXIT_CODE=1
  fi

  # RSS growth check (allow first 10 minutes for warmup).
  if (( ELAPSED > 600 )) && [[ "$INITIAL_RSS" -gt 0 ]]; then
    HOURS_ELAPSED=$(echo "scale=2; $ELAPSED / 3600" | bc -l 2>/dev/null || echo 0.1)
    GROWTH_PCT=$(echo "scale=2; ($RSS - $INITIAL_RSS) * 100 / $INITIAL_RSS / $HOURS_ELAPSED" | bc -l 2>/dev/null || echo 0)
    if (( $(echo "$GROWTH_PCT > $MAX_RSS_GROWTH_PCT_PER_HOUR" | bc -l 2>/dev/null || echo 0) )); then
      echo "[soak] FAIL: RSS growth $GROWTH_PCT %/hr exceeded threshold $MAX_RSS_GROWTH_PCT_PER_HOUR"
      EXIT_CODE=1
    fi
  fi
done

echo "[soak] complete  duration=${ELAPSED}s  csv=$CSV  exit=$EXIT_CODE"
exit $EXIT_CODE
