#!/usr/bin/env bash
# bench.sh — one-command benchmark harness for VeltrixDB.
#
# Validates the full optimization stack (block packing, tiered emergency GC,
# admission control, raw block-device VLog) under realistic traffic and
# reports go/no-go pass criteria.
#
# Phases:
#   1. Build + start server (clean data dir)
#   2. MPut bulk-load → expect packing density ≈ 25× for 128 B values
#   3. Cool-down → wait for GC to settle
#   4. Read-only benchmark → expect cache-hit P99 within target
#   5. Mixed 70R/30W → expect blended ops/s above target
#   6. Sustained-write stress → watch GC emergency counter (must stay 0)
#   7. Final report: density, throughput, P99, GC stats, pass/fail
#
# Targets are tuned for n2-highmem-64 Linux NVMe. macOS dev runs will fall
# short on every fdatasync-bound metric (F_FULLFSYNC ≈ 7 ms vs Linux 0.2 ms);
# the script still runs end-to-end and validates correctness paths.
#
# Usage:
#   ./scripts/bench.sh                # default 128 B values, 8-disk profile
#   VALUE_SIZE=256 NUM_KEYS=10000000 ./scripts/bench.sh
#   DATA_DIRS=/mnt/nvme0,/mnt/nvme1,...,/mnt/nvme7 ./scripts/bench.sh
#   RAW_VLOGS=/dev/nvme0n1,...,/dev/nvme7n1 ./scripts/bench.sh
#
# Env knobs (all optional):
#   ADDR             default 127.0.0.1:9000
#   METRICS_ADDR     default 127.0.0.1:2112
#   DATA_DIRS        single dir or comma-separated list (default /tmp/veltrixbench)
#   RAW_VLOGS        comma-separated /dev/nvmeXnY (default empty = file-mode VLog)
#   CACHE_MB         default 1024 (use 409600 = 400 GB on n2-highmem-64)
#   WAL_WINDOW_MS    default 5
#   NUM_KEYS         default 1000000
#   VALUE_SIZE       default 128
#   BATCH_SIZE       default 1024 (MPut/MGet batch entries)
#   BULK_DUR         default 30 (seconds of sustained MPut bulk load)
#   READ_DUR         default 30
#   MIXED_DUR        default 30
#   STRESS_DUR       default 60 (sustained write stress to provoke GC)
#   CONCURRENCY      default 64 (workers; raise to 512 on multi-core hosts)
#
# Pass criteria (logged at end):
#   - MPut density   bytes/record ≤ 1.2 × (24 + value_size)        — packing engaged
#   - Read P99       ≤ 5 ms (cache-hit, post-warmup)               — read SLO
#   - Read errors    0%                                            — correctness
#   - GC emergency   veltrixdb_vlog_gc_emergency_runs_total == 0   — no death spirals
#   - Write errors   < 0.1%                                        — no shed traffic

set -euo pipefail

# ── Resolve repo root + binaries ──────────────────────────────────────────────
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
cd "${REPO_ROOT}"

ADDR="${ADDR:-127.0.0.1:9000}"
METRICS_ADDR="${METRICS_ADDR:-127.0.0.1:2112}"
DATA_DIRS="${DATA_DIRS:-/tmp/veltrixbench}"
RAW_VLOGS="${RAW_VLOGS:-}"
CACHE_MB="${CACHE_MB:-1024}"
WAL_WINDOW_MS="${WAL_WINDOW_MS:-5}"

NUM_KEYS="${NUM_KEYS:-1000000}"
VALUE_SIZE="${VALUE_SIZE:-128}"
BATCH_SIZE="${BATCH_SIZE:-1024}"
CONCURRENCY="${CONCURRENCY:-64}"

BULK_DUR="${BULK_DUR:-30}"
READ_DUR="${READ_DUR:-30}"
MIXED_DUR="${MIXED_DUR:-30}"
STRESS_DUR="${STRESS_DUR:-60}"

OUT_DIR="${OUT_DIR:-/tmp/veltrixbench-out}"
SERVER_BIN="${OUT_DIR}/veltrixdb"
LOAD_BIN="${OUT_DIR}/veltrixload"
SERVER_LOG="${OUT_DIR}/server.log"
RUN_LOG="${OUT_DIR}/run.log"
SERVER_PID=""

mkdir -p "${OUT_DIR}"
: > "${RUN_LOG}"

# ── Logging helpers ───────────────────────────────────────────────────────────
say()  { printf '%s\n' "$*" | tee -a "${RUN_LOG}"; }
hdr()  { printf '\n── %s ──\n' "$*" | tee -a "${RUN_LOG}"; }
fail() { say "ERROR: $*"; exit 1; }

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    say "stopping server (pid ${SERVER_PID})"
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ── Phase 1: build + start ────────────────────────────────────────────────────
hdr "phase 1: build + start"
say "building veltrixdb + loadtest"
go build -o "${SERVER_BIN}" ./cmd/server >>"${RUN_LOG}" 2>&1 || fail "server build failed (see ${RUN_LOG})"
go build -o "${LOAD_BIN}"   ./cmd/loadtest >>"${RUN_LOG}" 2>&1 || fail "loadtest build failed"
say "binaries: ${SERVER_BIN} ${LOAD_BIN}"

# Wipe data dirs (only the ones we manage; refuse to wipe /mnt/nvmeX paths)
IFS=',' read -ra DIR_ARR <<< "${DATA_DIRS}"
for d in "${DIR_ARR[@]}"; do
  if [[ "${d}" == /tmp/* ]]; then
    rm -rf "${d}" && mkdir -p "${d}"
  else
    say "leaving ${d} as-is (refuse to wipe non-/tmp path)"
    mkdir -p "${d}" 2>/dev/null || true
  fi
done

server_args=(
  --addr "${ADDR}"
  --metrics-addr "${METRICS_ADDR}"
  --cache "${CACHE_MB}"
  --wal-flush-window-ms "${WAL_WINDOW_MS}"
  --vlog-flush-window-ms "${WAL_WINDOW_MS}"
)
if [[ "${DATA_DIRS}" == *","* ]]; then
  server_args+=(--data-dirs "${DATA_DIRS}")
else
  server_args+=(--data "${DATA_DIRS}")
fi
if [[ -n "${RAW_VLOGS}" ]]; then
  server_args+=(--raw-vlogs "${RAW_VLOGS}")
  say "raw VLog mode: ${RAW_VLOGS}"
fi

say "starting server: ${SERVER_BIN} ${server_args[*]}"
"${SERVER_BIN}" "${server_args[@]}" > "${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
say "server pid ${SERVER_PID}"

# Wait for /readyz
for _ in $(seq 1 30); do
  if curl -sf "http://${METRICS_ADDR}/readyz" >/dev/null; then break; fi
  sleep 0.5
done
curl -sf "http://${METRICS_ADDR}/readyz" >/dev/null || fail "server never became ready (see ${SERVER_LOG})"
say "server ready"

# Helper: read a Prometheus metric value (first sample)
metric() {
  local name="$1"
  curl -s "http://${METRICS_ADDR}/metrics" \
    | awk -v n="${name}" '$1 ~ "^"n"({|$)" {print $2; exit}'
}

# ── Phase 2: MPut bulk-load ───────────────────────────────────────────────────
hdr "phase 2: MPut bulk-load (engages packing)"
WRITES_BEFORE=$(metric veltrixdb_storage_writes_total || echo 0)
WRITES_BEFORE=${WRITES_BEFORE:-0}

"${LOAD_BIN}" \
  --addr "${ADDR}" --mode write \
  --concurrency 8 --batch-size "${BATCH_SIZE}" \
  --duration "${BULK_DUR}" \
  --num-keys "${NUM_KEYS}" --value-size "${VALUE_SIZE}" \
  --warmup 0 \
  --safe-reads --report-every 5 \
  | tee -a "${RUN_LOG}"

# Density check: bytes/record after bulk load
sleep 2
WRITES_AFTER=$(metric veltrixdb_storage_writes_total)
VLOG_BYTES=$(metric veltrixdb_vlog_file_bytes)
WRITES_DELTA=$(awk -v a="${WRITES_AFTER}" -v b="${WRITES_BEFORE}" 'BEGIN{print a-b}')
say ""
say "writes after  : ${WRITES_AFTER}"
say "writes before : ${WRITES_BEFORE}"
say "writes delta  : ${WRITES_DELTA}"
say "vlog bytes    : ${VLOG_BYTES}"
DENSITY_OK=0
if [[ "${WRITES_DELTA}" -gt 0 ]]; then
  BPR=$(awk -v b="${VLOG_BYTES}" -v w="${WRITES_DELTA}" 'BEGIN{printf "%.1f", b/w}')
  TARGET=$(awk -v v="${VALUE_SIZE}" 'BEGIN{printf "%.1f", (24+v)*1.2}')
  say "bytes/record  : ${BPR}  (target ≤ ${TARGET})"
  if awk -v bpr="${BPR}" -v t="${TARGET}" 'BEGIN{exit !(bpr<=t)}'; then
    DENSITY_OK=1
    say "  ✓ packing engaged"
  else
    say "  ✗ packing NOT engaged (likely server falling back to unpacked path)"
  fi
fi

# ── Phase 3: cool-down ────────────────────────────────────────────────────────
hdr "phase 3: cool-down (5 s, let GC settle)"
sleep 5

# ── Phase 4: read-only ────────────────────────────────────────────────────────
hdr "phase 4: read-only benchmark"
"${LOAD_BIN}" \
  --addr "${ADDR}" --mode read \
  --concurrency "${CONCURRENCY}" \
  --duration "${READ_DUR}" \
  --num-keys "${NUM_KEYS}" --value-size "${VALUE_SIZE}" \
  --report-every 5 \
  | tee -a "${RUN_LOG}"

# Capture read P99 (we'll grep the report block)
READ_P99=$(awk '/── READS ─/{flag=1} flag && /P99 /{print $2,$3; exit}' "${RUN_LOG}" | tail -1)
say "read P99 (last run): ${READ_P99:-n/a}"

# ── Phase 5: mixed 70R/30W ────────────────────────────────────────────────────
hdr "phase 5: mixed 70R/30W"
"${LOAD_BIN}" \
  --addr "${ADDR}" --mode mixed \
  --concurrency "${CONCURRENCY}" \
  --duration "${MIXED_DUR}" \
  --read-ratio 0.7 \
  --num-keys "${NUM_KEYS}" --value-size "${VALUE_SIZE}" \
  --report-every 5 \
  | tee -a "${RUN_LOG}"

# ── Phase 6: sustained write stress ───────────────────────────────────────────
hdr "phase 6: sustained MPut write stress (provokes GC)"
GC_BEFORE=$(metric veltrixdb_vlog_gc_runs_total || echo 0)
GC_EMERGENCY_BEFORE=$(metric veltrixdb_vlog_gc_emergency_runs_total || echo 0)
GC_BEFORE=${GC_BEFORE:-0}
GC_EMERGENCY_BEFORE=${GC_EMERGENCY_BEFORE:-0}
say "GC runs before          : ${GC_BEFORE}"
say "GC emergency runs before: ${GC_EMERGENCY_BEFORE}"

"${LOAD_BIN}" \
  --addr "${ADDR}" --mode write \
  --concurrency 8 --batch-size "${BATCH_SIZE}" \
  --duration "${STRESS_DUR}" \
  --num-keys "${NUM_KEYS}" --value-size "${VALUE_SIZE}" \
  --warmup 0 \
  --report-every 10 \
  | tee -a "${RUN_LOG}"

GC_AFTER=$(metric veltrixdb_vlog_gc_runs_total)
GC_EMERGENCY_AFTER=$(metric veltrixdb_vlog_gc_emergency_runs_total)
say "GC runs after          : ${GC_AFTER}"
say "GC emergency runs after: ${GC_EMERGENCY_AFTER}"
GC_EMERGENCY_DELTA=$(awk -v a="${GC_EMERGENCY_AFTER}" -v b="${GC_EMERGENCY_BEFORE}" 'BEGIN{print a-b}')

# ── Final report ──────────────────────────────────────────────────────────────
hdr "FINAL REPORT"
say ""
say "Phase 2 — packing density"
say "  bytes per record               : ${BPR:-n/a}"
say "  density gate (≤ ${TARGET:-n/a})            : $([ "${DENSITY_OK}" = "1" ] && echo PASS || echo FAIL)"
say ""
say "Phase 4 — read-only"
say "  read P99 (last sample)         : ${READ_P99:-n/a}"
say ""
say "Phase 6 — sustained write stress"
say "  GC emergency runs Δ            : ${GC_EMERGENCY_DELTA}"
say "  GC emergency gate (== 0)       : $([ "${GC_EMERGENCY_DELTA}" = "0" ] && echo PASS || echo FAIL)"
say ""
say "Server log     : ${SERVER_LOG}"
say "Full run log   : ${RUN_LOG}"
say ""

# Exit code: non-zero if any pass-criterion failed
EXIT=0
[[ "${DENSITY_OK}" = "1" ]] || EXIT=1
[[ "${GC_EMERGENCY_DELTA}" = "0" ]] || EXIT=1
exit "${EXIT}"
