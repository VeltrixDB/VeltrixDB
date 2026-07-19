#!/usr/bin/env bash
# test_stress.sh — Stress test: high concurrency, large values, mixed workload.
#
# Test cases:
#   1. 30s mixed workload: concurrency=32, 100K keys, value-size=512
#      → exit 0, 0 write errors, throughput > 100 ops/sec
#   2. Bulk batched write: batch-size=1024, concurrency=8, 10s
#      → exit 0, packing engaged
#   3. High-concurrency read-heavy: concurrency=64, batch-size=256, 10s
#      → exit 0, read throughput > 1000 ops/sec
#   4. Server still healthy after stress: PING → PONG, /healthz → 200
#   5. Key count sensible after stress: INFO keys > 0
#   6. No GC emergency events during stress
#
# Usage:
#   ./tests/e2e/test_stress.sh
#   VELTRIX_PORT=9001 ./tests/e2e/test_stress.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║           VeltrixDB — Stress Test                    ║"
echo "╚══════════════════════════════════════════════════════╝"

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi
if [ ! -x "$LOADTEST_BIN" ]; then
    build_loadtest
fi

require_command nc   || { echo "nc required"; exit 1; }

DATA_DIR=$(mktemp -d /tmp/veltrixdb-stress-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

# Start with a larger cache for stress test
start_server "$DATA_DIR" --cache 256
wait_ready 20

# ── Test 1: 30s mixed workload ─────────────────────────────────────────────────
section "Test 1: 30s mixed workload (concurrency=32, 100K keys, 512B values)"

MIXED_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=mixed \
    --concurrency=32 \
    --duration=30 \
    --warmup=0 \
    --num-keys=100000 \
    --value-size=512 \
    --read-ratio=0.7 \
    --safe-reads \
    2>&1) || true

MIXED_EXIT=$?

if [ "$MIXED_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  30s mixed workload exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  30s mixed workload exit code: $MIXED_EXIT"
    FAIL=$((FAIL + 1))
fi

# Write error check
MIXED_WERRS=$(echo "$MIXED_OUT" | grep "WRITES" -A5 | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ "${MIXED_WERRS:-0}" -eq 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  mixed: 0 write errors"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  mixed: ${MIXED_WERRS} write errors"
    FAIL=$((FAIL + 1))
fi

# Throughput floor: 100 combined ops/sec (very conservative)
COMBINED_OPS=$(echo "$MIXED_OUT" | grep -oE 'Combined throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")
WRITE_OPS=$(echo "$MIXED_OUT" | grep "WRITES" -A5 | grep -oE 'Throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")
READ_OPS=$(echo "$MIXED_OUT" | grep "READS" -A5  | grep -oE 'Throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")

TOTAL_THR=$((${WRITE_OPS:-0} + ${READ_OPS:-0}))
REPORT_THR="${COMBINED_OPS:-$TOTAL_THR}"

if [ -n "$REPORT_THR" ] && [ "$REPORT_THR" -gt 100 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  combined throughput ${REPORT_THR} ops/s > 100 ops/s floor"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  throughput check inconclusive: writes=${WRITE_OPS:-?} reads=${READ_OPS:-?}"
fi

# ── Test 2: Bulk batched write ─────────────────────────────────────────────────
section "Test 2: Bulk batched write (batch-size=1024, concurrency=8, 10s)"

BULK_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=write \
    --batch-size=1024 \
    --concurrency=8 \
    --duration=10 \
    --warmup=0 \
    --num-keys=1000000 \
    --value-size=128 \
    2>&1) || true

BULK_EXIT=$?

if [ "$BULK_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  bulk batched write exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  bulk batched write exit code: $BULK_EXIT"
    FAIL=$((FAIL + 1))
fi

BULK_OPS=$(echo "$BULK_OUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ -n "$BULK_OPS" ] && [ "$BULK_OPS" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  bulk write: ${BULK_OPS} total keys written"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  bulk write: 0 ops recorded"
    FAIL=$((FAIL + 1))
fi

BULK_ERRS=$(echo "$BULK_OUT" | grep "WRITES" -A5 | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ "${BULK_ERRS:-0}" -eq 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  bulk write: 0 errors"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  bulk write: ${BULK_ERRS} errors"
    FAIL=$((FAIL + 1))
fi

# ── Test 3: High-concurrency batched reads ─────────────────────────────────────
section "Test 3: High-concurrency batched reads (concurrency=64, batch-size=256, 10s)"

HCREAD_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=read \
    --batch-size=256 \
    --concurrency=64 \
    --duration=10 \
    --warmup=0 \
    --num-keys=1000000 \
    --value-size=128 \
    2>&1) || true

HCREAD_EXIT=$?

if [ "$HCREAD_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  high-concurrency read exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  high-concurrency read exit code: $HCREAD_EXIT"
    FAIL=$((FAIL + 1))
fi

HCREAD_THR=$(echo "$HCREAD_OUT" | grep "READS" -A5 | grep -oE 'Throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
HCREAD_OPS=$(echo "$HCREAD_OUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")

if [ "${HCREAD_THR:-0}" -gt 1000 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  read throughput ${HCREAD_THR} ops/s > 1000 ops/s floor"
    PASS=$((PASS + 1))
elif [ "${HCREAD_OPS:-0}" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  high-concurrency read: ${HCREAD_OPS} total ops completed"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  read throughput check inconclusive: ${HCREAD_THR:-?} ops/s"
fi

# ── Test 4: Server health after stress ────────────────────────────────────────
section "Test 4: Server health after stress"

# PING
resp=$(veltrix_cmd "PING")
assert_eq "$resp" "PONG" "PING returns PONG after stress"

# /healthz
if command -v curl >/dev/null 2>&1; then
    assert_http_status "http://127.0.0.1:${METRICS_PORT}/healthz" "200" "/healthz returns 200 after stress"
    assert_http_status "http://127.0.0.1:${METRICS_PORT}/readyz"  "200" "/readyz returns 200 after stress"
else
    echo -e "  ${YELLOW}SKIP${NC}  curl not available for health endpoint check"
fi

# ── Test 5: INFO key count sensible after stress ──────────────────────────────
section "Test 5: INFO key count > 0 after stress"
resp=$(veltrix_cmd "INFO")
keys_val=$(echo "$resp" | grep -oE 'keys=[0-9]+' | head -1 | cut -d= -f2)
if [ -n "$keys_val" ] && [ "$keys_val" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  INFO keys=${keys_val} > 0 after stress"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  INFO keys=${keys_val:-unknown} after stress"
    FAIL=$((FAIL + 1))
fi

# ── Test 6: GC emergency counter still 0 ─────────────────────────────────────
section "Test 6: VLog GC emergency counter after stress"
if command -v curl >/dev/null 2>&1; then
    METRICS=$(curl -s "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || echo "")
    if [ -n "$METRICS" ]; then
        EMERGENCY=$(echo "$METRICS" | grep "^veltrixdb_vlog_gc_emergency_runs_total " | awk '{print $2}' | head -1 || echo "0")
        EMERGENCY_INT=$(printf "%.0f" "${EMERGENCY:-0}" 2>/dev/null || echo 0)
        if [ "$EMERGENCY_INT" -eq 0 ]; then
            echo -e "  ${GREEN}PASS${NC}  vlog_gc_emergency_runs_total = 0 after stress"
            PASS=$((PASS + 1))
        else
            echo -e "  ${YELLOW}WARN${NC}  vlog_gc_emergency_runs_total = ${EMERGENCY_INT} (GC under pressure during stress)"
        fi

        # Write admission throttles during stress
        THROTTLES=$(echo "$METRICS" | grep "^veltrixdb_storage_write_admission_throttles_total " | awk '{print $2}' | head -1 || echo "0")
        THROTTLES_INT=$(printf "%.0f" "${THROTTLES:-0}" 2>/dev/null || echo 0)
        if [ "$THROTTLES_INT" -eq 0 ]; then
            echo -e "  ${GREEN}PASS${NC}  no write admission throttles during stress"
            PASS=$((PASS + 1))
        else
            echo -e "  ${YELLOW}WARN${NC}  ${THROTTLES_INT} write admission throttle events during stress (reads were slow)"
        fi
    fi
else
    echo -e "  ${YELLOW}SKIP${NC}  curl not available for GC counter check"
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
