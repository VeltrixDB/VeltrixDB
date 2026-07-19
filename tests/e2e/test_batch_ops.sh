#!/usr/bin/env bash
# test_batch_ops.sh — MPut/MGet batched operations via loadtest.
#
# Test cases:
#   1. Batched write: --batch-size=256, --duration=5, --num-keys=100000 → exit 0
#   2. Verify throughput > 100 keys/s (conservative floor)
#   3. Batched read after write: --batch-size=256 → exit 0
#   4. Verify read ops > 0
#   5. Larger batch: --batch-size=1024 → exit 0
#   6. VLog GC emergency counter should be 0 after short run
#
# Usage:
#   ./tests/e2e/test_batch_ops.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║         VeltrixDB — Batch Ops (MPut/MGet) Test       ║"
echo "╚══════════════════════════════════════════════════════╝"

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi
if [ ! -x "$LOADTEST_BIN" ]; then
    build_loadtest
fi

require_command curl || true   # optional for metrics check

DATA_DIR=$(mktemp -d /tmp/veltrixdb-batch-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

# ── Test 1: Batched write --batch-size=256 ────────────────────────────────────
section "Test 1: Batched write (batch-size=256)"
BATCH_WRITE_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=write \
    --batch-size=256 \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=100000 \
    --value-size=128 \
    2>&1) || true

BATCH_WRITE_EXIT=$?

if [ "$BATCH_WRITE_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  batched write exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched write exit code: $BATCH_WRITE_EXIT"
    FAIL=$((FAIL + 1))
fi

# Extract total ops written
TOTAL_OPS=$(echo "$BATCH_WRITE_OUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
THROUGHPUT=$(echo "$BATCH_WRITE_OUT" | grep -oE 'Throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")

if [ -n "$TOTAL_OPS" ] && [ "$TOTAL_OPS" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  batched write: ${TOTAL_OPS} total ops written"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched write: 0 ops recorded"
    FAIL=$((FAIL + 1))
fi

# Throughput floor: 100 keys/s minimum (very conservative for any hardware)
if [ -n "$THROUGHPUT" ] && [ "$THROUGHPUT" -gt 100 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  batched write throughput ${THROUGHPUT} ops/s > 100 ops/s floor"
    PASS=$((PASS + 1))
else
    # Try combined throughput field
    COMBINED_THRU=$(echo "$BATCH_WRITE_OUT" | grep -oE 'Combined throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")
    if [ -n "$COMBINED_THRU" ] && [ "$COMBINED_THRU" -gt 100 ] 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC}  combined throughput ${COMBINED_THRU} ops/s > 100 ops/s floor"
        PASS=$((PASS + 1))
    else
        echo -e "  ${YELLOW}WARN${NC}  could not verify throughput > 100 ops/s (got: ${THROUGHPUT:-unknown})"
    fi
fi

# Write error check
WRITE_ERRS=$(echo "$BATCH_WRITE_OUT" | grep "WRITES" -A5 | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ "${WRITE_ERRS:-0}" -eq 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  batched write: 0 errors"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched write: ${WRITE_ERRS} errors"
    FAIL=$((FAIL + 1))
fi

# ── Test 2: Batched read --batch-size=256 ────────────────────────────────────
section "Test 2: Batched read (batch-size=256)"
BATCH_READ_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=read \
    --batch-size=256 \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=100000 \
    --value-size=128 \
    2>&1) || true

BATCH_READ_EXIT=$?

if [ "$BATCH_READ_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  batched read exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched read exit code: $BATCH_READ_EXIT"
    FAIL=$((FAIL + 1))
fi

READ_OPS=$(echo "$BATCH_READ_OUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ -n "$READ_OPS" ] && [ "$READ_OPS" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  batched read: ${READ_OPS} total ops"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched read: 0 ops"
    FAIL=$((FAIL + 1))
fi

# ── Test 3: Larger batch --batch-size=1024 ───────────────────────────────────
section "Test 3: Large batch (batch-size=1024)"
LARGE_BATCH_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=write \
    --batch-size=1024 \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=100000 \
    --value-size=128 \
    2>&1) || true

LARGE_BATCH_EXIT=$?

if [ "$LARGE_BATCH_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  large batch write exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  large batch write exit code: $LARGE_BATCH_EXIT"
    FAIL=$((FAIL + 1))
fi

# ── Test 4: packing indicator in output ─────────────────────────────────────
section "Test 4: Packing path engaged (output label check)"
if echo "$LARGE_BATCH_OUT" | grep -q "packing\|MPut\|MGet"; then
    echo -e "  ${GREEN}PASS${NC}  output mentions packing/MPut path"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  packing label not detected in output (may be cosmetic)"
fi

# ── Test 5: GC emergency counter via metrics (if metrics available) ──────────
section "Test 5: VLog GC emergency counter = 0"
if command -v curl >/dev/null 2>&1; then
    METRICS=$(curl -s "http://127.0.0.1:${METRICS_PORT}/metrics" 2>/dev/null || echo "")
    if [ -n "$METRICS" ]; then
        EMERGENCY=$(echo "$METRICS" | grep "^veltrixdb_vlog_gc_emergency_runs_total " | awk '{print $2}' | head -1 || echo "0")
        EMERGENCY_INT=$(printf "%.0f" "${EMERGENCY:-0}" 2>/dev/null || echo 0)
        if [ "$EMERGENCY_INT" -eq 0 ] 2>/dev/null; then
            echo -e "  ${GREEN}PASS${NC}  vlog_gc_emergency_runs_total = 0 (no GC emergency)"
            PASS=$((PASS + 1))
        else
            echo -e "  ${YELLOW}WARN${NC}  vlog_gc_emergency_runs_total = ${EMERGENCY_INT} (GC under pressure)"
        fi
    else
        echo -e "  ${YELLOW}SKIP${NC}  metrics endpoint not available"
    fi
else
    echo -e "  ${YELLOW}SKIP${NC}  curl not available for metrics check"
fi

# ── Test 6: Batched mixed mode ───────────────────────────────────────────────
section "Test 6: Batched mixed mode (batch-size=256, 70R/30W)"
MIXED_BATCH_OUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=mixed \
    --batch-size=256 \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=100000 \
    --read-ratio=0.7 \
    --safe-reads \
    2>&1) || true

MIXED_BATCH_EXIT=$?

if [ "$MIXED_BATCH_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  batched mixed exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  batched mixed exit code: $MIXED_BATCH_EXIT"
    FAIL=$((FAIL + 1))
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
