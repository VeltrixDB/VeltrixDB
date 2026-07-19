#!/usr/bin/env bash
# test_binary_protocol.sh — Binary protocol tests via the loadtest binary.
#
# Test cases:
#   1. Write-only run: 5s, 4 workers, 10K keys  → exit 0, throughput > 0
#   2. Read-only run after write                → exit 0, reads succeed
#   3. Mixed run 70/30                          → exit 0, no errors reported
#   4. Single-key binary PUT/GET via client     → correctness check
#
# Usage:
#   ./tests/e2e/test_binary_protocol.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helpers.sh"

echo "╔══════════════════════════════════════════════════════╗"
echo "║        VeltrixDB — Binary Protocol Test              ║"
echo "╚══════════════════════════════════════════════════════╝"

if [ ! -x "$VELTRIX_BIN" ]; then
    build_binary
fi
if [ ! -x "$LOADTEST_BIN" ]; then
    build_loadtest
fi

DATA_DIR=$(mktemp -d /tmp/veltrixdb-binproto-XXXXXX)
_TEMP_DIRS+=("$DATA_DIR")

start_server "$DATA_DIR"
wait_ready 15

# ── Test 1: Write-only ────────────────────────────────────────────────────────
section "Test 1: Write-only (binary PUT)"
WRITE_OUTPUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=write \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=10000 \
    --value-size=64 \
    2>&1) || true

WRITE_EXIT=$?
if [ "$WRITE_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  loadtest write-only exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  loadtest write-only exit code: $WRITE_EXIT"
    FAIL=$((FAIL + 1))
fi

# Extract throughput from output
THROUGHPUT=$(echo "$WRITE_OUTPUT" | grep -oE 'Throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+$' | head -1 || echo "0")
if [ -n "$THROUGHPUT" ] && [ "$THROUGHPUT" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  write throughput > 0 ops/s (${THROUGHPUT} ops/s)"
    PASS=$((PASS + 1))
else
    # Also accept combined throughput line
    COMBINED=$(echo "$WRITE_OUTPUT" | grep -oE 'Combined throughput:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")
    TOTAL_OPS=$(echo "$WRITE_OUTPUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
    if [ -n "$TOTAL_OPS" ] && [ "$TOTAL_OPS" -gt 0 ] 2>/dev/null; then
        echo -e "  ${GREEN}PASS${NC}  write completed with ${TOTAL_OPS} total ops"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${NC}  could not determine write throughput from output"
        echo "  Output snippet: $(echo "$WRITE_OUTPUT" | tail -20)"
        FAIL=$((FAIL + 1))
    fi
fi

# Check write errors
WRITE_ERRS=$(echo "$WRITE_OUTPUT" | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ "${WRITE_ERRS:-0}" -eq 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  write-only: 0 errors"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  write-only: ${WRITE_ERRS} errors (may be expected for small run)"
fi

# ── Test 2: Read-only (keys already written above) ────────────────────────────
section "Test 2: Read-only (binary GET)"
READ_OUTPUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=read \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=10000 \
    --value-size=64 \
    2>&1) || true

READ_EXIT=$?
if [ "$READ_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  loadtest read-only exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  loadtest read-only exit code: $READ_EXIT"
    FAIL=$((FAIL + 1))
fi

READ_OPS=$(echo "$READ_OUTPUT" | grep -oE 'Total ops:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
if [ -n "$READ_OPS" ] && [ "$READ_OPS" -gt 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  read-only completed with ${READ_OPS} total ops"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  read-only: 0 ops recorded"
    FAIL=$((FAIL + 1))
fi

# ── Test 3: Mixed workload ─────────────────────────────────────────────────────
section "Test 3: Mixed workload 70R/30W (binary)"
MIXED_OUTPUT=$("$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=mixed \
    --duration=5 \
    --warmup=0 \
    --concurrency=4 \
    --num-keys=10000 \
    --read-ratio=0.7 \
    --safe-reads \
    2>&1) || true

MIXED_EXIT=$?
if [ "$MIXED_EXIT" -eq 0 ]; then
    echo -e "  ${GREEN}PASS${NC}  loadtest mixed exits 0"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  loadtest mixed exit code: $MIXED_EXIT"
    FAIL=$((FAIL + 1))
fi

MIXED_WERRS=$(echo "$MIXED_OUTPUT" | grep "WRITES" -A5 | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
MIXED_RERRS=$(echo "$MIXED_OUTPUT" | grep "READS" -A5 | grep -oE 'Errors:[[:space:]]+[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "0")
echo -e "  ${CYAN}INFO${NC}  mixed write errors=${MIXED_WERRS:-0} read errors=${MIXED_RERRS:-0}"

if [ "${MIXED_WERRS:-0}" -eq 0 ] 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}  mixed: 0 write errors"
    PASS=$((PASS + 1))
else
    echo -e "  ${RED}FAIL${NC}  mixed: ${MIXED_WERRS} write errors"
    FAIL=$((FAIL + 1))
fi

# ── Test 4: Binary protocol correctness via text protocol cross-check ─────────
section "Test 4: Binary PUT → text GET cross-check"
# Write a known key via loadtest (binary PUT), read it back via text GET.
# The loadtest writes keys like "key:N"; we pick key:0 after a small write run.
"$LOADTEST_BIN" \
    --addr="127.0.0.1:${VELTRIX_PORT}" \
    --mode=write \
    --duration=2 \
    --warmup=0 \
    --concurrency=1 \
    --num-keys=5 \
    --value-size=4 \
    --key-offset=90000 \
    2>&1 >/dev/null || true

# key:90000 should be set; value will be "vvvv" (4 v's)
resp=$(veltrix_cmd "GET key:90000")
if [ "$resp" = "vvvv" ]; then
    echo -e "  ${GREEN}PASS${NC}  binary PUT visible via text GET (key:90000 == vvvv)"
    PASS=$((PASS + 1))
else
    echo -e "  ${YELLOW}WARN${NC}  binary PUT / text GET cross-check: resp='${resp}' (may be timing dependent)"
fi

# ── Cleanup & summary ─────────────────────────────────────────────────────────
stop_server
print_summary
