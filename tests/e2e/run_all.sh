#!/usr/bin/env bash
# run_all.sh — Master test runner: builds all binaries, runs every e2e test
# script sequentially, collects pass/fail counts, prints summary.
#
# Usage:
#   ./tests/e2e/run_all.sh                     # run all tests
#   ./tests/e2e/run_all.sh basic metrics       # run specific suites
#   SKIP_STRESS=1 ./tests/e2e/run_all.sh       # skip the 30s stress test
#   VELTRIX_PORT=9001 ./tests/e2e/run_all.sh   # use a custom port
#
# Environment:
#   VELTRIX_BIN   — path to server binary (built here if missing)
#   LOADTEST_BIN  — path to loadtest binary (built here if missing)
#   VELTRIX_PORT  — base TCP port (default 9000)
#   METRICS_PORT  — base metrics port (default 2112)
#   SKIP_STRESS   — if set to 1, skip test_stress.sh (saves ~90s)
#   SKIP_TTL      — if set to 1, skip test_ttl.sh (saves ~10s for sleeps)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# ── Env ───────────────────────────────────────────────────────────────────────
export VELTRIX_BIN="${VELTRIX_BIN:-/tmp/veltrixdb}"
export LOADTEST_BIN="${LOADTEST_BIN:-/tmp/veltrix-loadtest}"
export VELTRIX_PORT="${VELTRIX_PORT:-9000}"
export METRICS_PORT="${METRICS_PORT:-2112}"
export REPO_ROOT

SKIP_STRESS="${SKIP_STRESS:-0}"
SKIP_TTL="${SKIP_TTL:-0}"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR="${TMPDIR:-/tmp}/veltrixdb-e2e-logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
SUITE_LOG="$LOG_DIR/run_all_${TIMESTAMP}.log"

echo "╔══════════════════════════════════════════════════════╗"
echo "║        VeltrixDB — E2E Test Suite Runner             ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo -e "  ${CYAN}Repo${NC}:        $REPO_ROOT"
echo -e "  ${CYAN}Server${NC}:      $VELTRIX_BIN"
echo -e "  ${CYAN}Loadtest${NC}:    $LOADTEST_BIN"
echo -e "  ${CYAN}Port${NC}:        $VELTRIX_PORT"
echo -e "  ${CYAN}Metrics${NC}:     $METRICS_PORT"
echo -e "  ${CYAN}Log${NC}:         $SUITE_LOG"
echo ""

# ── Dependency checks ─────────────────────────────────────────────────────────
echo -e "${CYAN}▶ Checking dependencies${NC}"
MISSING_DEPS=0
for cmd in nc curl kill; do
    if command -v "$cmd" >/dev/null 2>&1; then
        echo -e "  ${GREEN}OK${NC}    $cmd found at $(command -v "$cmd")"
    else
        echo -e "  ${YELLOW}WARN${NC}  $cmd not found (some tests may be skipped)"
        MISSING_DEPS=$((MISSING_DEPS + 1))
    fi
done
echo ""

# ── Build binaries ────────────────────────────────────────────────────────────
echo -e "${CYAN}▶ Building binaries${NC}"

if [ ! -x "$VELTRIX_BIN" ] || [ "$REPO_ROOT/cmd/server/main.go" -nt "$VELTRIX_BIN" ] 2>/dev/null; then
    echo -e "  Building server..."
    (cd "$REPO_ROOT" && go build -o "$VELTRIX_BIN" ./cmd/server) 2>&1 | tee -a "$SUITE_LOG" || {
        echo -e "  ${RED}ERROR${NC}  server build failed"
        exit 1
    }
    echo -e "  ${GREEN}OK${NC}    server: $VELTRIX_BIN"
else
    echo -e "  ${GREEN}OK${NC}    server binary up-to-date: $VELTRIX_BIN"
fi

if [ ! -x "$LOADTEST_BIN" ] || [ "$REPO_ROOT/cmd/loadtest/main.go" -nt "$LOADTEST_BIN" ] 2>/dev/null; then
    echo -e "  Building loadtest..."
    (cd "$REPO_ROOT" && go build -o "$LOADTEST_BIN" ./cmd/loadtest) 2>&1 | tee -a "$SUITE_LOG" || {
        echo -e "  ${RED}ERROR${NC}  loadtest build failed"
        exit 1
    }
    echo -e "  ${GREEN}OK${NC}    loadtest: $LOADTEST_BIN"
else
    echo -e "  ${GREEN}OK${NC}    loadtest binary up-to-date: $LOADTEST_BIN"
fi
echo ""

# ── Test registry ─────────────────────────────────────────────────────────────
# Each entry: "script_name:description"
ALL_TESTS=(
    "test_basic_ops.sh:Basic PUT/GET/DEL/PING/INFO operations"
    "test_persistence.sh:WAL replay and crash recovery"
    "test_binary_protocol.sh:Binary protocol via loadtest"
    "test_metrics.sh:Prometheus metrics and health endpoints"
    "test_namespaces.sh:Namespace operations (NSPUT/NSGET/NSDEL/NSSCAN/NSLIST)"
    "test_auth.sh:Authentication and RBAC"
    "test_batch_ops.sh:Batched MPut/MGet operations"
    "test_backup_restore.sh:Backup and restore via checkpoint"
    "test_node_failover.sh:Node crash/restart and data durability"
    "test_node_addition.sh:Node addition and data sharing"
    "test_ttl.sh:Key expiry via TTL"
    "test_stress.sh:High-concurrency stress test"
)

# Parse optional positional args to filter suites
FILTER_ARGS=("$@")

# ── Run tests ─────────────────────────────────────────────────────────────────
echo -e "${CYAN}▶ Running tests${NC}"
echo ""

SUITE_PASS=0
SUITE_FAIL=0
SUITE_SKIP=0
FAILED_SUITES=()
START_TIME=$(date +%s)

for entry in "${ALL_TESTS[@]}"; do
    script="${entry%%:*}"
    description="${entry#*:}"
    script_path="$SCRIPT_DIR/$script"

    # Apply CLI filter if arguments given
    if [ "${#FILTER_ARGS[@]}" -gt 0 ]; then
        matched=0
        for arg in "${FILTER_ARGS[@]}"; do
            if echo "$script" | grep -qi "$arg"; then
                matched=1
                break
            fi
        done
        if [ "$matched" -eq 0 ]; then
            continue
        fi
    fi

    # Apply env-based skips
    if [ "$SKIP_STRESS" = "1" ] && [ "$script" = "test_stress.sh" ]; then
        echo -e "  ${YELLOW}SKIP${NC}  $script — SKIP_STRESS=1"
        SUITE_SKIP=$((SUITE_SKIP + 1))
        continue
    fi
    if [ "$SKIP_TTL" = "1" ] && [ "$script" = "test_ttl.sh" ]; then
        echo -e "  ${YELLOW}SKIP${NC}  $script — SKIP_TTL=1"
        SUITE_SKIP=$((SUITE_SKIP + 1))
        continue
    fi

    if [ ! -f "$script_path" ]; then
        echo -e "  ${YELLOW}SKIP${NC}  $script — file not found"
        SUITE_SKIP=$((SUITE_SKIP + 1))
        continue
    fi

    echo -e "┌────────────────────────────────────────────────────"
    echo -e "│ ${CYAN}SUITE${NC}: $script"
    echo -e "│        $description"
    echo -e "└────────────────────────────────────────────────────"

    SUITE_START=$(date +%s)
    SUITE_LOG_FILE="$LOG_DIR/${script%%.sh}_${TIMESTAMP}.log"

    # Run the test script; capture output to log and tee to stdout
    if bash "$script_path" 2>&1 | tee "$SUITE_LOG_FILE"; then
        SUITE_END=$(date +%s)
        elapsed=$((SUITE_END - SUITE_START))
        echo ""
        echo -e "  ${GREEN}SUITE PASSED${NC}  $script  (${elapsed}s)"
        SUITE_PASS=$((SUITE_PASS + 1))
    else
        SUITE_END=$(date +%s)
        elapsed=$((SUITE_END - SUITE_START))
        echo ""
        echo -e "  ${RED}SUITE FAILED${NC}  $script  (${elapsed}s)"
        SUITE_FAIL=$((SUITE_FAIL + 1))
        FAILED_SUITES+=("$script")
    fi
    echo ""
done

# ── Summary ───────────────────────────────────────────────────────────────────
TOTAL_TIME=$(( $(date +%s) - START_TIME ))
TOTAL_SUITES=$((SUITE_PASS + SUITE_FAIL + SUITE_SKIP))

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                  Test Suite Summary                  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo -e "  Total suites:  $TOTAL_SUITES"
echo -e "  ${GREEN}Passed${NC}:        $SUITE_PASS"
echo -e "  ${RED}Failed${NC}:        $SUITE_FAIL"
echo -e "  ${YELLOW}Skipped${NC}:       $SUITE_SKIP"
echo -e "  Total time:    ${TOTAL_TIME}s"
echo ""

if [ "${#FAILED_SUITES[@]}" -gt 0 ]; then
    echo -e "  ${RED}Failed suites:${NC}"
    for s in "${FAILED_SUITES[@]}"; do
        echo -e "    ${RED}-${NC} $s  (log: $LOG_DIR/${s%%.sh}_${TIMESTAMP}.log)"
    done
    echo ""
fi

echo -e "  Full logs: $LOG_DIR/"
echo ""

if [ "$SUITE_FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}=== ALL SUITES PASSED ===${NC}"
    echo ""
    exit 0
else
    echo -e "  ${RED}=== $SUITE_FAIL SUITE(S) FAILED ===${NC}"
    echo ""
    exit 1
fi
