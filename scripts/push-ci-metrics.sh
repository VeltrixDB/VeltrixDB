#!/usr/bin/env bash
# push-ci-metrics.sh — push CI job duration + status to Prometheus Pushgateway.
#
# Called from each CI job after all steps finish (always runs).
# If PUSH_GW_URL is empty the script exits 0 silently so it never breaks CI.
#
# Usage:
#   bash scripts/push-ci-metrics.sh \
#     --job       "node-1-storage" \
#     --start     "1716480000"      \   # unix seconds from $(date +%s)
#     --status    "success"         \   # success | failure | cancelled
#     --run-id    "$GITHUB_RUN_ID"  \
#     --sha       "$GITHUB_SHA"     \
#     --branch    "$GITHUB_REF_NAME"
#
# Pushgateway grouping key:
#   /metrics/job/veltrixdb_ci/job_name/<job>/run_id/<run_id>
#
# Metrics pushed:
#   veltrixdb_ci_job_duration_seconds  gauge  — wall-clock seconds for the job
#   veltrixdb_ci_job_status            gauge  — 0=success 1=failure 2=cancelled

set -euo pipefail

JOB_NAME=""
START_TS=""
STATUS="success"
RUN_ID="${GITHUB_RUN_ID:-local}"
SHA="${GITHUB_SHA:-unknown}"
BRANCH="${GITHUB_REF_NAME:-unknown}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job)     JOB_NAME="$2";  shift 2 ;;
    --start)   START_TS="$2";  shift 2 ;;
    --status)  STATUS="$2";    shift 2 ;;
    --run-id)  RUN_ID="$2";    shift 2 ;;
    --sha)     SHA="$2";       shift 2 ;;
    --branch)  BRANCH="$2";    shift 2 ;;
    *) echo "Unknown arg: $1" >&2; shift ;;
  esac
done

# Nothing to do if Pushgateway URL is not configured.
if [[ -z "${PUSH_GW_URL:-}" ]]; then
  echo "[push-ci-metrics] PUSH_GW_URL not set — skipping metric push."
  exit 0
fi

if [[ -z "$JOB_NAME" || -z "$START_TS" ]]; then
  echo "[push-ci-metrics] --job and --start are required." >&2
  exit 1
fi

NOW_TS=$(date +%s)
DURATION=$(( NOW_TS - START_TS ))

case "$STATUS" in
  success)   STATUS_CODE=0 ;;
  failure)   STATUS_CODE=1 ;;
  cancelled) STATUS_CODE=2 ;;
  *)         STATUS_CODE=1 ;;
esac

# Sanitise job name for use as a URL path segment (replace spaces and slashes).
JOB_SLUG="${JOB_NAME// /_}"
JOB_SLUG="${JOB_SLUG//\//_}"

# Short SHA for labels (first 8 chars).
SHORT_SHA="${SHA:0:8}"

PUSH_URL="${PUSH_GW_URL}/metrics/job/veltrixdb_ci/job_name/${JOB_SLUG}/run_id/${RUN_ID}"

PAYLOAD=$(cat <<PROM
# HELP veltrixdb_ci_job_duration_seconds Wall-clock seconds the CI job ran from first step to last.
# TYPE veltrixdb_ci_job_duration_seconds gauge
veltrixdb_ci_job_duration_seconds{branch="${BRANCH}",sha="${SHORT_SHA}",status="${STATUS}"} ${DURATION}
# HELP veltrixdb_ci_job_status CI job result: 0=success 1=failure 2=cancelled.
# TYPE veltrixdb_ci_job_status gauge
veltrixdb_ci_job_status{branch="${BRANCH}",sha="${SHORT_SHA}"} ${STATUS_CODE}
PROM
)

echo "[push-ci-metrics] job=${JOB_NAME} duration=${DURATION}s status=${STATUS}(${STATUS_CODE})"
echo "[push-ci-metrics] pushing to ${PUSH_URL}"

HTTP_CODE=$(echo "$PAYLOAD" | curl -sf -o /dev/null -w "%{http_code}" \
  --data-binary @- \
  "$PUSH_URL" || echo "000")

if [[ "$HTTP_CODE" =~ ^2 ]]; then
  echo "[push-ci-metrics] pushed OK (HTTP ${HTTP_CODE})"
else
  echo "[push-ci-metrics] WARNING: push returned HTTP ${HTTP_CODE} — non-fatal, continuing." >&2
fi
