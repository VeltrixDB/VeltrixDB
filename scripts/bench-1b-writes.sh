#!/usr/bin/env bash
# bench-1b-writes.sh — 1-billion write benchmark across 10 Kubernetes pods.
#
# Uses the main VeltrixDB image which includes /veltrixdb-loadtest.
#
# Usage:
#   VELTRIX_IMAGE=ghcr.io/veltrixdb/veltrixdb:1.0.0 \
#   VELTRIX_ADDR=veltrixdb.veltrixdb.svc.cluster.local:9000 \
#   NAMESPACE=veltrixdb \
#   ./scripts/bench-1b-writes.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VELTRIX_IMAGE="${VELTRIX_IMAGE:?VELTRIX_IMAGE must be set, e.g. ghcr.io/veltrixdb/veltrixdb:1.0.0}"
VELTRIX_ADDR="${VELTRIX_ADDR:-veltrixdb.veltrixdb.svc.cluster.local:9000}"
NAMESPACE="${NAMESPACE:-veltrixdb}"
JOB_NAME="veltrixdb-bench-1b"

log() { echo "[bench] $*"; }

# ── 1. Delete any previous run ────────────────────────────────────────────────
if kubectl get job "$JOB_NAME" -n "$NAMESPACE" &>/dev/null; then
  log "Deleting previous job $JOB_NAME ..."
  kubectl delete job "$JOB_NAME" -n "$NAMESPACE" --ignore-not-found
  kubectl wait --for=delete pod \
    -l "app.kubernetes.io/name=veltrixdb-bench" \
    -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
fi

# ── 2. Deploy job ─────────────────────────────────────────────────────────────
log "Deploying benchmark job (10 pods × 4096 concurrency × 3600s) ..."
log "Image : $VELTRIX_IMAGE"
log "Target: $VELTRIX_ADDR"

VELTRIX_IMAGE="$VELTRIX_IMAGE" VELTRIX_ADDR="$VELTRIX_ADDR" \
  envsubst < "$REPO_ROOT/scripts/bench-job.yaml" \
  | kubectl apply -n "$NAMESPACE" -f -

# ── 3. Wait for pods to start ─────────────────────────────────────────────────
log "Waiting for pods to start ..."
sleep 10
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=veltrixdb-bench"

# ── 4. Stream live stats ──────────────────────────────────────────────────────
log "Streaming live stats (report-every=60s). Ctrl-C to detach — job keeps running."
echo "──────────────────────────────────────────────────────────────────────────────"
kubectl logs -n "$NAMESPACE" \
  -l "app.kubernetes.io/name=veltrixdb-bench" \
  --prefix --follow --max-log-requests 10 2>/dev/null || true

# ── 5. Wait for completion ────────────────────────────────────────────────────
log "Waiting for job to complete (timeout 75 min) ..."
kubectl wait job/"$JOB_NAME" -n "$NAMESPACE" \
  --for=condition=Complete --timeout=4500s

# ── 6. Aggregate results ──────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════════════════════"
echo " BENCHMARK COMPLETE"
echo "══════════════════════════════════════════════════════════════════════════════"
TOTAL_WRITES=0
TOTAL_ERRORS=0
for pod in $(kubectl get pods -n "$NAMESPACE" \
    -l "app.kubernetes.io/name=veltrixdb-bench" \
    -o jsonpath='{.items[*].metadata.name}'); do
  echo ""
  echo "── $pod ──"
  kubectl logs -n "$NAMESPACE" "$pod" | tail -20
  WRITES=$(kubectl logs -n "$NAMESPACE" "$pod" | grep -oP 'writes=\K[0-9]+' | tail -1 || echo 0)
  ERRORS=$(kubectl logs -n "$NAMESPACE" "$pod" | grep -oP 'write_errors=\K[0-9]+' | tail -1 || echo 0)
  TOTAL_WRITES=$((TOTAL_WRITES + WRITES))
  TOTAL_ERRORS=$((TOTAL_ERRORS + ERRORS))
done
echo ""
echo "Total writes : $TOTAL_WRITES"
echo "Total errors : $TOTAL_ERRORS"
log "Done. Job auto-deletes in 1 hour (ttlSecondsAfterFinished=3600)."
