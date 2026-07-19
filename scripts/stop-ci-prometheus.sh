#!/usr/bin/env bash
# stop-ci-prometheus.sh — gracefully stop the CI Prometheus process.
# Called by the CI workflow in the always() cleanup step.

set -euo pipefail

PID_FILE="/tmp/veltrix-prometheus-ci.pid"
LOG_FILE="/tmp/veltrix-ci-prometheus/prometheus.log"

if [ ! -f "$PID_FILE" ]; then
  echo "No PID file found at $PID_FILE — nothing to stop."
  exit 0
fi

PID=$(cat "$PID_FILE")

if kill -0 "$PID" 2>/dev/null; then
  echo "Stopping prometheus PID ${PID} ..."
  # SIGTERM gives prometheus a chance to flush the WAL.
  kill -TERM "$PID"
  # Wait up to 10 s for clean exit.
  for i in $(seq 1 20); do
    if ! kill -0 "$PID" 2>/dev/null; then
      echo "Prometheus stopped cleanly."
      break
    fi
    sleep 0.5
  done
  # Force-kill if still running.
  if kill -0 "$PID" 2>/dev/null; then
    echo "Prometheus did not stop in 10 s — sending SIGKILL."
    kill -KILL "$PID" 2>/dev/null || true
  fi
else
  echo "Prometheus PID ${PID} already gone."
fi

rm -f "$PID_FILE"

# Print last 20 lines of prometheus log so failures are diagnosable.
if [ -f "$LOG_FILE" ]; then
  echo "--- last 20 lines of prometheus.log ---"
  tail -20 "$LOG_FILE" || true
  echo "---------------------------------------"
fi
