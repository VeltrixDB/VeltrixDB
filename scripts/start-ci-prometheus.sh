#!/usr/bin/env bash
# start-ci-prometheus.sh — download and start Prometheus for CI metrics collection.
#
# Scrapes all VeltrixDB test-server instances registered in VELTRIX_CI_SD_FILE
# via Prometheus file-based service discovery (refresh every 3 s).
#
# If GRAFANA_CLOUD_PROM_URL, GRAFANA_CLOUD_PROM_USER, and GRAFANA_CLOUD_API_KEY
# are all non-empty the scraped metrics are remote-written to Grafana Cloud in
# real time, labelled with ci_run, commit_sha, and branch so every CI run is
# a separate filterable slice in your dashboards.
#
# Usage (called by the CI workflow before running go test):
#   export VELTRIX_CI_SD_FILE=/tmp/veltrix-ci-sd.json
#   export GRAFANA_CLOUD_PROM_URL=https://prometheus-prod-XX-prod-XX.grafana.net
#   export GRAFANA_CLOUD_PROM_USER=123456
#   export GRAFANA_CLOUD_API_KEY=glc_xxxxx
#   bash scripts/start-ci-prometheus.sh

set -euo pipefail

PROM_VERSION="2.51.2"
PROM_DIR="/tmp/veltrix-ci-prometheus"
PROM_BIN="$PROM_DIR/prometheus"
PROM_DATA="$PROM_DIR/data"
PROM_CFG="$PROM_DIR/prometheus.yml"
PROM_PID_FILE="/tmp/veltrix-prometheus-ci.pid"
PROM_PORT="${PROM_PORT:-9091}"
SD_FILE="${VELTRIX_CI_SD_FILE:-/tmp/veltrix-ci-sd.json}"

# Derive CI labels from GitHub Actions env (fallback to git for local runs).
CI_RUN="${GITHUB_RUN_ID:-local}"
COMMIT_SHA="${GITHUB_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
BRANCH="${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)}"
WORKFLOW="veltrixdb-ci"

mkdir -p "$PROM_DIR" "$PROM_DATA"

# Initialise an empty service discovery file — the Go test writes targets here.
echo '[]' > "$SD_FILE"
echo "SD file initialised: $SD_FILE"

# ── Download prometheus binary (cached between CI runs via actions/cache) ─────
if [ ! -f "$PROM_BIN" ]; then
  ARCH="linux-amd64"
  TARBALL="prometheus-${PROM_VERSION}.${ARCH}.tar.gz"
  URL="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${TARBALL}"
  echo "Downloading prometheus ${PROM_VERSION} from ${URL} ..."
  curl -fsSL "$URL" | tar -xz -C "$PROM_DIR" --strip-components=1
  echo "prometheus binary ready: $PROM_BIN"
else
  echo "prometheus binary already present: $PROM_BIN"
fi

# ── Generate prometheus.yml ───────────────────────────────────────────────────
cat > "$PROM_CFG" << PROMCFG
global:
  scrape_interval:     5s
  evaluation_interval: 5s
  external_labels:
    ci_run:     "${CI_RUN}"
    commit_sha: "${COMMIT_SHA}"
    branch:     "${BRANCH}"
    workflow:   "${WORKFLOW}"

scrape_configs:
  - job_name: veltrixdb_ci
    file_sd_configs:
      - files:
          - "${SD_FILE}"
        refresh_interval: 3s
    relabel_configs:
      # Keep the node and scenario labels set by the Go test.
      - source_labels: [node]
        target_label: node
      - source_labels: [scenario]
        target_label: scenario

  # Scrape CI job duration metrics pushed by each GitHub Actions job.
  # Pushgateway exposes them as a regular /metrics endpoint.
  - job_name: veltrixdb_ci_pushgateway
    honor_labels: true          # preserve job_name/run_id labels from the push
    static_configs:
      - targets: ["${PUSHGW_HOST:-localhost}:${PUSHGW_PORT:-9091}"]
PROMCFG

# ── Optional remote_write to Grafana Cloud ────────────────────────────────────
if [ -n "${GRAFANA_CLOUD_PROM_URL:-}" ] && \
   [ -n "${GRAFANA_CLOUD_PROM_USER:-}" ] && \
   [ -n "${GRAFANA_CLOUD_API_KEY:-}" ]; then
  echo "Grafana Cloud remote_write: ${GRAFANA_CLOUD_PROM_URL}"
  cat >> "$PROM_CFG" << RWCFG

remote_write:
  - url: "${GRAFANA_CLOUD_PROM_URL}/api/prom/push"
    basic_auth:
      username: "${GRAFANA_CLOUD_PROM_USER}"
      password: "${GRAFANA_CLOUD_API_KEY}"
    queue_config:
      max_samples_per_send: 2000
      batch_send_deadline:  5s
      max_shards:           4
RWCFG
else
  echo "GRAFANA_CLOUD_PROM_URL not set — metrics stay local (artifact upload only)."
fi

echo "--- prometheus.yml ---"
cat "$PROM_CFG"
echo "----------------------"

# ── Start prometheus in the background ───────────────────────────────────────
"$PROM_BIN" \
  --config.file="$PROM_CFG" \
  --storage.tsdb.path="$PROM_DATA" \
  --storage.tsdb.retention.time=2h \
  --web.listen-address="127.0.0.1:${PROM_PORT}" \
  --log.level=warn \
  >> "$PROM_DIR/prometheus.log" 2>&1 &

PROM_PID=$!
echo "$PROM_PID" > "$PROM_PID_FILE"
echo "Prometheus started: PID=${PROM_PID}  port=${PROM_PORT}  data=${PROM_DATA}"

# Wait for the HTTP API to come up (up to 10 s).
for i in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:${PROM_PORT}/-/healthy" > /dev/null 2>&1; then
    echo "Prometheus ready."
    exit 0
  fi
  sleep 0.5
done
echo "WARNING: Prometheus did not become healthy within 10 s. Continuing anyway."
