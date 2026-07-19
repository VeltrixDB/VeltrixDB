#!/usr/bin/env bash
# airgap-bundle.sh — produce a self-contained tarball for offline / air-gapped
# installation of VeltrixDB.
#
# What's in the bundle:
#   - veltrixdb-server-{linux-amd64,linux-arm64} binaries (Go-only build)
#   - kubectl-veltrix admin plugin (linux-amd64, linux-arm64, darwin-amd64,
#     darwin-arm64)
#   - VeltrixDB Helm chart (templates + values.yaml)
#   - Container image saved as a docker-archive tar (loadable via `docker load`
#     or `podman load` on the air-gapped target)
#   - Operator CRDs + manager manifest
#   - PromtheusRule alert YAML standalone copy
#   - Grafana dashboard JSON
#   - Install script: install-airgap.sh
#
# Caller environment:
#   VERSION              required, e.g. v1.0.0
#   IMAGE_REGISTRY       registry your air-gapped target will pull from
#                        (e.g. registry.internal.example.com)
#   OUT                  output directory (default ./airgap-out)
#
# Output: $OUT/veltrixdb-airgap-$VERSION.tar.zst (or .tar.gz if zstd missing)

set -euo pipefail

VERSION="${VERSION:?set VERSION=v1.0.0}"
IMAGE_REGISTRY="${IMAGE_REGISTRY:?set IMAGE_REGISTRY}"
OUT="${OUT:-./airgap-out}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

BUNDLE="veltrixdb-airgap-${VERSION}"
DEST="$WORK/$BUNDLE"
mkdir -p "$DEST"/{bin,helm,operator,images,grafana,prometheus,docs}

echo "[airgap] building binaries"

# Go-only build: works on any glibc Linux without liburing.  C++ io_uring
# layer requires platform-specific build, intentionally skipped for the
# portable airgap bundle.
cd "$REPO_ROOT"

build_one() {
  local goos="$1"; local goarch="$2"; local name="$3"; local pkg="$4"
  echo "  $goos-$goarch $name"
  GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 \
    go build -o "$DEST/bin/$name-$goos-$goarch" "$pkg"
}

build_one linux  amd64 veltrixdb-server  ./cmd/server
build_one linux  arm64 veltrixdb-server  ./cmd/server
build_one linux  amd64 kubectl-veltrix   ./cmd/kubectl-veltrix
build_one linux  arm64 kubectl-veltrix   ./cmd/kubectl-veltrix
build_one darwin amd64 kubectl-veltrix   ./cmd/kubectl-veltrix
build_one darwin arm64 kubectl-veltrix   ./cmd/kubectl-veltrix

echo "[airgap] copying helm chart + operator manifests"
cp -r "$REPO_ROOT/VeltrixDB-Helm-Chart"        "$DEST/helm/"
cp -r "$REPO_ROOT/VeltrixDB-Kubernetes-Operator" "$DEST/operator/"

echo "[airgap] copying observability artifacts"
cp -r "$REPO_ROOT/grafana"/* "$DEST/grafana/" 2>/dev/null || true
cp "$REPO_ROOT/VeltrixDB-Helm-Chart/templates/prometheusrule.yaml" \
   "$DEST/prometheus/prometheusrule.yaml.tmpl"

echo "[airgap] copying docs"
for f in README.md ARCHITECTURE.md PERFORMANCE.md BENCHMARKING.md \
         docs/DR_RUNBOOK.md docs/SOC2_CONTROLS.md; do
  src="$REPO_ROOT/$f"
  if [[ -f "$src" ]]; then
    mkdir -p "$DEST/docs/$(dirname "${f}")"
    cp "$src" "$DEST/docs/$f"
  fi
done

echo "[airgap] saving container image (skipped if docker is unavailable)"
if command -v docker > /dev/null 2>&1 && docker info > /dev/null 2>&1; then
  IMAGE_TAG="${IMAGE_REGISTRY}/veltrixdb:${VERSION}"
  if docker image inspect "$IMAGE_TAG" > /dev/null 2>&1; then
    docker save "$IMAGE_TAG" | gzip > "$DEST/images/veltrixdb-${VERSION}.tar.gz"
  else
    echo "  (image $IMAGE_TAG not present locally; build with 'docker build -t $IMAGE_TAG .' first)"
    cat > "$DEST/images/README.txt" <<EOF
Image not bundled. To produce it:
  docker build -t ${IMAGE_TAG} -f Dockerfile .
  docker save ${IMAGE_TAG} | gzip > veltrixdb-${VERSION}.tar.gz
EOF
  fi
else
  cat > "$DEST/images/README.txt" <<EOF
Docker not available at bundle build time. Produce the image on a build host:
  docker build -t ${IMAGE_REGISTRY}/veltrixdb:${VERSION} -f Dockerfile .
  docker save ${IMAGE_REGISTRY}/veltrixdb:${VERSION} | gzip > veltrixdb-${VERSION}.tar.gz
Then drop it into the bundle's images/ directory before tarring.
EOF
fi

echo "[airgap] writing install script"
cat > "$DEST/install-airgap.sh" <<'EOS'
#!/usr/bin/env bash
# install-airgap.sh — installer for an air-gapped target.
#
# Required env:
#   IMAGE_REGISTRY    your internal registry (must already be reachable)
#   NAMESPACE         k8s namespace (default: veltrixdb)
set -euo pipefail
IMAGE_REGISTRY="${IMAGE_REGISTRY:?set IMAGE_REGISTRY}"
NAMESPACE="${NAMESPACE:-veltrixdb}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# 1. Load the container image into the local docker / containerd.
if ls "$HERE"/images/*.tar.gz > /dev/null 2>&1; then
  echo "[airgap] loading container image"
  zcat "$HERE"/images/*.tar.gz | docker load
fi

# 2. Push to the internal registry.
echo "[airgap] pushing image to $IMAGE_REGISTRY"
docker push "$IMAGE_REGISTRY/veltrixdb:$(ls "$HERE"/images/ | grep -oE 'v[0-9.]+' | head -1)" || true

# 3. Install kubectl-veltrix plugin to /usr/local/bin
arch="$(uname -m)"
case "$arch" in
  x86_64)  plug="$HERE/bin/kubectl-veltrix-linux-amd64";;
  aarch64) plug="$HERE/bin/kubectl-veltrix-linux-arm64";;
  arm64)   plug="$HERE/bin/kubectl-veltrix-darwin-arm64";;
  *)       echo "unknown arch: $arch"; exit 1;;
esac
sudo install -m 0755 "$plug" /usr/local/bin/kubectl-veltrix
echo "[airgap] installed kubectl-veltrix"

# 4. Helm install
helm upgrade --install veltrixdb "$HERE/helm/VeltrixDB-Helm-Chart" \
  --namespace "$NAMESPACE" --create-namespace \
  --set image.repository="$IMAGE_REGISTRY/veltrixdb" \
  --set serviceMonitor.enabled=true

echo "[airgap] done. Test with: kubectl veltrix stats"
EOS
chmod +x "$DEST/install-airgap.sh"

echo "[airgap] writing manifest"
cat > "$DEST/MANIFEST.txt" <<EOF
VeltrixDB air-gapped install bundle
Version: ${VERSION}
Built: $(date -u +%Y-%m-%dT%H:%M:%SZ)
Target registry: ${IMAGE_REGISTRY}

Contents:
  bin/                 — server + kubectl plugin binaries (Go-only build)
  helm/                — Helm chart
  operator/            — Kubernetes Operator + CRDs
  images/              — container image (docker-archive)
  grafana/             — dashboards
  prometheus/          — PrometheusRule template
  docs/                — README, runbook, SOC2, perf docs
  install-airgap.sh    — installer (run on target with IMAGE_REGISTRY set)
EOF

cd "$WORK"
mkdir -p "$REPO_ROOT/$OUT"

if command -v zstd > /dev/null 2>&1; then
  echo "[airgap] packing tar.zst"
  tar -cf - "$BUNDLE" | zstd -19 -T0 > "$REPO_ROOT/$OUT/${BUNDLE}.tar.zst"
  ls -la "$REPO_ROOT/$OUT/${BUNDLE}.tar.zst"
else
  echo "[airgap] packing tar.gz (zstd not found)"
  tar -czf "$REPO_ROOT/$OUT/${BUNDLE}.tar.gz" "$BUNDLE"
  ls -la "$REPO_ROOT/$OUT/${BUNDLE}.tar.gz"
fi

echo "[airgap] done"
