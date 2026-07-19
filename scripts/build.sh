#!/usr/bin/env bash
# scripts/build.sh — Build VeltrixDB fat binary with static C++ libs
#
# Usage:
#   ./scripts/build.sh                    # build everything
#   ./scripts/build.sh --go-only          # skip C++ (macOS dev)
#   ./scripts/build.sh --output ./dist    # custom output directory
#
# Requirements (Linux):
#   gcc/g++ 11+, cmake 3.20+, liburing-dev, go 1.19+
#
# The resulting tarball contains:
#   veltrixdb          — statically linked binary
#   config.yaml.example
#   scripts/sysctl.conf
#   scripts/hugepages.sh

set -euo pipefail

# ── Defaults ────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${REPO_ROOT}/dist"
GO_ONLY=false
VERSION="${VERSION:-$(git -C "${REPO_ROOT}" describe --tags --always --dirty 2>/dev/null || echo "dev")}"
GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --go-only)    GO_ONLY=true ; shift ;;
    --output)     OUTPUT_DIR="$2" ; shift 2 ;;
    --version)    VERSION="$2" ; shift 2 ;;
    *) echo "Unknown flag: $1" >&2 ; exit 1 ;;
  esac
done

mkdir -p "${OUTPUT_DIR}"

echo "=== VeltrixDB Build ==="
echo "  Version    : ${VERSION}"
echo "  GOOS/GOARCH: ${GOOS}/${GOARCH}"
echo "  Output     : ${OUTPUT_DIR}"
echo "  Go only    : ${GO_ONLY}"
echo ""

# ── Step 1: Build C++ shared objects (Linux only) ────────────────────────────
if [[ "${GO_ONLY}" == "false" && "${GOOS}" == "linux" ]]; then
  echo "--- Building C++ engine (io_uring ART scheduler) ---"

  CPP_BUILD_DIR="${REPO_ROOT}/cpp/build"
  mkdir -p "${CPP_BUILD_DIR}"

  cmake \
    -S "${REPO_ROOT}/cpp" \
    -B "${CPP_BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=gcc \
    -DCMAKE_CXX_COMPILER=g++ \
    -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_CXX_FLAGS="-O3 -march=native -DNDEBUG" \
    -DCMAKE_EXE_LINKER_FLAGS="-static-libstdc++ -static-libgcc"

  cmake --build "${CPP_BUILD_DIR}" --parallel "$(nproc)"
  echo "C++ build complete: ${CPP_BUILD_DIR}"
else
  if [[ "${GOOS}" != "linux" ]]; then
    echo "--- Skipping C++ build (GOOS=${GOOS}; io_uring is Linux-only) ---"
  else
    echo "--- Skipping C++ build (--go-only) ---"
  fi
fi

# ── Step 2: Build Go binary ──────────────────────────────────────────────────
echo ""
echo "--- Building Go control plane ---"

# CGO_ENABLED=0 for a fully static binary (no libc dependency).
# On Linux prod builds, set CGO_ENABLED=1 if C++ ART integration is used.
CGO_ENABLED="${CGO_ENABLED:-0}"

LDFLAGS=(
  "-s" "-w"
  "-X main.Version=${VERSION}"
  "-X main.BuildDate=${BUILD_DATE}"
)

# If C++ objects were built, link them in.
if [[ "${GO_ONLY}" == "false" && "${GOOS}" == "linux" && -d "${CPP_BUILD_DIR:-}" ]]; then
  CGO_ENABLED=1
  CPP_LIBS=$(find "${CPP_BUILD_DIR}" -name "*.a" 2>/dev/null | tr '\n' ' ')
  if [[ -n "${CPP_LIBS}" ]]; then
    export CGO_LDFLAGS="-L${CPP_BUILD_DIR} ${CPP_LIBS} -luring -lstdc++ -lm"
    echo "  Linking C++ libs: ${CPP_LIBS}"
  fi
fi

GO_BINARY="${OUTPUT_DIR}/veltrixdb"

( cd "${REPO_ROOT}" && \
  CGO_ENABLED="${CGO_ENABLED}" \
  GOOS="${GOOS}" \
  GOARCH="${GOARCH}" \
  go build \
    -ldflags "${LDFLAGS[*]}" \
    -o "${GO_BINARY}" \
    ./cmd/server )

echo "Go binary: ${GO_BINARY}"

# ── Step 3: Build load test binary ──────────────────────────────────────────
echo ""
echo "--- Building loadtest ---"

( cd "${REPO_ROOT}" && \
  CGO_ENABLED=0 \
  GOOS="${GOOS}" \
  GOARCH="${GOARCH}" \
  go build \
    -ldflags "-s -w -X main.Version=${VERSION}" \
    -o "${OUTPUT_DIR}/veltrixdb-loadtest" \
    ./cmd/loadtest )

echo "Loadtest binary: ${OUTPUT_DIR}/veltrixdb-loadtest"

# ── Step 4: Copy supporting files ────────────────────────────────────────────
echo ""
echo "--- Packaging ---"

cp "${SCRIPT_DIR}/sysctl.conf"    "${OUTPUT_DIR}/" 2>/dev/null || true
cp "${SCRIPT_DIR}/hugepages.sh"   "${OUTPUT_DIR}/" 2>/dev/null || true

# Generate a config.yaml.example
cat > "${OUTPUT_DIR}/config.yaml.example" << 'EOF'
# VeltrixDB Configuration — copy to config.yaml and edit before running.
# All fields have equivalent CLI flags; config file takes precedence.

server:
  addr: ":9000"
  metricsAddr: ":2112"

storage:
  # Comma-separated list of NVMe mount paths.
  # Single disk (dev): /var/lib/veltrixdb
  # 8-disk prod:       /mnt/nvme0,/mnt/nvme1,...,/mnt/nvme7
  dataDirs: "/var/lib/veltrixdb"

cache:
  # LIRS cache size in MiB.
  # Dev:  256 (256 MB)
  # Prod: 65536 (64 GB)
  maxSizeMB: 256

cluster:
  # Comma-separated list of peer addresses for gossip bootstrap.
  # Leave empty for standalone mode.
  peers: ""
  replicationFactor: 1
EOF

# ── Step 5: Create tarball ───────────────────────────────────────────────────
TARBALL="${OUTPUT_DIR}/veltrixdb-${VERSION}-${GOOS}-${GOARCH}.tar.gz"
tar -czf "${TARBALL}" \
  -C "${OUTPUT_DIR}" \
  veltrixdb \
  veltrixdb-loadtest \
  config.yaml.example \
  $(ls "${OUTPUT_DIR}/sysctl.conf" 2>/dev/null | xargs -I{} basename {}) \
  $(ls "${OUTPUT_DIR}/hugepages.sh" 2>/dev/null | xargs -I{} basename {}) \
  2>/dev/null || \
tar -czf "${TARBALL}" -C "${OUTPUT_DIR}" veltrixdb veltrixdb-loadtest config.yaml.example

echo ""
echo "=== Build complete ==="
echo "  Binary  : ${GO_BINARY}"
echo "  Tarball : ${TARBALL}"
echo ""
echo "Run: ${GO_BINARY} -addr :9000 -data ./data -cache 256"
