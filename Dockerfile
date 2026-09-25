# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.22-bookworm AS builder

# CGO_ENABLED=1 (default) builds the C++ layer in: the off-heap native index
# (storage/native_index.cpp, on by default), the batch engine, and the
# io_uring VLog write bridge (compiled in but off unless
# VELTRIXDB_URING_BRIDGE=on|sqpoll). The native index needs the node's
# vm.max_map_count >= 262144 at large key counts (scripts/sysctl.conf) — a
# container cannot set it. Pass --build-arg CGO_ENABLED=0 for the old fully
# static, pure-Go image (Go map index).
ARG CGO_ENABLED=1

RUN if [ "$CGO_ENABLED" = "1" ]; then \
      apt-get update -qq && \
      apt-get install -y --no-install-recommends g++ liburing-dev && \
      rm -rf /var/lib/apt/lists/*; \
    fi

WORKDIR /src

# Cache module downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# ISA: the cgo directives pin a portable baseline (x86-64-v2), never
# -march=native, so this image runs on any node it is scheduled to.
RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath \
    -o /bin/veltrixdb ./cmd/server

# Gather the shared libraries a cgo binary needs beyond glibc (liburing,
# libstdc++, libgcc_s) so the runtime stage does not depend on what a given
# distroless tag happens to ship. Empty for a CGO_ENABLED=0 build.
RUN mkdir -p /runtime-libs && \
    if [ "$CGO_ENABLED" = "1" ]; then \
      ldd /bin/veltrixdb | awk '/=> \// {print $3}' \
        | grep -Ev '/(libc|libm|libpthread|libdl|librt|ld-linux[^/]*)\.so' \
        | xargs -r -I{} cp -L {} /runtime-libs/ ; \
      ls -l /runtime-libs; \
    fi

# ── Stage 2: minimal runtime ──────────────────────────────────────────────────
# distroless/cc: glibc and nothing else — no shell, minimal attack surface.
# (distroless/static has no libc, so it cannot run a cgo binary.)
# Root variant (not :nonroot): raw VLog mode must open /dev/nvmeXnYpZ which is
# owned root:disk (mode 0660) — requires UID 0. The Operator's SecurityContext
# enforces runAsNonRoot:true + runAsUser:65532 for non-rawVLog deployments.
#
# io_uring under Kubernetes: the RuntimeDefault seccomp profile of recent
# containerd / Docker releases blocks io_uring_setup. The bridge is opt-in
# (VELTRIXDB_URING_BRIDGE); an opted-in engine detects the failure, logs it,
# and falls back to pwrite — correct, just without io_uring. To actually use
# it the pod needs a seccomp profile that
# allows io_uring_setup / io_uring_enter / io_uring_register. The native
# index needs nothing special.
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /runtime-libs/ /usr/lib/x86_64-linux-gnu/
COPY --from=builder /bin/veltrixdb /veltrixdb

# DB protocol port / Prometheus metrics port
EXPOSE 9000 2112

ENTRYPOINT ["/veltrixdb"]
