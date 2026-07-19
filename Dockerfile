# ── Stage 1: Go build ─────────────────────────────────────────────────────────
FROM golang:1.22-bookworm AS builder

WORKDIR /src

# Cache module downloads separately from source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath \
    -o /bin/veltrixdb ./cmd/server

# ── Stage 2: Minimal runtime ───────────────────────────────────────────────────
# distroless/static has no shell, no libc, minimal attack surface.
# Root variant (not :nonroot): raw VLog mode must open /dev/nvmeXnYpZ which is
# owned root:disk (mode 0660) — requires UID 0. The Operator's SecurityContext
# enforces runAsNonRoot:true + runAsUser:65532 for non-rawVLog deployments.
FROM gcr.io/distroless/static-debian12

COPY --from=builder /bin/veltrixdb /veltrixdb

# DB protocol port / Prometheus metrics port
EXPOSE 9000 2112

ENTRYPOINT ["/veltrixdb"]
