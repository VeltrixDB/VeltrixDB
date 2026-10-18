# VeltrixDB — shortcuts for building, running and benchmarking.
# `make help` lists the targets. Every target is a thin wrapper around a
# script or go command you can also run by hand; QUICKSTART.md explains each.

GO      ?= go
BIN_DIR ?= bin
ADDR    ?= 127.0.0.1:9000
DATA    ?= ./dev-data
CACHE   ?= 256
MODE    ?= raft

# YCSB (bench/compare): workloads, records and threads for the bench-ycsb* targets.
WORKLOADS ?= a b c f
RECORDS   ?= 100000
OPS       ?= 100000
THREADS   ?= 32

LOCAL_DIR := $(CURDIR)/.local-cluster

.DEFAULT_GOAL := help
.PHONY: help build run docker-run test test-short cluster-up cluster-status cluster-down cluster-clean \
	bench-quick bench bench-ycsb bench-ycsb-cluster bench-compare bench-gcp-dry-run bench-gcp clean

help: ## list targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[1m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## build the server, CLI and loadtest into ./bin
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/veltrixdb ./cmd/server
	$(GO) build -o $(BIN_DIR)/veltrix ./cmd/veltrix
	$(GO) build -o $(BIN_DIR)/loadtest ./cmd/loadtest
	@echo "built: $(BIN_DIR)/veltrixdb $(BIN_DIR)/veltrix $(BIN_DIR)/loadtest"

run: build ## run one node in the foreground (ADDR, DATA, CACHE)
	$(BIN_DIR)/veltrixdb -addr $(ADDR) -data $(DATA) -cache $(CACHE)

docker-run: ## run the published image (linux/amd64) on :9000
	docker run --rm -p 9000:9000 -p 2112:2112 ghcr.io/veltrixdb/veltrixdb:latest

test-short: ## unit tests with -short (skips the slowest crash / soak tests)
	$(GO) test -short -count=1 ./storage/... ./cmd/... ./cluster/... ./replication/...

test: ## full unit suite (consensus alone takes ~4 min)
	$(GO) test -count=1 ./...

cluster-up: ## start a local 3-node cluster (MODE=raft|replicated)
	MODE=$(MODE) DIR=$(LOCAL_DIR) ./scripts/local-cluster.sh up

cluster-status: ## show the local cluster's nodes and raft leader
	DIR=$(LOCAL_DIR) ./scripts/local-cluster.sh status

cluster-down: ## stop the local cluster (data kept)
	DIR=$(LOCAL_DIR) ./scripts/local-cluster.sh down

cluster-clean: ## stop the local cluster and delete its data
	DIR=$(LOCAL_DIR) ./scripts/local-cluster.sh clean

bench-quick: ## 1-minute single-node benchmark, prints one table (DURATION, CONCURRENCY, VALUE_SIZE)
	./scripts/quick-bench.sh

bench: ## gated storage benchmark: bulk load, reads, mixed, write stress (scripts/bench.sh)
	./scripts/bench.sh

bench-ycsb: ## YCSB against a throwaway local node, no Docker (WORKLOADS, RECORDS, OPS, THREADS)
	@MODE=standalone DIR=$(CURDIR)/.local-ycsb BASE_PORT=9400 ./scripts/local-cluster.sh up >/dev/null; \
	cd bench/compare && START=0 DBS=veltrixdb VELTRIX_ADDR=127.0.0.1:9400 \
	  WORKLOADS="$(WORKLOADS)" RECORDS=$(RECORDS) OPS=$(OPS) THREADS=$(THREADS) ./compare.sh; rc=$$?; \
	cd $(CURDIR) && DIR=$(CURDIR)/.local-ycsb ./scripts/local-cluster.sh clean >/dev/null; exit $$rc

bench-ycsb-cluster: ## YCSB against the running `make cluster-up` cluster (3 nodes, RF 3)
	cd bench/compare && START=0 NODES=3 DBS=veltrixdb VELTRIX_MODE=$$(cat $(LOCAL_DIR)/mode) \
	  VELTRIX_ADDRS=127.0.0.1:9000,127.0.0.1:9010,127.0.0.1:9020 \
	  VELTRIX_METRICS_ADDRS=127.0.0.1:9004,127.0.0.1:9014,127.0.0.1:9024 \
	  WORKLOADS="$(WORKLOADS)" RECORDS=$(RECORDS) OPS=$(OPS) THREADS=$(THREADS) ./compare.sh

bench-compare: ## VeltrixDB vs Aerospike vs ScyllaDB on this machine (needs Docker; BENCH_DATA on NVMe)
	cd bench/compare && ./compare.sh

bench-gcp-dry-run: ## print the gcloud commands of the 3-VM GCP benchmark (creates nothing)
	cd bench/compare && DRY_RUN=1 PROJECT=$(or $(PROJECT),example-project) ./gcp-multihost.sh

bench-gcp: ## REAL 3-VM + client benchmark on GCP, deleted at the end (PROJECT=<your sandbox> required; costs money)
	@test -n "$(PROJECT)" || { echo "set PROJECT=<gcp project>"; exit 2; }
	cd bench/compare && PROJECT=$(PROJECT) ./gcp-multihost.sh

clean: ## remove ./bin and local cluster / benchmark data
	-DIR=$(LOCAL_DIR) ./scripts/local-cluster.sh clean >/dev/null 2>&1
	rm -rf $(BIN_DIR) .local-ycsb
