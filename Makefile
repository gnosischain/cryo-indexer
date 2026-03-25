# Cryo Indexer Makefile
.PHONY: help build start stop logs clean continuous historical maintain auto-maintain run-migrations run-migrations-live status validate live

# Load environment variables from .env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Set fallback defaults only if not defined in .env
MODE ?= custom
WORKERS ?= 1
DATASETS ?= blocks,transactions,logs,contracts,native_transfers,traces

# Default target
.DEFAULT_GOAL := help

# Help
help:
	@echo "Cryo Indexer - Commands"
	@echo ""
	@echo "Setup:"
	@echo "  build                Build the Docker image"
	@echo "  run-migrations       Run database migrations (execution DB)"
	@echo "  run-migrations-live  Run database migrations (execution_live DB)"
	@echo ""
	@echo "Core Operations:"
	@echo "  continuous           Start continuous indexing (follows chain tip)"
	@echo "  historical           Index a specific block range"
	@echo "  maintain             Fix data integrity (requires scrapers stopped)"
	@echo "  auto-maintain        Periodic self-healing (safe while continuous runs)"
	@echo "  validate             Check indexing status (read-only)"
	@echo "  live                 Start live indexer (near-real-time, execution_live DB)"
	@echo ""
	@echo "Management:"
	@echo "  start                Alias for continuous"
	@echo "  stop                 Stop all containers"
	@echo "  logs                 View logs"
	@echo "  status               Alias for validate"
	@echo "  clean                Remove containers and volumes"
	@echo "  shell                Open shell in container"
	@echo ""
	@echo "Indexing Modes:"
	@echo "  MODE=minimal         blocks, transactions, logs"
	@echo "  MODE=extra           contracts, native_transfers, traces"
	@echo "  MODE=diffs           balance_diffs, code_diffs, nonce_diffs, storage_diffs"
	@echo "  MODE=full            all datasets"
	@echo "  MODE=custom          use DATASETS variable (default)"
	@echo ""
	@echo "Examples:"
	@echo "  make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8"
	@echo "  make continuous MODE=full"
	@echo "  make maintain START_BLOCK=1000000 END_BLOCK=2000000"
	@echo "  make auto-maintain"
	@echo "  make live LIVE_START_BLOCK=45300000"

# ============================================================
# Setup
# ============================================================

build:
	docker build -t cryo-indexer:latest . --no-cache

run-migrations:
	@echo "Running database migrations (execution)..."
	docker compose --profile migrations up migrations

run-migrations-live:
	@echo "Running database migrations (execution_live)..."
	docker run --rm \
		--env-file .env \
		-e CLICKHOUSE_DATABASE=execution_live \
		-e MIGRATIONS_DIR=/app/migrations_live \
		-v $(CURDIR)/migrations_live:/app/migrations_live \
		--entrypoint python \
		cryo-indexer:latest \
		-m src migrations

# ============================================================
# Core Operations
# ============================================================

continuous:
	@echo "Starting continuous indexing with MODE=$(MODE)..."
	docker compose up cryo-indexer-$(MODE)

start: continuous

historical:
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ] || [ "$(START_BLOCK)" = "0" ] || [ "$(END_BLOCK)" = "0" ]; then \
		echo "Error: START_BLOCK and END_BLOCK are required and must be non-zero"; \
		echo "Usage: make historical START_BLOCK=<n> END_BLOCK=<n> [WORKERS=<n>]"; \
		exit 1; \
	fi
	@echo "Starting historical indexing from $(START_BLOCK) to $(END_BLOCK)..."
	docker compose --profile historical up historical-job

maintain:
	@echo "Starting maintenance operation..."
	docker compose --profile maintain up maintain-job

auto-maintain:
	@echo "Starting auto-maintain (safe while continuous runs)..."
	docker compose run \
		-e OPERATION=auto-maintain \
		-e MODE=$(MODE) \
		-e DATASETS=$(DATASETS) \
		-e WORKERS=$(WORKERS) \
		-e AUTO_MAINTAIN_LOOKBACK_HOURS=$(AUTO_MAINTAIN_LOOKBACK_HOURS) \
		-e STUCK_RANGE_TIMEOUT_HOURS=$(STUCK_RANGE_TIMEOUT_HOURS) \
		maintain-job

AUTO_MAINTAIN_LOOKBACK_HOURS ?= 48
STUCK_RANGE_TIMEOUT_HOURS ?= 2

validate:
	@echo "Checking indexing status..."
	docker compose --profile validate up validate-job

status: validate

# ============================================================
# Live Indexer (execution_live DB, near-real-time)
# ============================================================

LIVE_START_BLOCK ?= 0

live:
	@echo "Starting live indexer (execution_live, BATCH_SIZE=10)..."
	LIVE_START_BLOCK=$(LIVE_START_BLOCK) docker compose up cryo-indexer-live

# ============================================================
# Management
# ============================================================

stop:
	docker compose down --remove-orphans

logs:
	docker compose logs -f --tail=100

clean:
	docker compose down -v --remove-orphans
	@echo "Removed all containers and volumes"

shell:
	docker compose run --rm cryo-indexer-custom bash

# ============================================================
# Quick mode shortcuts
# ============================================================

.PHONY: minimal extra diffs full custom

minimal:
	MODE=minimal make continuous

extra:
	MODE=extra make continuous

diffs:
	MODE=diffs make continuous

full:
	MODE=full make continuous

custom:
	@if [ -z "$(DATASETS)" ]; then \
		echo "Error: DATASETS is required for custom mode"; \
		echo "Usage: make custom DATASETS=blocks,transactions,logs"; \
		exit 1; \
	fi
	MODE=custom make continuous

# ============================================================
# Development and testing
# ============================================================

.PHONY: test-range

test-range:
	@echo "Testing with 1000 blocks..."
	make historical START_BLOCK=$(or $(START_BLOCK),18000000) END_BLOCK=$(or $(END_BLOCK),18001000) WORKERS=1
