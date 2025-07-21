# Cryo Indexer - Simplified Makefile
.PHONY: help build start stop logs clean continuous historical maintain run-migrations status

# Load environment variables from .env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Set fallback defaults only if not defined in .env
MODE ?= minimal
WORKERS ?= 1

# Default target
.DEFAULT_GOAL := help

# Help
help:
	@echo "Cryo Indexer - Simplified Commands"
	@echo ""
	@echo "Setup:"
	@echo "  build                Build the Docker image"
	@echo "  run-migrations       Run database migrations"
	@echo ""
	@echo "Core Operations:"
	@echo "  continuous           Start continuous indexing (follows chain tip)"
	@echo "  historical           Index a specific block range"
	@echo "  maintain             Fix data integrity issues (gaps, failed ranges, timestamps)"
	@echo ""
	@echo "Management:"
	@echo "  start                Start the indexer (alias for continuous)"
	@echo "  stop                 Stop the indexer"
	@echo "  logs                 View logs"
	@echo "  status               Show indexing status"
	@echo "  clean                Remove containers and volumes"
	@echo "  shell                Open shell in container"
	@echo ""
	@echo "Indexing Modes:"
	@echo "  MODE=minimal         blocks, transactions, logs (default)"
	@echo "  MODE=extra           contracts, native_transfers, traces"
	@echo "  MODE=diffs           balance_diffs, code_diffs, nonce_diffs, storage_diffs"
	@echo "  MODE=full            all datasets"
	@echo "  MODE=custom          use DATASETS variable"
	@echo ""
	@echo "Examples:"
	@echo "  make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8"
	@echo "  make continuous MODE=full"
	@echo "  make maintain START_BLOCK=1000000 END_BLOCK=2000000"

# Build the Docker image
build:
	docker build -t cryo-indexer:latest .

# Run database migrations
run-migrations:
	@echo "Running database migrations..."
	docker-compose --profile migrations up migrations
	@echo "Migrations complete"

# Start continuous indexing
continuous:
	@echo "Starting continuous indexing with MODE=$(MODE)"
	docker-compose up -d cryo-indexer-$(MODE)
	@echo "Started continuous indexing. Run 'make logs' to view logs."

# Start (alias for continuous)
start: continuous

# Historical indexing
historical:
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ] || [ "$(START_BLOCK)" = "0" ] || [ "$(END_BLOCK)" = "0" ]; then \
		echo "Error: START_BLOCK and END_BLOCK are required and must be non-zero"; \
		echo "Usage: make historical START_BLOCK=<n> END_BLOCK=<n> [WORKERS=<n>]"; \
		exit 1; \
	fi
	@echo "Starting historical indexing from $(START_BLOCK) to $(END_BLOCK)"
	docker-compose --profile historical up historical-job

# Maintain data integrity
maintain:
	@echo "Starting maintenance operation..."
	docker-compose --profile maintain up maintain-job

# Stop all containers
stop:
	docker-compose down

# View logs
logs:
	docker-compose logs -f --tail=100

# Show indexing status
status:
	@echo "Checking indexing status..."
	@docker-compose --profile validate up validate-job

# Clean everything (containers and volumes)
clean:
	docker-compose down -v
	@echo "Removed all containers and volumes"

# Open shell in container
shell:
	docker-compose run --rm cryo-indexer-minimal bash

# Quick operations for different modes
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

# Development and testing
.PHONY: test-range

# Test with a small range
test-range:
	@echo "Testing with 1000 blocks..."
	make historical START_BLOCK=$(START_BLOCK:-18000000) END_BLOCK=$(($(START_BLOCK:-18000000)+1000)) WORKERS=1