# Cryo Indexer - Makefile
.PHONY: help build start stop logs clean continuous historical fill-gaps validate backfill \
	run-migrations status ps restart shell fix-timestamps

# Default target
.DEFAULT_GOAL := help

# Help
help:
	@echo "Cryo Indexer - Commands"
	@echo ""
	@echo "Setup:"
	@echo "  build                Build the Docker image"
	@echo "  run-migrations       Run database migrations"
	@echo ""
	@echo "Operations:"
	@echo "  continuous           Start continuous indexing (follows chain tip)"
	@echo "  historical           Index a specific block range"
	@echo "  fill-gaps            Find and fill gaps in indexed data"
	@echo "  validate             Check data completeness"
	@echo "  backfill             Populate indexing_state from existing data"
	@echo "  fix-timestamps       Fix incorrect timestamps in tables"
	@echo ""
	@echo "Management:"
	@echo "  start                Start the indexer (alias for continuous)"
	@echo "  stop                 Stop the indexer"
	@echo "  restart              Restart the indexer"
	@echo "  logs                 View logs"
	@echo "  status               Show indexing status"
	@echo "  ps                   Show running containers"
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
	@echo "  make continuous MODE=custom DATASETS=blocks,transactions,traces"
	@echo "  make fill-gaps START_BLOCK=1000000 END_BLOCK=2000000"
	@echo "  make backfill BACKFILL_FORCE=true"

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
	@echo "Starting continuous indexing with MODE=$(MODE:-minimal)"
	docker-compose up -d cryo-indexer-$(MODE:-minimal)
	@echo "Started continuous indexing. Run 'make logs' to view logs."

# Start (alias for continuous)
start: continuous

# Historical indexing
historical:
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ]; then \
		echo "Error: START_BLOCK and END_BLOCK are required"; \
		echo "Usage: make historical START_BLOCK=<n> END_BLOCK=<n> [WORKERS=<n>]"; \
		exit 1; \
	fi
	@echo "Starting historical indexing from $(START_BLOCK) to $(END_BLOCK)"
	docker-compose --profile historical up historical-job

# Fill gaps in indexed data
fill-gaps:
	@echo "Starting gap filling operation..."
	docker-compose --profile fill-gaps up fill-gaps-job

# Validate indexed data
validate:
	@echo "Running validation..."
	@docker-compose --profile validate up validate-job

# Backfill indexing_state from existing data
backfill:
	@echo "Starting backfill operation..."
	@if [ "$(BACKFILL_FORCE)" = "true" ]; then \
		echo "FORCE MODE: Will recreate existing entries"; \
	fi
	docker-compose --profile backfill up backfill-job

# Fix incorrect timestamps
fix-timestamps:
	@echo "Starting timestamp fix operation..."
	docker-compose --profile fix-timestamps up fix-timestamps-job

# Stop all containers
stop:
	docker-compose down

# Restart the indexer
restart: stop start

# View logs
logs:
	docker-compose logs -f --tail=100

# Show indexing status
status:
	@echo "Checking indexing status..."
	@docker-compose --profile validate up validate-job | grep -A 1000 "=== INDEXING PROGRESS ===" || echo "No status available"

# Show running containers
ps:
	docker-compose ps

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

# Common workflows
.PHONY: migration-workflow quick-historical conservative-settings

# Complete migration workflow for existing data
migration-workflow:
	@echo "Starting migration workflow..."
	@echo "Step 1/3: Backfilling existing data..."
	@$(MAKE) backfill
	@echo ""
	@echo "Step 2/3: Validating backfill..."
	@$(MAKE) validate
	@echo ""
	@echo "Step 3/3: Filling gaps..."
	@$(MAKE) fill-gaps
	@echo ""
	@echo "✓ Migration workflow complete!"

# Quick historical with conservative settings
quick-historical:
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ]; then \
		echo "Error: START_BLOCK and END_BLOCK are required"; \
		exit 1; \
	fi
	WORKERS=2 BATCH_SIZE=100 REQUESTS_PER_SECOND=10 \
	make historical START_BLOCK=$(START_BLOCK) END_BLOCK=$(END_BLOCK)

# Apply conservative settings for rate-limited RPCs
conservative-settings:
	@echo "Applying conservative settings..."
	$(eval export WORKERS=2)
	$(eval export BATCH_SIZE=100)
	$(eval export REQUESTS_PER_SECOND=10)
	$(eval export MAX_CONCURRENT_REQUESTS=2)
	@echo "Settings applied: WORKERS=2 BATCH_SIZE=100 REQUESTS_PER_SECOND=10"

# Enhanced operations with options
.PHONY: fill-gaps-enhanced backfill-force

# Fill gaps with enhanced options
fill-gaps-enhanced:
	HANDLE_FAILED_RANGES=true DELETE_FAILED_BEFORE_RETRY=true \
	make fill-gaps

# Force backfill (delete and recreate)
backfill-force:
	BACKFILL_FORCE=true make backfill

# Development and testing
.PHONY: test-range logs-follow

# Test with a small range
test-range:
	@echo "Testing with 1000 blocks..."
	make historical START_BLOCK=$(START_BLOCK:-18000000) END_BLOCK=$$(($(START_BLOCK:-18000000)+1000)) WORKERS=1

# Follow logs with better formatting
logs-follow:
	docker-compose logs -f --tail=50 | grep -E "INFO|ERROR|WARNING"

# Database queries
.PHONY: db-stats gaps-report stale-jobs

# Show database statistics
db-stats:
	@echo "Database statistics..."
	@docker run --rm --env-file .env cryo-indexer:latest python -c "\
	from src.db.clickhouse_manager import ClickHouseManager; \
	import os; \
	cm = ClickHouseManager( \
		host=os.environ['CLICKHOUSE_HOST'], \
		user=os.environ.get('CLICKHOUSE_USER', 'default'), \
		password=os.environ['CLICKHOUSE_PASSWORD'], \
		database=os.environ.get('CLICKHOUSE_DATABASE', 'blockchain'), \
		port=int(os.environ.get('CLICKHOUSE_PORT', '8443')), \
		secure=os.environ.get('CLICKHOUSE_SECURE', 'true').lower() == 'true' \
	); \
	client = cm._connect(); \
	tables = ['blocks', 'transactions', 'logs']; \
	for table in tables: \
		try: \
			result = client.query(f'SELECT COUNT(*) FROM {cm.database}.{table}'); \
			print(f'{table}: {result.result_rows[0][0]:,} rows'); \
		except: \
			print(f'{table}: no data')"

# Show gaps report
gaps-report:
	@echo "Checking for gaps..."
	docker-compose --profile validate up validate-job | grep -A 50 "=== CHECKING FOR GAPS ===" || echo "No gaps found"

# Show stale jobs
stale-jobs:
	@echo "Checking for stale jobs..."
	docker-compose --profile validate up validate-job | grep -A 20 "=== STALE JOBS ===" || echo "No stale jobs"

# Help for specific operations
.PHONY: help-historical help-backfill help-modes

help-historical:
	@echo "Historical Indexing Help"
	@echo ""
	@echo "Required parameters:"
	@echo "  START_BLOCK    Starting block number"
	@echo "  END_BLOCK      Ending block number"
	@echo ""
	@echo "Optional parameters:"
	@echo "  WORKERS        Number of parallel workers (default: 1)"
	@echo "  BATCH_SIZE     Blocks per batch (default: 1000)"
	@echo "  MODE           Indexing mode (default: minimal)"
	@echo ""
	@echo "Examples:"
	@echo "  make historical START_BLOCK=1000000 END_BLOCK=2000000"
	@echo "  make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8 MODE=full"

help-backfill:
	@echo "Backfill Help"
	@echo ""
	@echo "Backfill creates indexing_state entries from existing data."
	@echo ""
	@echo "Options:"
	@echo "  START_BLOCK      Start of range to backfill (optional)"
	@echo "  END_BLOCK        End of range to backfill (optional)"
	@echo "  BACKFILL_FORCE   Delete existing entries first (true/false)"
	@echo ""
	@echo "Examples:"
	@echo "  make backfill                    # Backfill all data"
	@echo "  make backfill BACKFILL_FORCE=true  # Force recreate all entries"
	@echo "  make backfill START_BLOCK=1000000 END_BLOCK=2000000"

help-modes:
	@echo "Indexing Modes Help"
	@echo ""
	@echo "Available modes:"
	@echo "  minimal   blocks, transactions, logs (default)"
	@echo "  extra     contracts, native_transfers, traces"
	@echo "  diffs     balance_diffs, code_diffs, nonce_diffs, storage_diffs"
	@echo "  full      all datasets"
	@echo "  custom    specify datasets with DATASETS variable"
	@echo ""
	@echo "Examples:"
	@echo "  make continuous MODE=minimal"
	@echo "  make continuous MODE=full"
	@echo "  make continuous MODE=custom DATASETS=blocks,transactions,traces"