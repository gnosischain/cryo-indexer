# Cryo Indexer - Simplified Makefile
.PHONY: help build start stop logs clean continuous historical fill-gaps validate backfill \
	run-migrations status ps restart shell validate-dataset fix-timestamps fix-timestamps-verbose \
	verify-timestamps strict-historical fix-workflow

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
	@echo "Operations:"
	@echo "  continuous           Start continuous indexing (follows chain tip)"
	@echo "  historical           Index a specific block range"
	@echo "  fill-gaps            Find and fill gaps in indexed data"
	@echo "  validate             Check data completeness"
	@echo "  backfill             Populate indexing_state from existing data"
	@echo "  fix-timestamps       Fix incorrect timestamps in tables"
	@echo ""
	@echo "Management:"
	@echo "  start                Start the indexer (continuous mode by default)"
	@echo "  stop                 Stop the indexer"
	@echo "  restart              Restart the indexer"
	@echo "  logs                 View logs"
	@echo "  status               Show indexing status"
	@echo "  ps                   Show running containers"
	@echo "  clean                Remove containers and volumes"
	@echo ""
	@echo "Utilities:"
	@echo "  shell                Open shell in container"
	@echo "  validate-dataset     Validate specific dataset"
	@echo "  verify-timestamps    Check for timestamp issues"
	@echo ""
	@echo "Examples:"
	@echo "  # Index specific range with 8 workers:"
	@echo "  make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8"
	@echo ""
	@echo "  # Index only blocks:"
	@echo "  make continuous MODE=minimal"
	@echo ""
	@echo "  # Index all data types:"
	@echo "  make continuous MODE=full"
	@echo ""
	@echo "  # Custom datasets:"
	@echo "  make continuous DATASETS=blocks,transactions,traces"
	@echo ""
	@echo "  # Fill gaps between blocks 1M and 2M:"
	@echo "  make fill-gaps START_BLOCK=1000000 END_BLOCK=2000000"
	@echo ""
	@echo "  # Backfill existing data (for pre-existing databases):"
	@echo "  make backfill"
	@echo ""
	@echo "  # Backfill specific range:"
	@echo "  make backfill START_BLOCK=1000000 END_BLOCK=2000000"
	@echo ""
	@echo "  # Force backfill (recreate existing entries):"
	@echo "  make backfill BACKFILL_FORCE=true"
	@echo ""
	@echo "  # Fix timestamps:"
	@echo "  make fix-timestamps"
	@echo ""
	@echo "Migration Workflow (for existing data):"
	@echo "  1. make backfill     # Populate indexing_state from existing data"
	@echo "  2. make validate     # Verify backfill worked correctly"
	@echo "  3. make fill-gaps    # Fill any remaining gaps"

# Build
build:
	docker build -t cryo-indexer:latest . --no-cache

# Run migrations
run-migrations:
	@echo "Running database migrations..."
	TIMESTAMP=$$(date +%Y%m%d%H%M%S) docker-compose --profile migrations up migrations
	@echo "Migrations complete"

# Continuous indexing (default)
continuous:
	OPERATION=continuous docker-compose up -d
	@echo "Started continuous indexing. Run 'make logs' to view logs."

# Historical indexing
historical:
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ]; then \
		echo "Error: START_BLOCK and END_BLOCK are required"; \
		echo "Usage: make historical START_BLOCK=<n> END_BLOCK=<n> [WORKERS=<n>]"; \
		exit 1; \
	fi
	OPERATION=historical RESTART_POLICY=no docker-compose up 
	@echo "Started historical indexing from $(START_BLOCK) to $(END_BLOCK)"

# Fill gaps
fill-gaps:
	OPERATION=fill_gaps RESTART_POLICY=no docker-compose up
	@echo "Gap filling complete. Check logs for results."

# Validate
validate:
	@OPERATION=validate RESTART_POLICY=no docker-compose up

# Backfill indexing_state from existing data
backfill:
	@echo "Starting backfill operation..."
	@if [ "$(BACKFILL_FORCE)" = "true" ]; then \
		echo "FORCE MODE: Will recreate existing entries"; \
	fi
	@if [ -n "$(START_BLOCK)" ] && [ -n "$(END_BLOCK)" ]; then \
		echo "Backfilling range: $(START_BLOCK) to $(END_BLOCK)"; \
	else \
		echo "Backfilling entire dataset"; \
	fi
	OPERATION=backfill RESTART_POLICY=no docker-compose up
	@echo "Backfill complete. Run 'make validate' to verify results."

# Start (alias for continuous)
start: continuous

# Stop
stop:
	docker-compose down

# Restart
restart: stop start

# View logs
logs:
	docker-compose logs -f --tail=100

# Status - query the database for current state
status:
	@echo "Checking indexing status..."
	@docker run --rm \
		--env-file .env \
		cryo-indexer:latest \
		python -c "from src.db.clickhouse_manager import ClickHouseManager; \
		from src.core.state_manager import StateManager; \
		import os; \
		cm = ClickHouseManager( \
			host=os.environ['CLICKHOUSE_HOST'], \
			user=os.environ.get('CLICKHOUSE_USER', 'default'), \
			password=os.environ['CLICKHOUSE_PASSWORD'], \
			database=os.environ.get('CLICKHOUSE_DATABASE', 'blockchain'), \
			port=int(os.environ.get('CLICKHOUSE_PORT', '8443')), \
			secure=os.environ.get('CLICKHOUSE_SECURE', 'true').lower() == 'true' \
		); \
		sm = StateManager(cm); \
		summary = sm.get_progress_summary('${MODE:-default}'); \
		print('\nIndexing Progress:'); \
		for dataset, stats in summary.items(): \
			print(f'\n{dataset}:'); \
			for k, v in stats.items(): \
				print(f'  {k}: {v}') if v is not None else None"

# Show running containers
ps:
	docker-compose ps

# Clean everything
clean:
	docker-compose down -v
	@echo "Removed all containers and volumes"

# Open shell in container
shell:
	docker-compose run --rm cryo-indexer bash

# Quick validation of specific dataset
validate-dataset:
	@if [ -z "$(DATASET)" ]; then \
		echo "Error: DATASET is required"; \
		echo "Usage: make validate-dataset DATASET=blocks"; \
		exit 1; \
	fi
	OPERATION=validate DATASETS=$(DATASET) RESTART_POLICY=no docker-compose up

# Timestamp fix operations
fix-timestamps:
	OPERATION=fix_timestamps docker-compose up cryo-indexer

fix-timestamps-verbose:
	OPERATION=fix_timestamps LOG_LEVEL=DEBUG docker-compose up cryo-indexer

verify-timestamps:
	OPERATION=fix_timestamps docker-compose run --rm cryo-indexer python -c \
		"from src.timestamp_fixer import TimestampFixer; \
		from src.db.clickhouse_manager import ClickHouseManager; \
		from src.config import settings; \
		ch = ClickHouseManager(settings.clickhouse_host, settings.clickhouse_user, \
			settings.clickhouse_password, settings.clickhouse_database, \
			settings.clickhouse_port, settings.clickhouse_secure); \
		fixer = TimestampFixer(ch, None); \
		import json; \
		print(json.dumps(fixer.verify_fixes(), indent=2))"

# Run with strict timestamp mode
strict-historical:
	OPERATION=historical STRICT_TIMESTAMP_MODE=true docker-compose up cryo-indexer

# Complete timestamp fix workflow
fix-workflow:
	@echo "Running complete timestamp fix workflow..."
	@$(MAKE) fix-timestamps
	@echo "Verifying fixes..."
	@$(MAKE) verify-timestamps

# Quick shortcuts for common workflows
.PHONY: backfill-all quick-backfill migration-workflow

# Backfill all existing data
backfill-all:
	@echo "Backfilling all existing data..."
	make backfill
	@echo "Backfill complete. Run 'make validate' to check results."

# Quick backfill with safe defaults
quick-backfill:
	@echo "Running quick backfill with conservative settings..."
	BACKFILL_BATCH_SIZE=500 BACKFILL_CHUNK_SIZE=50000 make backfill

# Complete migration workflow for existing data
migration-workflow:
	@echo "Starting complete migration workflow for existing data..."
	@echo "Step 1/3: Backfilling existing data..."
	make backfill || (echo "Backfill failed, stopping workflow"; exit 1)
	@echo ""
	@echo "Step 2/3: Validating backfill results..."
	make validate || (echo "Validation failed, check your data"; exit 1)
	@echo ""
	@echo "Step 3/3: Filling any remaining gaps..."
	make fill-gaps || (echo "Gap filling failed"; exit 1)
	@echo ""
	@echo "✓ Migration workflow complete!"
	@echo "Your existing data is now properly tracked in indexing_state."

# Performance testing shortcuts
.PHONY: conservative-settings test-range

# Set conservative settings for rate-limited RPCs
conservative-settings:
	$(eval export REQUESTS_PER_SECOND=20)
	$(eval export MAX_CONCURRENT_REQUESTS=3)
	$(eval export WORKERS=2)
	$(eval export BATCH_SIZE=500)
	@echo "Using conservative settings:"
	@echo "  REQUESTS_PER_SECOND=20"
	@echo "  MAX_CONCURRENT_REQUESTS=3"
	@echo "  WORKERS=2"
	@echo "  BATCH_SIZE=500"

# Test with a small range
test-range:
	@if [ -z "$(START_BLOCK)" ]; then \
		echo "Testing with default range (latest 1000 blocks)"; \
		make historical START_BLOCK=$$((latest-1000)) END_BLOCK=latest WORKERS=1; \
	else \
		echo "Testing range $(START_BLOCK) to $$(($(START_BLOCK)+1000))"; \
		make historical START_BLOCK=$(START_BLOCK) END_BLOCK=$$(($(START_BLOCK)+1000)) WORKERS=1; \
	fi