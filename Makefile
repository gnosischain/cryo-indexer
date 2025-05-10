.PHONY: help build-image run-migrations force-migrations debug-migrations \
       run-indexer run-state run-traces run-tokens run-multi run-parallel \
       stop stop-multi stop-parallel \
       logs logs-service logs-parallel \
       backfill backfill-blocks backfill-transactions backfill-full clean-backfill \
       clean purge-indexer purge-state purge-traces purge-tokens purge-parallel purge-migrations purge-all start

# Default environment file
ENV_FILE := .env

# Image name and tag
IMAGE_NAME := cryo-indexer
IMAGE_TAG := latest

# Load environment variables from .env file if it exists
ifneq (,$(wildcard $(ENV_FILE)))
	include $(ENV_FILE)
	export
endif

# Default values for environment variables
PARALLEL_WORKERS ?= 4
WORKER_BATCH_SIZE ?= 1000
MEMORY_LIMIT ?= 4G
CPU_LIMIT ?= 4

help:
	@echo "Cryo Indexer Commands:"
	@echo ""
	@echo "=== Setup ==="
	@echo "  make build-image      - Build a single Docker image for all services"
	@echo "  make run-migrations   - Run database migrations only"
	@echo "  make force-migrations - Run database migrations, even if already applied"
	@echo "  make debug-migrations - Debug migrations with shell access"
	@echo ""
	@echo "=== Run Indexers ==="
	@echo "  make run-indexer      - Start the indexer in default sequential mode"
	@echo "  make run-state        - Start the state diffs indexer"
	@echo "  make run-traces       - Start the traces indexer"
	@echo "  make run-tokens       - Start the token data indexer"
	@echo "  make run-multi        - Start all specialized indexers"
	@echo "  make run-parallel     - Start the parallel indexer with multiple workers"
	@echo "  make run-parallel PARALLEL_WORKERS=8 - Start with 8 worker processes"
	@echo "  make start            - Run migrations and start the default indexer"
	@echo ""
	@echo "=== Stop Indexers ==="
	@echo "  make stop             - Stop the default indexer"
	@echo "  make stop-multi       - Stop all specialized indexers"
	@echo "  make stop-parallel    - Stop the parallel indexer"
	@echo "  make stop-all         - Stop all services"
	@echo ""
	@echo "=== Logs ==="
	@echo "  make logs             - View logs from the default indexer"
	@echo "  make logs-service S=<service> - View logs for a specific service"
	@echo "  make logs-parallel    - View logs from the parallel indexer"
	@echo ""
	@echo "=== Backfill ==="
	@echo "  make backfill START_BLOCK=<block> [END_BLOCK=<block>] [MODE=<mode>] [DATASETS=<datasets>]"
	@echo "  make backfill-blocks START_BLOCK=<block> [END_BLOCK=<block>]"
	@echo "  make backfill-transactions START_BLOCK=<block> [END_BLOCK=<block>]"
	@echo "  make backfill-full START_BLOCK=<block> [END_BLOCK=<block>]"
	@echo "  make clean-backfill   - Clean up backfill containers and volumes"
	@echo ""
	@echo "=== Cleanup ==="
	@echo "  make clean            - Remove all containers and volumes"
	@echo "  make purge-indexer    - Delete only the default indexer volumes"
	@echo "  make purge-state      - Delete only the state diffs indexer volumes"
	@echo "  make purge-traces     - Delete only the traces indexer volumes"
	@echo "  make purge-tokens     - Delete only the tokens indexer volumes"
	@echo "  make purge-parallel   - Delete only the parallel indexer volumes"
	@echo "  make purge-migrations - Delete only the migrations volumes"
	@echo "  make purge-all        - Delete all Docker volumes (complete reset)"

# Build the Docker image
build-image:
	@echo "Building Docker image for all services..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "Image $(IMAGE_NAME):$(IMAGE_TAG) built successfully."

# Database migrations
run-migrations:
	@echo "Running database migrations..."
	docker compose -f docker-compose.migrations.yml up --abort-on-container-exit

force-migrations:
	@echo "Forcing database migrations to run..."
	FORCE_RUN_MIGRATIONS=true docker compose -f docker-compose.migrations.yml up --abort-on-container-exit

debug-migrations:
	@echo "Debugging migrations with shell access..."
	docker compose -f docker-compose.migrations.yml run --rm --entrypoint /bin/bash cryo-migrations

# Run indexers
run-indexer:
	@echo "Starting the indexer in default sequential mode..."
	docker compose up -d

run-state:
	@echo "Starting the state diffs indexer..."
	docker compose -f docker-compose.state.yml up -d

run-traces:
	@echo "Starting the traces indexer..."
	docker compose -f docker-compose.traces.yml up -d

run-tokens:
	@echo "Starting the token data indexer..."
	docker compose -f docker-compose.tokens.yml up -d

run-multi:
	@echo "Starting all specialized indexers..."
	docker compose -f docker-compose.multi.yml up -d
	@echo "All specialized indexers started."

run-parallel:
	@echo "Starting parallel indexer with $(PARALLEL_WORKERS) workers..."
	PARALLEL_WORKERS=$(PARALLEL_WORKERS) \
	WORKER_BATCH_SIZE=$(WORKER_BATCH_SIZE) \
	MEMORY_LIMIT=$(MEMORY_LIMIT) \
	CPU_LIMIT=$(CPU_LIMIT) \
	docker compose -f docker-compose.parallel.yml up -d
	@echo "Parallel indexer started with $(PARALLEL_WORKERS) workers."

# Convenience target to run migrations and start the indexer
start: run-migrations run-indexer
	@echo "Indexer started with migrations applied."

# Stop services
stop:
	@echo "Stopping the default indexer..."
	docker compose down

stop-multi:
	@echo "Stopping all specialized indexers..."
	docker compose -f docker-compose.multi.yml down

stop-parallel:
	@echo "Stopping parallel indexer..."
	docker compose -f docker-compose.parallel.yml down

stop-all: stop stop-multi stop-parallel
	@echo "All indexers stopped."

# View logs
logs:
	@echo "Showing default indexer logs..."
	docker compose logs -f --tail=500

logs-service:
	@if [ -z "$(S)" ]; then \
		echo "Please specify a service with S=<service_name>"; \
		echo "Example: make logs-service S=cryo-indexer-state"; \
		exit 1; \
	fi
	@echo "Showing logs for $(S)..."
	docker logs -f $(S) --tail=500

logs-parallel:
	@echo "Showing parallel indexer logs..."
	docker logs -f cryo-indexer-parallel --tail=500

# Backfill operations
backfill:
	@if [ -z "$(START_BLOCK)" ]; then \
		echo "Error: START_BLOCK parameter is required"; \
		echo "Usage: make backfill START_BLOCK=<block> [END_BLOCK=<block>] [MODE=<mode>] [DATASETS=<datasets>]"; \
		exit 1; \
	fi
	@echo "Running backfill with START_BLOCK=$(START_BLOCK), END_BLOCK=$(END_BLOCK), MODE=$(MODE), DATASETS=$(DATASETS)"
	@BACKFILL_ID=$$(date +%Y%m%d%H%M%S) \
	START_BLOCK=$(START_BLOCK) \
	END_BLOCK=$(END_BLOCK) \
	MODE=$(MODE) \
	DATASETS=$(DATASETS) \
	docker compose -f docker-compose.backfill.yml up --abort-on-container-exit

backfill-blocks:
	@if [ -z "$(START_BLOCK)" ]; then \
		echo "Error: START_BLOCK parameter is required"; \
		echo "Usage: make backfill-blocks START_BLOCK=<block> [END_BLOCK=<block>]"; \
		exit 1; \
	fi
	@echo "Running blocks backfill from block $(START_BLOCK)"
	@make backfill MODE=blocks START_BLOCK=$(START_BLOCK) END_BLOCK=$(END_BLOCK)

backfill-transactions:
	@if [ -z "$(START_BLOCK)" ]; then \
		echo "Error: START_BLOCK parameter is required"; \
		echo "Usage: make backfill-transactions START_BLOCK=<block> [END_BLOCK=<block>]"; \
		exit 1; \
	fi
	@echo "Running transactions backfill from block $(START_BLOCK)"
	@make backfill MODE=transactions START_BLOCK=$(START_BLOCK) END_BLOCK=$(END_BLOCK)

backfill-full:
	@if [ -z "$(START_BLOCK)" ]; then \
		echo "Error: START_BLOCK parameter is required"; \
		echo "Usage: make backfill-full START_BLOCK=<block> [END_BLOCK=<block>]"; \
		exit 1; \
	fi
	@echo "Running full backfill from block $(START_BLOCK)"
	@make backfill MODE=full START_BLOCK=$(START_BLOCK) END_BLOCK=$(END_BLOCK)

# Clean backfill volumes
clean-backfill:
	@echo "Cleaning up backfill containers and volumes..."
	docker compose -f docker-compose.backfill.yml down -v

# Clean up containers and volumes
clean:
	@echo "Cleaning up containers and volumes..."
	docker compose down -v
	docker compose -f docker-compose.migrations.yml down -v
	docker compose -f docker-compose.state.yml down -v
	docker compose -f docker-compose.traces.yml down -v
	docker compose -f docker-compose.tokens.yml down -v
	docker compose -f docker-compose.multi.yml down -v
	docker compose -f docker-compose.parallel.yml down -v
	@echo "Cleanup complete."

# Volume purge commands
purge-indexer:
	@echo "Stopping default indexer containers..."
	docker compose down
	@echo "Deleting default indexer volumes..."
	docker volume rm -f cryo-indexer_indexer_data cryo-indexer_indexer_logs cryo-indexer_indexer_state
	@echo "Default indexer volumes purged."

purge-state:
	@echo "Stopping state diffs indexer containers..."
	docker compose -f docker-compose.state.yml down
	@echo "Deleting state diffs indexer volumes..."
	docker volume rm -f cryo-indexer_state_data cryo-indexer_state_logs cryo-indexer_state_state
	@echo "State diffs indexer volumes purged."

purge-traces:
	@echo "Stopping traces indexer containers..."
	docker compose -f docker-compose.traces.yml down
	@echo "Deleting traces indexer volumes..."
	docker volume rm -f cryo-indexer_traces_data cryo-indexer_traces_logs cryo-indexer_traces_state
	@echo "Traces indexer volumes purged."

purge-tokens:
	@echo "Stopping tokens indexer containers..."
	docker compose -f docker-compose.tokens.yml down
	@echo "Deleting tokens indexer volumes..."
	docker volume rm -f cryo-indexer_tokens_data cryo-indexer_tokens_logs cryo-indexer_tokens_state
	@echo "Tokens indexer volumes purged."

purge-parallel:
	@echo "Stopping parallel indexer containers..."
	docker compose -f docker-compose.parallel.yml down
	@echo "Deleting parallel indexer volumes..."
	docker volume rm -f cryo-indexer_parallel_data cryo-indexer_parallel_logs cryo-indexer_parallel_state
	@echo "Parallel indexer volumes purged."

purge-migrations:
	@echo "Stopping migration containers..."
	docker compose -f docker-compose.migrations.yml down
	@echo "Deleting migration volumes..."
	docker volume rm -f cryo-indexer_migration_data cryo-indexer_migration_logs cryo-indexer_migration_state
	@echo "Migration volumes purged."

purge-all:
	@echo "Stopping all containers..."
	docker compose down -v || true
	docker compose -f docker-compose.migrations.yml down -v || true
	docker compose -f docker-compose.state.yml down -v || true
	docker compose -f docker-compose.traces.yml down -v || true
	docker compose -f docker-compose.tokens.yml down -v || true
	docker compose -f docker-compose.multi.yml down -v || true
	docker compose -f docker-compose.parallel.yml down -v || true
	docker compose -f docker-compose.backfill.yml down -v || true
	
	@echo "Forcibly removing any remaining containers..."
	docker ps -a | grep 'cryo-' | awk '{print $$1}' | xargs -r docker rm -f || true
	
	@echo "Deleting all Docker volumes..."
	@sleep 2  # Give Docker a moment to clean up
	docker volume ls -q | grep cryo-indexer | xargs -r docker volume rm -f || true
	
	@if [ $$(docker volume ls -q | grep cryo-indexer | wc -l) -gt 0 ]; then \
		echo "Some volumes could not be removed. Trying with force..."; \
		docker volume ls -q | grep cryo-indexer | xargs -r docker volume rm -f; \
	fi
	
	@if [ $$(docker volume ls -q | grep cryo-indexer | wc -l) -gt 0 ]; then \
		echo "WARNING: Some volumes still remain and might require manual Docker pruning"; \
		echo "You can try: docker system prune -a --volumes"; \
	else \
		echo "All volumes successfully purged. The system has been completely reset."; \
	fi