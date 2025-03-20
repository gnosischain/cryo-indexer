.PHONY: build run-migrations run-indexer stop-indexer logs clean help force-migrations run-state run-traces run-tokens run-multi stop-multi purge-volumes purge-all purge-state purge-indexer purge-migrations build-image

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

help:
	@echo "Cryo Indexer Commands:"
	@echo "  make build-image      - Build a single Docker image for all services"
	@echo "  make run-migrations   - Run database migrations only"
	@echo "  make force-migrations - Run database migrations, even if already applied"
	@echo "  make run-indexer      - Start the indexer in default mode"
	@echo "  make run-state        - Start the state diffs indexer"
	@echo "  make run-traces       - Start the traces indexer"
	@echo "  make run-tokens       - Start the token data indexer"
	@echo "  make run-multi        - Start all specialized indexers"
	@echo "  make start            - Run migrations and start the indexer"
	@echo "  make stop             - Stop the default indexer"
	@echo "  make stop-multi       - Stop all specialized indexers"
	@echo "  make logs             - View logs from the indexer"
	@echo "  make logs-service S=<service> - View logs for a specific service"
	@echo "  make clean            - Remove all containers and volumes"
	@echo ""
	@echo "Volume Management Commands:"
	@echo "  make purge-all         - Delete all Docker volumes (complete reset)"
	@echo "  make purge-indexer     - Delete only the default indexer volumes"
	@echo "  make purge-state       - Delete only the state diffs indexer volumes"
	@echo "  make purge-migrations  - Delete only the migrations volumes"

build-image:
	@echo "Building a single Docker image for all services..."
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "Image $(IMAGE_NAME):$(IMAGE_TAG) built successfully."

run-migrations:
	@echo "Running database migrations..."
	docker compose -f docker-compose.migrations.yml up --abort-on-container-exit

force-migrations:
	@echo "Forcing database migrations to run..."
	FORCE_RUN_MIGRATIONS=true docker compose -f docker-compose.migrations.yml up --abort-on-container-exit

debug-migrations:
	@echo "Debugging migrations with shell access..."
	docker compose -f docker-compose.migrations.yml run --rm --entrypoint /bin/bash cryo-migrations

run-indexer:
	@echo "Starting the indexer in default mode..."
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

run-range:
	@echo "Running indexer with block range..."
	@if [ -z "$(START_BLOCK)" ] || [ -z "$(END_BLOCK)" ]; then \
		echo "ERROR: START_BLOCK and END_BLOCK must be specified"; \
		echo "Usage: make run-range START_BLOCK=<start_block> END_BLOCK=<end_block> [INDEXER_MODE=<mode>]"; \
		exit 1; \
	fi
	START_BLOCK=$(START_BLOCK) END_BLOCK=$(END_BLOCK) INDEXER_MODE=$(INDEXER_MODE) docker-compose up

start: run-migrations run-indexer
	@echo "Indexer started with migrations applied."

stop:
	@echo "Stopping the indexer..."
	docker compose down

stop-multi:
	@echo "Stopping all specialized indexers..."
	docker compose -f docker-compose.multi.yml down

logs:
	@echo "Showing indexer logs..."
	docker compose logs -f --tail=500

logs-service:
	@if [ -z "$(S)" ]; then \
		echo "Please specify a service with S=<service_name>"; \
		echo "Example: make logs-service S=cryo-indexer-state"; \
		exit 1; \
	fi
	@echo "Showing logs for $(S)..."
	docker logs -f $(S) --tail=500

clean:
	@echo "Cleaning up containers and volumes..."
	docker compose down -v
	docker compose -f docker-compose.migrations.yml down -v
	docker compose -f docker-compose.state.yml down -v
	docker compose -f docker-compose.traces.yml down -v
	docker compose -f docker-compose.tokens.yml down -v
	docker compose -f docker-compose.multi.yml down -v
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