.PHONY: build run-migrations run-indexer stop-indexer logs clean help force-migrations run-state run-traces run-tokens run-multi stop-multi

# Default environment file
ENV_FILE := .env

# Load environment variables from .env file if it exists
ifneq (,$(wildcard $(ENV_FILE)))
	include $(ENV_FILE)
	export
endif

help:
	@echo "Cryo Indexer Commands:"
	@echo "  make build            - Build the Docker containers"
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

build:
	@echo "Building Docker containers..."
	docker compose build

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