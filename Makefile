.PHONY: build run-migrations run-indexer stop-indexer logs clean help force-migrations

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
	@echo "  make run-indexer      - Start the indexer in scraper mode"
	@echo "  make start            - Run migrations and start the indexer"
	@echo "  make stop             - Stop the indexer"
	@echo "  make logs             - View logs from the indexer"
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

run-indexer:
	@echo "Starting the indexer in scraper mode..."
	docker compose up -d

start: run-migrations run-indexer
	@echo "Indexer started with migrations applied."

stop:
	@echo "Stopping the indexer..."
	docker compose down
	docker compose -f docker-compose.migrations.yml down

logs:
	@echo "Showing indexer logs..."
	docker compose logs -f --tail=500

clean:
	@echo "Cleaning up containers and volumes..."
	docker compose down -v
	docker compose -f docker-compose.migrations.yml down -v
	rm -rf data/*
	rm -rf state/*
	rm -rf logs/*