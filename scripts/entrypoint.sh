#!/bin/bash
set -e

# Print environment variables without exposing secrets
echo "Starting Cryo Indexer with the following configuration:"
echo "ETH_RPC_URL: ${ETH_RPC_URL:-not set}"
echo "CLICKHOUSE_HOST: ${CLICKHOUSE_HOST:-not set}"
echo "CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE:-blockchain}"
echo "CONFIRMATION_BLOCKS: ${CONFIRMATION_BLOCKS:-20}"
echo "POLL_INTERVAL: ${POLL_INTERVAL:-15}"
echo "LOG_LEVEL: ${LOG_LEVEL:-INFO}"
echo "CHAIN_ID: ${CHAIN_ID:-1}"
echo "START_BLOCK: ${START_BLOCK:-0}"
echo "MAX_BLOCKS_PER_BATCH: ${MAX_BLOCKS_PER_BATCH:-1000}"
echo "DATASETS: ${DATASETS:-blocks,transactions,logs}"
echo "RUN_MIGRATIONS: ${RUN_MIGRATIONS:-true}"

# Verify that required environment variables are set
if [ -z "$ETH_RPC_URL" ]; then
    echo "ERROR: ETH_RPC_URL environment variable is required"
    exit 1
fi

if [ -z "$CLICKHOUSE_HOST" ]; then
    echo "ERROR: CLICKHOUSE_HOST environment variable is required"
    exit 1
fi

# Test Cryo installation
echo "Testing Cryo installation..."
cryo --version

# Run database migrations if enabled
if [ "${RUN_MIGRATIONS:-true}" = "true" ]; then
    echo "Running database migrations..."
    bash /app/scripts/run_migrations.sh
fi

# Run the indexer
echo "Starting indexer..."
cd /app
python -m src.indexer