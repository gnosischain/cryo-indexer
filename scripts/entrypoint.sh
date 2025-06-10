#!/bin/bash
set -e

# Print configuration
echo "Cryo Indexer Configuration:"
echo "=========================="
echo "Network: ${NETWORK_NAME:-gnosis}"
echo "Operation: ${OPERATION:-continuous}"
echo "Mode: ${MODE:-default}"
echo "Workers: ${WORKERS:-1}"
echo "Batch Size: ${BATCH_SIZE:-1000}"

if [ "${OPERATION}" = "historical" ]; then
    echo "Start Block: ${START_BLOCK}"
    echo "End Block: ${END_BLOCK}"
fi

if [ -n "${DATASETS}" ]; then
    echo "Custom Datasets: ${DATASETS}"
fi

echo ""

# Verify required environment variables
if [ -z "$ETH_RPC_URL" ]; then
    echo "ERROR: ETH_RPC_URL is required"
    exit 1
fi

if [ -z "$CLICKHOUSE_HOST" ]; then
    echo "ERROR: CLICKHOUSE_HOST is required"
    exit 1
fi

# Test ClickHouse connection
echo "Testing ClickHouse connection..."
if [ "${CLICKHOUSE_SECURE:-true}" = "true" ]; then
    protocol="https"
else
    protocol="http"
fi

curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT:-8443}" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-binary "SELECT 1" > /dev/null

if [ $? -eq 0 ]; then
    echo "ClickHouse connection successful"
else
    echo "ERROR: Cannot connect to ClickHouse"
    exit 2
fi

# Create required directories
mkdir -p /app/data /app/logs

# Run migrations if requested
if [ "${RUN_MIGRATIONS}" = "true" ]; then
    echo "Running database migrations..."
    python -m src.migrations
fi

# Start the indexer
echo ""
echo "Starting indexer..."
echo "=================="
exec python -m src.indexer