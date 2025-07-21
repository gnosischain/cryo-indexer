#!/bin/bash
set -e

# Print configuration
echo "Cryo Indexer - Simplified"
echo "========================="
echo "Network: ${NETWORK_NAME:-ethereum}"
echo "Operation: ${OPERATION:-continuous}"
echo "Mode: ${MODE:-minimal}"

if [ "${OPERATION}" = "historical" ] || [ "${OPERATION}" = "maintain" ]; then
    echo "Workers: ${WORKERS:-1}"
    if [ -n "${START_BLOCK}" ] && [ -n "${END_BLOCK}" ]; then
        echo "Block Range: ${START_BLOCK} to ${END_BLOCK}"
    fi
fi

if [ -n "${DATASETS}" ]; then
    echo "Custom Datasets: ${DATASETS}"
fi

echo ""

# Validate required environment variables
if [ -z "$ETH_RPC_URL" ]; then
    echo "ERROR: ETH_RPC_URL is required"
    exit 1
fi

if [ -z "$CLICKHOUSE_HOST" ]; then
    echo "ERROR: CLICKHOUSE_HOST is required"
    exit 1
fi

# Validate MODE parameter
if [[ "${MODE}" != "minimal" && "${MODE}" != "extra" && "${MODE}" != "diffs" && "${MODE}" != "full" && "${MODE}" != "custom" ]]; then
    echo "WARNING: Invalid MODE '${MODE}'. Valid options are: minimal, extra, diffs, full, custom"
    echo "Setting MODE to 'minimal'"
    export MODE="minimal"
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

# Start the indexer
echo ""
echo "Starting indexer..."
echo "=================="
exec python -m src