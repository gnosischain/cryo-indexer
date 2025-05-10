#!/bin/bash
set -e

# Make sure DATASETS_BACKUP is properly set
DATASETS_BACKUP="${DATASETS:-blocks,transactions,logs}"

# Print environment variables without exposing secrets
echo "Starting Cryo Indexer with the following configuration:"
echo "ETH_RPC_URL: ${ETH_RPC_URL:-not set}"
echo "CLICKHOUSE_HOST: ${CLICKHOUSE_HOST:-not set}"
echo "CLICKHOUSE_PORT: ${CLICKHOUSE_PORT:-not set}"
echo "CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE:-blockchain}"
echo "CONFIRMATION_BLOCKS: ${CONFIRMATION_BLOCKS:-20}"
echo "POLL_INTERVAL: ${POLL_INTERVAL:-15}"
echo "LOG_LEVEL: ${LOG_LEVEL:-INFO}"
echo "NETWORK_NAME: ${NETWORK_NAME:-gnosis}"
echo "START_BLOCK: ${START_BLOCK:-0}"
echo "MAX_BLOCKS_PER_BATCH: ${MAX_BLOCKS_PER_BATCH:-1000}"
echo "DATASETS: ${DATASETS_BACKUP}"
echo "OPERATION_MODE: ${OPERATION_MODE:-scraper}"
echo "INDEXER_MODE: ${INDEXER_MODE:-custom}"
echo "CHAIN_ID: ${CHAIN_ID:-100}"
echo "GENESIS_TIMESTAMP: ${GENESIS_TIMESTAMP:-1539024180}"
echo "SECONDS_PER_BLOCK: ${SECONDS_PER_BLOCK:-5}"

# Print parallel processing settings if in parallel mode
if [ "${OPERATION_MODE}" = "parallel" ]; then
    echo ""
    echo "=== TRUE MULTIPROCESS INDEXING ==="
    echo "PARALLEL_WORKERS: ${PARALLEL_WORKERS:-3} separate Python processes"
    echo "WORKER_BATCH_SIZE: ${WORKER_BATCH_SIZE:-1000} blocks per worker batch"
    
    # Warn if PARALLEL_WORKERS is too high
    if [ "${PARALLEL_WORKERS:-3}" -gt 10 ]; then
        echo "WARNING: Using a high number of worker processes (${PARALLEL_WORKERS}). This may cause excessive resource usage."
    fi
    
    echo ""
fi

# Verify that required environment variables are set
if [ -z "$ETH_RPC_URL" ]; then
    echo "ERROR: ETH_RPC_URL environment variable is required"
    exit 1
fi

if [ -z "$CLICKHOUSE_HOST" ]; then
    echo "ERROR: CLICKHOUSE_HOST environment variable is required"
    exit 1
fi

# Test ClickHouse connection
echo "Testing ClickHouse connection..."
if [ "${CLICKHOUSE_SECURE:-true}" = "true" ]; then
    protocol="https"
else
    protocol="http"
fi
port="${CLICKHOUSE_PORT:-9440}"

# Capture both stdout and stderr
connection_test=$(curl -s -S -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
    -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-binary "SELECT 1" 2>&1)

if [[ "$connection_test" != *"1"* ]]; then
    echo "ERROR: Cannot connect to ClickHouse server:"
    echo "$connection_test"
    exit 2
fi

echo "ClickHouse connection successful"

# Test Cryo installation
echo "Testing Cryo installation..."
cryo --version

# Create state directory
mkdir -p /app/state

# Create a unique state file for each indexer mode
INDEXER_ID="${INDEXER_MODE:-custom}"
MIGRATION_STATE_FILE="/app/state/migrations_applied"
INDEXER_STATE_FILE="/app/state/indexer_state_${INDEXER_ID}.json"

echo "Using state file: ${INDEXER_STATE_FILE}"

# Create the state file if it doesn't exist
if [ ! -f "$INDEXER_STATE_FILE" ]; then
    echo "Creating new state file for indexer mode: ${INDEXER_ID}"
    echo '{
       "last_block": 0,
        "last_indexed_timestamp": 0,
        "network_name": "'"${NETWORK_NAME}"'",
        "mode": "'"${INDEXER_ID}"'",
        "chain_id": '"${CHAIN_ID:-100}"',
        "genesis_timestamp": '"${GENESIS_TIMESTAMP:-1539024180}"',
        "seconds_per_block": '"${SECONDS_PER_BLOCK:-5}"'
    }' > "$INDEXER_STATE_FILE"
fi

# Function to create database if it doesn't exist
create_database() {
    echo "Creating database ${CLICKHOUSE_DATABASE} if it doesn't exist..."
    curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
        -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-binary "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"
}

# Function to run database migrations
run_migrations() {
    echo "Running database migrations..."
    
    # Create database first if it doesn't exist
    create_database
    
    # Use set +e to prevent script from exiting on migration failure
    set +e
    bash /app/scripts/run_migrations.sh
    local migration_result=$?
    set -e
    
    if [ $migration_result -eq 0 ]; then
        # Mark migrations as applied
        echo "$(date -u)" > "$MIGRATION_STATE_FILE"
        echo "Migrations completed successfully and marked as applied"
        return 0
    else
        echo "Migrations failed with exit code $migration_result"
        return $migration_result
    fi
}

# Function to check if migrations have been applied
check_migrations() {
    # First, check if the state file exists
    if [ -f "$MIGRATION_STATE_FILE" ] && [ -s "$MIGRATION_STATE_FILE" ]; then
        echo "Migrations have already been marked as applied on $(cat $MIGRATION_STATE_FILE)"
        return 0
    fi
    
    echo "No migration state file found or it's empty"
    
    # Check if the database exists
    local db_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
        -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-binary "SELECT count() FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}'")
    
    echo "Database check result: $db_result"
    
    if [[ "$db_result" == *"1"* ]]; then
        echo "Database ${CLICKHOUSE_DATABASE} exists"
        
        # Check if migrations table exists
        local table_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
            -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            --data-binary "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'migrations'")
        
        echo "Migration table check result: $table_result"
        
        if [[ "$table_result" == *"1"* ]]; then
            echo "Migrations table exists"
            
            # Check if any migrations have been applied
            local count_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
                -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
                -H "Content-Type: application/x-www-form-urlencoded" \
                --data-binary "SELECT count() FROM ${CLICKHOUSE_DATABASE}.migrations")
            
            echo "Migration count result: $count_result"
            
            if [[ "$count_result" =~ [0-9]+ ]] && [ ${count_result//[!0-9]/} -gt 0 ]; then
                echo "Database and migration table exist with ${count_result//[!0-9]/} migrations"
                echo "$(date -u)" > "$MIGRATION_STATE_FILE"
                return 0
            fi
        fi
    fi
    
    # No migrations found
    return 1
}


# Handle different operation modes
case "${OPERATION_MODE}" in
    migrations_only)
        echo "Running in migrations_only mode"
        if [ "${FORCE_RUN_MIGRATIONS:-false}" = "true" ]; then
            echo "Force running migrations regardless of previous state"
            run_migrations
            migration_exit_code=$?
            if [ $migration_exit_code -ne 0 ]; then
                echo "Migration failed with exit code $migration_exit_code"
                exit $migration_exit_code
            fi
        elif check_migrations; then
            echo "Migrations already applied. Set FORCE_RUN_MIGRATIONS=true to run again if needed."
        else
            echo "Running migrations now..."
            run_migrations
            migration_exit_code=$?
            if [ $migration_exit_code -ne 0 ]; then
                echo "Migration failed with exit code $migration_exit_code"
                exit $migration_exit_code
            fi
        fi
        echo "Migrations-only mode completed. Exiting."
        exit 0
        ;;
        
    parallel)
        echo "Running in multiprocess parallel mode with ${PARALLEL_WORKERS:-3} processes"
        # Check if migrations have been applied
        if ! check_migrations; then
            echo "WARNING: Migrations have not been applied. It's recommended to run migrations first."
            echo "You can run migrations by using 'make run-migrations'"
        fi
        
        # Start the indexer
        echo "Starting indexer in parallel mode..."
        cd /app
        
        # Set environment variables for the indexer
        export INDEXER_STATE_FILE="$INDEXER_STATE_FILE"
        export CHAIN_ID="${CHAIN_ID:-100}"
        export OPERATION_MODE="parallel"
        
        # Set multiprocessing options
        export PYTHONUNBUFFERED=1        # Ensure Python output is not buffered
        export PYTHONFAULTHANDLER=1      # Enable fault handler for better error reporting
        
        # Detect available Python command
        PYTHON_CMD="python3"
        if ! command -v python3 &> /dev/null; then
            if command -v python &> /dev/null; then
                PYTHON_CMD="python"
            else
                echo "ERROR: No Python command found"
                exit 3
            fi
        fi
        
        echo "Using ${PYTHON_CMD} command"
        echo ""
        echo "===================================================="
        echo "STARTING MULTIPROCESS INDEXER WITH ${PARALLEL_WORKERS:-3} PROCESSES"
        echo "===================================================="
        echo ""
        
        DATASETS="$DATASETS_BACKUP" INDEXER_MODE="${INDEXER_MODE:-custom}" $PYTHON_CMD -m src.indexer
        ;;
        
    scraper|*)
        echo "Running in scraper mode"
        # Check if migrations have been applied
        if ! check_migrations; then
            echo "WARNING: Migrations have not been applied. It's recommended to run migrations first."
            echo "You can run migrations by using 'make run-migrations'"
        fi
        
        # Start the indexer
        echo "Starting indexer in scraper mode..."
        cd /app
        
        # Set environment variables for the indexer
        export INDEXER_STATE_FILE="$INDEXER_STATE_FILE"
        export CHAIN_ID="${CHAIN_ID:-100}"
        
        # Detect available Python command and explicitly set environment variables
        PYTHON_CMD="python3"
        if ! command -v python3 &> /dev/null; then
            if command -v python &> /dev/null; then
                PYTHON_CMD="python"
            else
                echo "ERROR: No Python command found"
                exit 3
            fi
        fi
        
        echo "Using ${PYTHON_CMD} command"
        DATASETS="$DATASETS_BACKUP" INDEXER_MODE="${INDEXER_MODE:-custom}" $PYTHON_CMD -m src.indexer
        ;;
esac