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
echo "CHAIN_ID: ${CHAIN_ID:-1}"
echo "START_BLOCK: ${START_BLOCK:-0}"
echo "MAX_BLOCKS_PER_BATCH: ${MAX_BLOCKS_PER_BATCH:-1000}"
echo "DATASETS: ${DATASETS_BACKUP}"
echo "OPERATION_MODE: ${OPERATION_MODE:-scraper}"

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

# Migration state file to track if migrations have been applied
MIGRATION_STATE_FILE="/app/state/migrations_applied"

# Function to check if ClickHouse database and migration table exists
check_database_exists() {
    echo "Checking if database and migrations exist..."
    
    # Check if database exists
    local db_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
        -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-binary "SELECT count() FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}'")
    
    echo "Database check result: $db_result"
    
    if [[ "$db_result" == *"1"* ]]; then
        echo "Database ${CLICKHOUSE_DATABASE} exists"
        
        # Database exists, check if migrations table exists
        local table_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
            -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            --data-binary "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'migrations'")
        
        echo "Migration table check result: $table_result"
        
        if [[ "$table_result" == *"1"* ]]; then
            echo "Migrations table exists"
            
            # Migrations table exists, check if any migrations have been applied
            local count_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
                -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
                -H "Content-Type: application/x-www-form-urlencoded" \
                --data-binary "SELECT count() FROM ${CLICKHOUSE_DATABASE}.migrations")
            
            echo "Migration count result: $count_result"
            
            if [[ "$count_result" =~ [0-9]+ ]] && [ ${count_result//[!0-9]/} -gt 0 ]; then
                echo "Database and migration table exist with ${count_result//[!0-9]/} migrations"
                return 0
            else
                echo "Migrations table exists but is empty"
            fi
        else
            echo "Migrations table does not exist"
        fi
    else
        echo "Database ${CLICKHOUSE_DATABASE} does not exist"
    fi
    
    return 1
}

# Function to run database migrations
run_migrations() {
    echo "Running database migrations..."
    
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
    
    # If state file doesn't exist or is empty, check the database
    if check_database_exists; then
        # If database has migrations, create the state file
        echo "$(date -u)" > "$MIGRATION_STATE_FILE"
        echo "Database already has migrations - marking as applied"
        return 0
    fi
    
    # No state file and no migrations in database
    echo "No migrations have been applied"
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
        
    scraper)
        echo "Running in scraper mode"
        # Check if migrations have been applied
        if ! check_migrations; then
            echo "WARNING: Migrations have not been applied. It's recommended to run migrations first."
            echo "You can run migrations by using 'make run-migrations'"
        fi
        
        # Start the indexer
        echo "Starting indexer in scraper mode..."
        cd /app
        
        # Detect available Python command and explicitly set DATASETS environment variable
        if command -v python3 &> /dev/null; then
            echo "Using python3 command"
            DATASETS="$DATASETS_BACKUP" python3 -m src.indexer
        elif command -v python &> /dev/null; then
            echo "Using python command"
            DATASETS="$DATASETS_BACKUP" python -m src.indexer
        else
            echo "ERROR: No Python command found"
            exit 3
        fi
        ;;
        
    *)
        echo "ERROR: Unknown operation mode: ${OPERATION_MODE}"
        echo "Supported modes: scraper, migrations_only"
        exit 1
        ;;
esac