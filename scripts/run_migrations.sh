#!/bin/bash
set -e

# This script runs all the SQL migration files
# Required environment variables: CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE

# Function to execute SQL query
execute_query() {
    local sql=$1
    local query_result

    # Replace {{database}} placeholder with the actual database name
    sql="${sql//\{\{database\}\}/$CLICKHOUSE_DATABASE}"
    
    echo "Executing SQL: ${sql:0:100}..." # Log first 100 chars of the query

    # Uses curl to execute ClickHouse query
    if [ "$CLICKHOUSE_SECURE" = "true" ]; then
        protocol="https"
    else
        protocol="http"
    fi

    query_result=$(curl -s -X POST "${protocol}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}" \
        -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        --data-binary "$sql")
    
    echo "Result: $query_result"
}

# Function to execute SQL file
execute_file() {
    local file=$1
    echo "Processing migration file: $file"
    
    # Read the SQL file content
    sql=$(cat "$file")
    
    # Execute the SQL
    execute_query "$sql"
}

# Create database if it doesn't exist
execute_query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"

# Main migration process
echo "Starting migrations for database: ${CLICKHOUSE_DATABASE}"

# Get all SQL files in the migrations directory, sorted
migration_files=$(find /app/migrations -name "*.sql" | sort)

# Execute each migration file
for file in $migration_files; do
    execute_file "$file"
done

echo "Migrations completed successfully!"