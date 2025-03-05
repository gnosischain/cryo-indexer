#!/bin/bash
set -e

# This script runs all the SQL migration files.
# Required environment variables: CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE

# Function to execute a SQL query via curl.
execute_query() {
    local sql="$1"
    local query_result

    # Skip empty statements.
    if [ -z "$(echo "$sql" | tr -d '[:space:]')" ]; then
        return 0
    fi

    # Skip comment-only statements.
    if [[ "$(echo "$sql" | grep -v '^--' | tr -d '[:space:]')" == "" ]]; then
        return 0
    fi

    # Replace the {{database}} placeholder with the actual database name.
    sql="${sql//\{\{database\}\}/$CLICKHOUSE_DATABASE}"
    
    echo "Executing SQL: ${sql:0:100}..."

    # Determine protocol based on CLICKHOUSE_SECURE.
    if [ "$CLICKHOUSE_SECURE" = "true" ]; then
        protocol="https"
    else
        protocol="http"
    fi

    # Use the port specified in the environment variable (default to 9440).
    port="${CLICKHOUSE_PORT:-9440}"

    # Capture both stdout and stderr
    local curl_output
    curl_output=$(curl -s -S -X POST "${protocol}://${CLICKHOUSE_HOST}:${port}" \
        -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-binary "$sql" 2>&1)

    # Check if curl command failed
    if [ $? -ne 0 ]; then
        echo "ERROR: Curl command failed: $curl_output"
        return 1
    fi

    query_result="$curl_output"
    echo "Result: $query_result"

    # Check for error in the response.
    if [[ "$query_result" == *"Code:"*"Exception:"* ]]; then
        echo "ERROR: Query failed with: $query_result"
        return 1
    fi

    # Return the query result (echoed).
    echo "$query_result"
    return 0
}

# Function to execute a migration file by splitting it into individual statements.
execute_file() {
    local file="$1"
    echo "Processing migration file: $file"
    
    local migration_name
    migration_name=$(basename "$file")
    
    # For files other than 001_create_database.sql, check if this migration was already executed.
    if [[ "$migration_name" != "001_create_database.sql" ]]; then
        local table_exists
        table_exists=$(execute_query "SELECT 1 FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'migrations' LIMIT 1")
        if [[ "$table_exists" == *"1"* ]]; then
            local check_result
            check_result=$(execute_query "SELECT name FROM ${CLICKHOUSE_DATABASE}.migrations WHERE name = '$migration_name' LIMIT 1")
            # Trim any surrounding whitespace.
            check_result=$(echo "$check_result" | xargs)
            if [ "$check_result" = "$migration_name" ]; then
                echo "Migration $migration_name already executed, skipping."
                return 0
            fi
        fi
    fi

    # Read file contents.
    local content
    if [ ! -f "$file" ]; then
        echo "ERROR: Migration file does not exist: $file"
        return 1
    fi
    
    content=$(cat "$file")
    # Convert Windows line endings if needed.
    content=${content//$'\r'/}

    # Split the file content into statements by semicolon.
    local IFS=$'\n'
    local statements=()
    local current_statement=""

    for line in $(echo "$content" | sed 's/;/;\n/g'); do
        if [[ "$line" == *";"* ]]; then
            # This line ends with a semicolon, complete the statement.
            current_statement="$current_statement$line"
            local clean_stmt
            clean_stmt=$(echo "$current_statement" | sed -e 's/^\s*//' -e 's/\s*$//')
            if [ -n "$clean_stmt" ]; then
                statements+=("$clean_stmt")
            fi
            current_statement=""
        else
            # Accumulate lines until we hit a semicolon.
            current_statement="$current_statement$line"$'\n'
        fi
    done

    # If any statement remains (without a trailing semicolon), add it.
    if [ -n "$current_statement" ]; then
        local clean_stmt
        clean_stmt=$(echo "$current_statement" | sed -e 's/^\s*//' -e 's/\s*$//')
        if [ -n "$clean_stmt" ]; then
            statements+=("$clean_stmt")
        fi
    fi

    # Execute each statement.
    local success=true
    for stmt in "${statements[@]}"; do
        # Skip empty or comment-only statements.
        if [[ -z "$(echo "$stmt" | grep -v '^--' | tr -d '[:space:]')" ]]; then
            continue
        fi
        
        if ! execute_query "$stmt"; then
            success=false
            break
        fi
    done

    # If all statements executed successfully, record this migration.
    if $success; then
        if [[ "$migration_name" != "001_create_database.sql" ]]; then
            local table_exists
            table_exists=$(execute_query "SELECT 1 FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'migrations' LIMIT 1")
            if [[ "$table_exists" == *"1"* ]]; then
                execute_query "INSERT INTO ${CLICKHOUSE_DATABASE}.migrations (name) VALUES ('$migration_name')"
            fi
        fi
        echo "Migration $migration_name completed successfully."
    else
        echo "Migration $migration_name failed."
        return 1
    fi
}

# Display all environment variables being used
echo "Running migrations with the following settings:"
echo "CLICKHOUSE_HOST: $CLICKHOUSE_HOST"
echo "CLICKHOUSE_PORT: ${CLICKHOUSE_PORT:-9440}"
echo "CLICKHOUSE_SECURE: ${CLICKHOUSE_SECURE:-true}"
echo "CLICKHOUSE_DATABASE: $CLICKHOUSE_DATABASE"
echo "CLICKHOUSE_USER: $CLICKHOUSE_USER (password hidden)"

# Main migration process.
echo "Starting migrations for database: \"${CLICKHOUSE_DATABASE}\""

# Check if migrations directory exists
if [ ! -d "/app/migrations" ]; then
    echo "ERROR: Migrations directory does not exist: /app/migrations"
    exit 1
fi

# List migration files
echo "Available migration files:"
ls -la /app/migrations/

# Test connection to ClickHouse before proceeding
echo "Testing connection to ClickHouse..."
if execute_query "SELECT 1"; then
    echo "ClickHouse connection successful"
else
    echo "ERROR: Could not connect to ClickHouse"
    exit 2
fi

# Ensure the database exists.
echo "Creating database if it doesn't exist..."
if ! execute_query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}"; then
    echo "ERROR: Failed to create database ${CLICKHOUSE_DATABASE}"
    exit 3
fi

# Find all .sql migration files in /app/migrations, sorted by filename.
migration_files=$(find /app/migrations -type f -name "*.sql" | sort)

# Check if any migration files were found
if [ -z "$migration_files" ]; then
    echo "ERROR: No migration files found in /app/migrations"
    exit 4
fi

echo "Found $(echo "$migration_files" | wc -l) migration files to process"

# First create migrations table if needed
echo "Creating migrations table if it doesn't exist..."
execute_query "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.migrations
(
    name String,
    executed_at DateTime DEFAULT now(),
    success UInt8 DEFAULT 1
) 
ENGINE = MergeTree()
ORDER BY name;"

# Process each migration file
for file in $migration_files; do
    echo "Processing file: $file"
    if ! execute_file "$file"; then
        echo "ERROR: Migration failed for file: $file"
        exit 5
    fi
done

echo "Migrations completed successfully!"