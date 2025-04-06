![Cryo Indexer Header](img/header-cryo-indexer.png)

# Cryo Indexer

A service for continuously indexing blockchain data using [Cryo](https://github.com/paradigmxyz/cryo) and storing it in ClickHouse, with support for running multiple specialized indexers with different datasets.

## Overview

Cryo Indexer extracts blockchain data using the Cryo tool and stores it in a ClickHouse database for easy querying and analysis. It supports automatic handling of chain reorganizations and can extract various types of blockchain data such as blocks, transactions, logs, state diffs, and token transfers.

## Multi-Mode Functionality

The Cryo indexer supports running multiple instances with different dataset configurations and starting blocks. This enables the following capabilities:

- Run different datasets in separate processes to balance resource usage
- Start different dataset types from different block heights
- Manage separate indexing progress for each dataset group
- Backfill specific data ranges as needed

## Features

- Continuously index blockchain data
- Handle chain reorganizations automatically
- Store data in ClickHouse database with optimized schema
- Configurable indexing options
- Multiple indexer modes with different dataset configurations
- Support for multiple datasets:
  - Blocks
  - Transactions
  - Logs
  - Contracts
  - ERC20/ERC721 transfers and metadata
  - State diffs (balance, code, nonce, storage)
  - Transaction traces
  - Native token transfers
  - And more...

## Requirements

- Docker and Docker Compose
- An Ethereum RPC node with archive access
- ClickHouse instance (Cloud or self-hosted)

## Quick Start

1. Clone the repository
2. Copy `.env.example` to `.env` and customize it with your settings
3. Build the Docker image:
```bash
make build-image
```
4. Run the migrations and start the indexer:
```bash
# Start a single indexer with default settings
make start

# Or start multiple specialized indexers
make run-multi
```

## Setup

### Configuration

The indexer is configured through environment variables in your `.env` file:

```
# Ethereum RPC
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/your-api-key

# ClickHouse Configuration
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=blockchain
CLICKHOUSE_PORT=9440
CLICKHOUSE_SECURE=true

# Indexer Configuration
CONFIRMATION_BLOCKS=20
POLL_INTERVAL=15
LOG_LEVEL=INFO
CHAIN_ID=1
MAX_BLOCKS_PER_BATCH=1000
NETWORK_NAME=ethereum

# Default indexer start block
START_BLOCK=0

# Specialized indexer start blocks
BASE_START_BLOCK=0       # For blocks, transactions, logs, contracts
STATE_START_BLOCK=1      # For state diffs (should typically start at 1)
TRACES_START_BLOCK=0     # For transaction traces
TOKENS_START_BLOCK=0     # For ERC20 and ERC721 tokens

# Default indexer mode and datasets
INDEXER_MODE=default
DATASETS=blocks,transactions,logs
```

### Available Indexer Modes

The indexer supports several predefined modes:

| Mode | Description | Datasets | Default Start Block |
|------|-------------|----------|-------------------|
| `default` | Basic blockchain data | blocks, transactions, logs | 0 |
| `blocks` | Only block data | blocks | 0 |
| `transactions` | Only transaction data | transactions | 0 |
| `logs` | Only event logs | logs | 0 |
| `contracts` | Only contract creations | contracts | 0 |
| `transfers` | Only native ETH transfers | native_transfers | 0 |
| `tx_data` | All transaction-related data | blocks, transactions, logs, contracts, traces, native_transfers | 0 |
| `traces` | Transaction traces | traces | 0 |
| `state_diffs` | State differences | balance_diffs, code_diffs, nonce_diffs, storage_diffs | 1 |
| `erc20` | ERC20 token data | erc20_transfers, erc20_metadata | 0 |
| `erc721` | ERC721 token data | erc721_transfers, erc721_metadata | 0 |
| `full` | All available data types | blocks, transactions, logs, contracts, native_transfers, traces, balance_diffs, code_diffs, nonce_diffs, storage_diffs | 0 |
| `custom` | User-defined datasets | Specified via DATASETS env var | Specified via START_BLOCK env var |

### Running Multiple Indexers

You can run multiple indexers, each focused on different datasets:

```bash
# Run migrations first if needed
make run-migrations

# Start the default indexer
make run-indexer

# Start the state diffs indexer
make run-state

# Start the traces indexer
make run-traces

# Start the token data indexer
make run-tokens

# Start all specialized indexers together
make run-multi
```

The multi-mode setup includes the following indexers:

1. **cryo-indexer-base**: Indexes blocks, transactions, logs, contracts, and native transfers
2. **cryo-indexer-state**: Indexes balance, code, nonce, and storage diffs (starting at block 1 by default)
3. **cryo-indexer-traces**: Indexes transaction traces
4. **cryo-indexer-tokens**: Indexes ERC20 and ERC721 token data

Each indexer maintains its own state and data using Docker volumes, so they can progress independently.

### Database Schema

The indexer creates the following tables in your ClickHouse database:

| Table Name | Description | Datasets |
|------------|-------------|----------|
| `blocks` | Block headers | blocks |
| `transactions` | Transaction data | transactions, txs |
| `logs` | Event logs | logs, events |
| `contracts` | Contract creations | contracts |
| `native_transfers` | Native token transfers | native_transfers |
| `traces` | Transaction traces | traces |
| `balance_diffs` | Balance changes | balance_diffs |
| `code_diffs` | Contract code changes | code_diffs |
| `nonce_diffs` | Account nonce changes | nonce_diffs |
| `storage_diffs` | Contract storage changes | storage_diffs, slot_diffs |
| `migrations` | Database migration tracking | (internal) |

Tables are created using a migrations system that ensures your database schema stays in sync with the codebase.

### Using Make Commands

The repository includes a Makefile to simplify common operations:

#### Environment Setup
- `make build-image` - Build the Docker image for all services

#### Database Management
- `make run-migrations` - Run database migrations only
- `make force-migrations` - Run migrations even if previously applied
- `make debug-migrations` - Debug migrations with shell access

#### Indexer Operations
- `make run-indexer` - Start the indexer in default mode
- `make run-state` - Start the state diffs indexer
- `make run-traces` - Start the traces indexer
- `make run-tokens` - Start the token data indexer
- `make run-multi` - Start all specialized indexers
- `make start` - Run migrations and start the indexer
- `make stop` - Stop the indexer
- `make stop-multi` - Stop all specialized indexers
- `make logs` - View logs from the indexer
- `make logs-service S=<service>` - View logs for a specific service

#### Backfilling Data
- `make backfill START_BLOCK=<block> [END_BLOCK=<block>] [MODE=<mode>] [DATASETS=<datasets>]` - Run a backfill for a specific block range
- `make backfill-blocks START_BLOCK=<block> [END_BLOCK=<block>]` - Backfill only blocks
- `make backfill-transactions START_BLOCK=<block> [END_BLOCK=<block>]` - Backfill only transactions
- `make backfill-full START_BLOCK=<block> [END_BLOCK=<block>]` - Backfill all datasets
- `make clean-backfill` - Clean up backfill containers and volumes

#### Volume Management
- `make clean` - Remove all containers and volumes
- `make purge-indexer` - Delete only the default indexer volumes
- `make purge-state` - Delete only the state diffs indexer volumes
- `make purge-traces` - Delete only the traces indexer volumes
- `make purge-tokens` - Delete only the tokens indexer volumes
- `make purge-migrations` - Delete only the migrations volumes
- `make purge-all` - Delete all Docker volumes (complete reset)

Run `make help` to see all available commands and their descriptions.

## Architecture

The Cryo Indexer architecture consists of multiple components:

1. **Migrations Service**: A one-time service that sets up the ClickHouse database schema
2. **Indexer Services**: Multiple services that index different types of blockchain data
3. **State Management**: Each indexer maintains its own state file to track progress
4. **Mode System**: Predefined configurations for different data extraction needs
5. **Docker Volumes**: Each service uses dedicated Docker volumes to store data, logs, and state

Each indexer uses the same code base but is configured differently through environment variables and mode settings, allowing them to focus on specific datasets and start from appropriate block heights.

## Data Storage

The indexer uses Docker named volumes for all data storage:

1. **Data Volumes**: Store the extracted blockchain data (e.g., `state_data`, `traces_data`)
2. **Logs Volumes**: Store log files (e.g., `state_logs`, `traces_logs`)
3. **State Volumes**: Store state files that track indexing progress (e.g., `state_state`, `traces_state`)

This approach keeps all data inside Docker's managed storage, avoiding permission issues while still ensuring persistence between container restarts.

## Data Flow

1. **Data Extraction**: Cryo extracts blockchain data for the specified blocks and datasets
2. **Data Storage**: The data is stored in the container's data volume
3. **Database Import**: The indexer loads data into ClickHouse tables
4. **Progress Tracking**: Each indexer maintains its own state file to remember where it left off
5. **Reorg Handling**: If a chain reorganization is detected, affected data is rolled back

## Monitoring

You can monitor each indexer's progress by:

1. Checking the logs for each service:
   ```bash
   make logs
   make logs-service S=cryo-indexer-state
   ```

2. Querying the ClickHouse database:
   ```sql
   -- Check blocks table progress
   SELECT MAX(block_number) FROM blockchain.blocks;
   
   -- Check state diffs progress
   SELECT MAX(block_number) FROM blockchain.storage_diffs;
   ```

## Customizing Start Blocks

You can customize the start block for each indexer type in your `.env` file:

```
# Start all indexers from a specific block
START_BLOCK=15000000
BASE_START_BLOCK=15000000
STATE_START_BLOCK=15000000
TRACES_START_BLOCK=15000000
TOKENS_START_BLOCK=15000000
```

## Backfilling Specific Ranges

You can backfill specific block ranges for targeted datasets:

```bash
# Backfill blocks 1000000-1100000 with all data
make backfill START_BLOCK=1000000 END_BLOCK=1100000 MODE=full

# Backfill only blocks and transactions for a range
make backfill START_BLOCK=1000000 END_BLOCK=1100000 DATASETS=blocks,transactions

# Use specialized backfill commands
make backfill-blocks START_BLOCK=1000000 END_BLOCK=1100000
make backfill-transactions START_BLOCK=1000000 END_BLOCK=1100000
```

Backfill operations run as one-time services that exit when complete, making them ideal for filling gaps in your data.

## Troubleshooting

### Different Services Progressing at Different Rates

This is normal. Some datasets (like state diffs) require more processing time and resources than others (like blocks). Each service runs independently to prevent slower datasets from holding back faster ones.

### Resource Considerations

Running multiple indexers can be resource-intensive. You may need to adjust the memory allocations in the Docker Compose files based on your hardware capabilities.

### High RPC Usage

If your RPC provider is limiting requests:
- Adjust `MAX_BLOCKS_PER_BATCH` to a lower value
- Consider specialized RPC providers for specific data types
- Check the logs for rate limiting messages

### State Diffs Failing Below Block 1

State diffs typically don't exist for block 0, so ensure `STATE_START_BLOCK` is set to at least 1.

### Database Connection Issues

If you're having trouble with ClickHouse connections:
- Verify your ClickHouse credentials in `.env`
- Make sure the ClickHouse instance is running and accessible
- Check the logs for detailed error messages
- Ensure your firewall allows connections to the ClickHouse port

### Checking Individual Service Status

To see the status of a specific indexer:

```bash
docker ps | grep cryo-indexer
```

### Migrations Issues

If you encounter problems with migrations:
- Run `make force-migrations` to force migrations to run again
- Check the migration logs for specific errors: `docker logs cryo-migrations`
- Use `make debug-migrations` to get a shell inside the migrations container for troubleshooting

### Handling Incomplete or Failed Runs

If an indexer stops unexpectedly:
- Check logs with `make logs-service S=<service_name>`
- You can safely restart the service with the same command used to start it
- The indexer will pick up where it left off using its state file

### Docker Volume Issues

If you encounter problems with Docker volumes:
- Use `make purge-all` to completely reset all volumes (warning: this will delete all indexed data)
- For targeted cleanup, use the specific volume purge commands like `make purge-indexer`
- Check Docker volume status with `docker volume ls | grep cryo-indexer`

## License

This project is licensed under the [MIT License](LICENSE).