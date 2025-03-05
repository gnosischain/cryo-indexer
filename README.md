# Cryo Indexer

![Cryo Indexer Header](img/header-cryo-indexer.png)

A service for continuously indexing blockchain data using Cryo and storing it in ClickHouse, with support for running multiple indexers with different datasets.

## Overview

Cryo Indexer extracts blockchain data using the [Cryo](https://github.com/paradigmxyz/cryo) tool and stores it in a ClickHouse database for easy querying and analysis. It supports automatic handling of chain reorganizations and can extract various types of blockchain data such as blocks, transactions, logs, and more.

## Multi-Mode Functionality

The cryo indexer supports running multiple instances with different dataset configurations and starting blocks. This enables the following capabilities:

- Run different datasets in separate processes to balance resource usage
- Start different dataset types from different block heights
- Manage separate indexing progress for each dataset group

## Features

- Continuously index blockchain data
- Handle chain reorganizations
- Store data in ClickHouse database
- Configurable indexing options
- Multiple indexer modes with different dataset configurations
- Support for multiple datasets:
  - Blocks
  - Transactions
  - Logs
  - Contracts
  - ERC20/ERC721 transfers
  - State diffs
  - And more...

## Requirements

- Docker and Docker Compose
- An Ethereum RPC node
- ClickHouse instance

## Quick Start

1. Clone the repository
2. Copy `.env.example` to `.env` and customize it with your settings
3. Run the migrations and start the indexer:

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

- `default`: Index blocks, transactions, and logs (starting from block 0)
- `blocks`: Index only blocks (starting from block 0)
- `transactions`: Index only transactions (starting from block 0)
- `logs`: Index only event logs (starting from block 0)
- `contracts`: Index only contract creations (starting from block 0)
- `transfers`: Index only native ETH transfers (starting from block 0)
- `tx_data`: Index all transaction-related data (blocks, transactions, logs, contracts, transfers) (starting from block 0)
- `traces`: Index transaction traces (starting from block 0)
- `state_diffs`: Index state differences (balance, code, nonce, storage) (starting from block 1)
- `erc20`: Index ERC20 token transfers and metadata (starting from block 0)
- `erc721`: Index ERC721 token transfers and metadata (starting from block 0)
- `full`: Index all available data types (starting from block 0)
- `custom`: Custom indexing mode configured via DATASETS and START_BLOCK environment variables

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

### Using Make Commands

The repository includes a Makefile to simplify common operations:

- `make build` - Build Docker containers
- `make run-migrations` - Run database migrations only
- `make force-migrations` - Run migrations even if previously applied
- `make run-indexer` - Start the default indexer
- `make run-state` - Start the state diffs indexer
- `make run-traces` - Start the traces indexer
- `make run-tokens` - Start the token data indexer
- `make run-multi` - Start all specialized indexers
- `make start` - Run migrations and start the default indexer
- `make stop` - Stop the default indexer
- `make stop-multi` - Stop all specialized indexers
- `make logs` - View logs for the default indexer
- `make logs-service S=cryo-indexer-state` - View logs for a specific service
- `make clean` - Clean up all containers and volumes

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

## Troubleshooting

### Different Services Progressing at Different Rates

This is normal. Some datasets (like state diffs) require more processing time and resources than others (like blocks). Each service runs independently to prevent slower datasets from holding back faster ones.

### Resource Considerations

Running multiple indexers can be resource-intensive. You may need to adjust the memory allocations in the Docker Compose files based on your hardware capabilities.

### High RPC Usage

If your RPC provider is limiting requests:
- Adjust `MAX_BLOCKS_PER_BATCH` to a lower value
- Consider specialized RPC providers for specific data types

### State Diffs Failing Below Block 1

State diffs typically don't exist for block 0, so ensure `STATE_START_BLOCK` is set to at least 1.

### Database Connection Issues

If you're having trouble with ClickHouse connections:
- Verify your ClickHouse credentials in `.env`
- Make sure the ClickHouse instance is running and accessible
- Check the logs for detailed error messages

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

## License

This project is licensed under the [MIT License](LICENSE).