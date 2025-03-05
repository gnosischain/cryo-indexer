# Cryo Indexer

![Cryo Indexer Header](img/header-cryo-indexer.png)

A service for continuously indexing blockchain data using Cryo and storing it in ClickHouse.

## Overview

Cryo Indexer extracts blockchain data using the [Cryo](https://github.com/paradigmxyz/cryo) tool and stores it in a ClickHouse database for easy querying and analysis. It supports automatic handling of chain reorganizations and can extract various types of blockchain data such as blocks, transactions, logs, and more.

## Features

- Continuously index blockchain data
- Handle chain reorganizations
- Store data in ClickHouse database
- Configurable indexing options
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
make start
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
START_BLOCK=0
MAX_BLOCKS_PER_BATCH=1000

# Datasets to index (comma-separated)
DATASETS=blocks,transactions,logs
```

### Using Make Commands

The repository includes a Makefile to simplify common operations:

- `make build` - Build Docker containers
- `make run-migrations` - Run database migrations
- `make force-migrations` - Run migrations even if previously applied
- `make run-indexer` - Start the indexer
- `make start` - Run migrations and start the indexer
- `make stop` - Stop all containers
- `make logs` - View logs
- `make clean` - Clean up all containers and volumes

### Using Docker Compose Directly

If you prefer to use Docker Compose directly:

1. Run migrations:
```bash
docker compose up --profile migrations cryo-migrations
```

2. Start the indexer:
```bash
docker compose up -d cryo-indexer
```

3. View logs:
```bash
docker compose logs -f
```

## Architecture

The Cryo Indexer consists of two main components:

1. **Migrations Service**: A one-time service that sets up the ClickHouse database schema
2. **Indexer Service**: A continuous service that indexes blockchain data

Both services share the same Docker image but run in different modes controlled by the `OPERATION_MODE` environment variable.

## Customization

### Adding Custom Datasets

To add custom datasets, modify the `DATASETS` environment variable:

```env
DATASETS=blocks,transactions,logs,contracts,native_transfers
```

### Restarting from a Specific Block

If you need to start indexing from a specific block:

```env
START_BLOCK=15000000
```

## Monitoring

You can monitor the indexer's progress by:

1. Checking the logs: `make logs`
2. Querying the ClickHouse database directly:

```sql
SELECT MAX(block_number) FROM blockchain.blocks;
```

## Troubleshooting

### Database Connection Issues

If you're having trouble connecting to ClickHouse, check:

1. Ensure the `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` are correct
2. Verify that the `CLICKHOUSE_PORT` and `CLICKHOUSE_SECURE` settings match your ClickHouse configuration

### Running Migrations Again

If you need to run migrations again:

```bash
make force-migrations
```

### Container Exits After Migrations

This is normal behavior for the migrations container. It's designed to run once and exit.

### Indexer Not Processing Blocks

1. Check if your RPC URL is valid and the node is synced
2. Ensure the ClickHouse database is accessible
3. Check the logs for any errors: `make logs`


## License

This project is licensed under the [MIT License](LICENSE).