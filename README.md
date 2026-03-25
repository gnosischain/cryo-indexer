# Cryo Indexer

![Cryo Indexer](img/header-cryo-indexer.png)

A blockchain indexer using [Cryo](https://github.com/paradigmxyz/cryo) for data extraction and ClickHouse for storage. Designed for Gnosis Chain with support for historical bulk indexing, real-time continuous ingestion, and a near-real-time live database.

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Operations](#operations)
- [Indexing Modes](#indexing-modes)
- [Live Database](#live-database)
- [Configuration](#configuration)
- [Running with Docker Compose](#running-with-docker-compose)
- [Running with Makefile](#running-with-makefile)
- [Database Schema](#database-schema)
- [State Management](#state-management)
- [Observability](#observability)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Quick Start

```bash
# 1. Clone and build
git clone <repository>
cd cryo-indexer
make build

# 2. Configure
cp .env.example .env
# Edit .env with your RPC URL and ClickHouse settings

# 3. Run migrations
make run-migrations

# 4. Start indexing
make start
```

## Architecture

```
                    ┌──────────────┐
                    │  Blockchain  │
                    │     RPC      │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │     Cryo     │
                    │  Extractor   │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
     ┌────────▼───┐  ┌────▼─────┐  ┌──▼──────────┐
     │ execution  │  │execution │  │ indexing     │
     │ (historical│  │  _live   │  │ _state      │
     │  database) │  │(realtime)│  │ (tracking)  │
     └────────────┘  └──────────┘  └─────────────┘
```

**Two databases, one codebase:**
- **`execution`** — Historical data indexed in 100-block ranges. Complete blockchain archive.
- **`execution_live`** — Near-real-time data (10-block ranges, ~50s latency). 2-day TTL, auto-cleaned.

### Key Principles

1. **Blocks First** — Always process blocks before other datasets (timestamps required)
2. **Mode-Independent State** — `indexing_state` tracks by `(dataset, start_block, end_block)` only, no mode partitioning
3. **Self-Healing** — Auto-maintain recovers stuck ranges, fixes gaps, detects silent failures
4. **Withdrawals Auto-Populated** — Block processing automatically inserts withdrawal records

## Operations

| Operation | Purpose | Safe While Running? |
|-----------|---------|-------------------|
| **`continuous`** | Follow chain tip in real-time | Primary operation |
| **`historical`** | Bulk index a specific block range | Yes |
| **`maintain`** | Fix gaps, failed/stuck ranges | **No** — stop scrapers first |
| **`auto-maintain`** | Periodic self-healing (every 4h) | **Yes** — uses claim_range() |
| **`validate`** | Check indexing progress (read-only) | Yes |

### Continuous

Follows the chain tip with fixed-size ranges. Automatically resumes from the last completed block.

```bash
make continuous                          # Default mode (custom)
make continuous MODE=full                # All datasets
make continuous MODE=minimal             # blocks, transactions, logs only
```

### Historical

Fast parallel bulk indexing of a specific block range.

```bash
make historical START_BLOCK=1000000 END_BLOCK=2000000
make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8
make historical START_BLOCK=1000000 END_BLOCK=2000000 MODE=full WORKERS=16
```

### Maintain

Fixes data integrity issues. **Requires all scrapers to be stopped.**

Finds and reprocesses:
- Gaps (ranges with no entry in indexing_state)
- Failed ranges
- Stuck processing ranges (>2 hours old)
- Deletes existing data (including withdrawals for blocks) before re-indexing

```bash
make maintain                                        # All ranges
make maintain START_BLOCK=1000000 END_BLOCK=2000000  # Specific range
make maintain WORKERS=16                             # Parallel
```

### Auto-Maintain

Periodic self-healing that is **safe to run alongside the continuous indexer**. Scans the last 48 hours of data.

Detects and fixes:
- Gaps in recent data
- Failed ranges
- Stuck processing ranges (marks as failed after 2h)
- Zero-row completed ranges for blocks/transactions/logs/traces (silent failures)

Uses `claim_range()` for concurrency safety — skips ranges currently being processed.

```bash
make auto-maintain
make auto-maintain AUTO_MAINTAIN_LOOKBACK_HOURS=72 WORKERS=8
```

### Validate

Read-only check of indexing progress across all datasets.

```bash
make validate                                        # Check all
make status                                          # Alias
```

## Indexing Modes

Controls which datasets are indexed. Mode does **not** affect state tracking.

| Mode | Datasets | Use Case |
|------|----------|----------|
| **custom** (default) | blocks, transactions, logs, contracts, native_transfers, traces | Standard deployment |
| **minimal** | blocks, transactions, logs | Lightweight / live DB |
| **extra** | contracts, native_transfers, traces | Supplementary data |
| **diffs** | balance_diffs, code_diffs, nonce_diffs, storage_diffs | State change analysis |
| **full** | All 10 datasets | Complete archive |

```bash
# Custom datasets
make continuous MODE=custom DATASETS=blocks,transactions,logs,traces
```

## Live Database

A separate `execution_live` database for near-real-time data with automatic 2-day TTL cleanup.

| Feature | Historical (`execution`) | Live (`execution_live`) |
|---------|------------------------|------------------------|
| Batch size | 100 blocks (~8 min) | 10 blocks (~50 sec) |
| Confirmation | 120+ blocks | 6 blocks (~30 sec) |
| Datasets | All configured | blocks, transactions, logs |
| Retention | Permanent | 2-day TTL |
| Partitioning | Monthly | Daily |

### Setup

```bash
# 1. Run live migrations (creates execution_live DB with TTL tables)
make run-migrations-live

# 2. Start live indexer (set START_BLOCK near chain tip)
make live LIVE_START_BLOCK=45300000
```

### Unified Read Path

The merge of `execution` and `execution_live` is handled in the dbt layer (separate repo). Historical takes precedence in the overlap window.

## Configuration

### Required
| Variable | Description |
|----------|-------------|
| `ETH_RPC_URL` | Blockchain RPC endpoint |
| `CLICKHOUSE_HOST` | ClickHouse host |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |

### Core
| Variable | Default | Description |
|----------|---------|-------------|
| `NETWORK_NAME` | ethereum | Network name for Cryo |
| `CLICKHOUSE_USER` | default | ClickHouse username |
| `CLICKHOUSE_DATABASE` | blockchain | Target database |
| `CLICKHOUSE_PORT` | 8443 | ClickHouse port |
| `CLICKHOUSE_SECURE` | true | Use HTTPS |

### Operation
| Variable | Default | Description |
|----------|---------|-------------|
| `OPERATION` | continuous | continuous, historical, maintain, auto-maintain, validate |
| `MODE` | minimal | minimal, extra, diffs, full, custom |
| `DATASETS` | blocks,transactions,logs | Custom dataset list (when MODE=custom) |
| `START_BLOCK` | 0 | Starting block (0 = resume from DB state) |
| `END_BLOCK` | 0 | Ending block (for historical/maintain) |

### Performance
| Variable | Default | Description |
|----------|---------|-------------|
| `WORKERS` | 1 | Parallel workers (use 4-16 for historical) |
| `BATCH_SIZE` | 100 | Blocks per range |
| `MAX_RETRIES` | 3 | Max retry attempts per dataset |
| `REQUESTS_PER_SECOND` | 20 | RPC rate limit |
| `MAX_CONCURRENT_REQUESTS` | 2 | Concurrent RPC requests |
| `CRYO_TIMEOUT` | 600 | Cryo command timeout (seconds) |

### Continuous Mode
| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIRMATION_BLOCKS` | 12 | Blocks to wait before indexing |
| `POLL_INTERVAL` | 10 | Seconds between chain tip checks |

### Auto-Maintain
| Variable | Default | Description |
|----------|---------|-------------|
| `AUTO_MAINTAIN_LOOKBACK_HOURS` | 48 | How far back to scan |
| `STUCK_RANGE_TIMEOUT_HOURS` | 2 | When to consider processing ranges stuck |

## Running with Docker Compose

### Environment Setup

```bash
cat > .env << 'EOF'
ETH_RPC_URL=https://gnosis.gateway.tenderly.co/YOUR_KEY
NETWORK_NAME=gnosis
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=execution
CLICKHOUSE_PORT=443
CLICKHOUSE_SECURE=true
MODE=custom
DATASETS=blocks,transactions,logs,contracts,native_transfers,traces
EOF
```

### Build and Run

```bash
# Build
docker compose build

# Run migrations
docker compose --profile migrations up migrations

# Continuous indexing
docker compose up cryo-indexer-custom

# Historical (one-shot)
docker compose --profile historical up historical-job

# Maintenance
docker compose --profile maintain up maintain-job

# Validate
docker compose --profile validate up validate-job

# Auto-maintain
docker compose run \
  -e OPERATION=auto-maintain \
  -e MODE=custom \
  -e DATASETS=blocks,transactions,logs,contracts,native_transfers,traces \
  -e WORKERS=4 \
  maintain-job

# Live indexer
make run-migrations-live
LIVE_START_BLOCK=45300000 docker compose up cryo-indexer-live
```

### Available Services

| Service | Description |
|---------|-------------|
| `cryo-indexer-minimal` | Continuous: blocks, transactions, logs |
| `cryo-indexer-extra` | Continuous: contracts, native_transfers, traces |
| `cryo-indexer-diffs` | Continuous: state diffs |
| `cryo-indexer-full` | Continuous: all datasets |
| `cryo-indexer-custom` | Continuous: DATASETS env var |
| `cryo-indexer-live` | Live: blocks, transactions, logs → execution_live |
| `historical-job` | One-shot historical indexing |
| `maintain-job` | One-shot maintenance |
| `validate-job` | One-shot validation |
| `migrations` | Database migrations |

## Running with Makefile

### Setup
```bash
make build              # Build Docker image
make run-migrations     # Run migrations (execution DB)
make run-migrations-live # Run migrations (execution_live DB)
```

### Core Operations
```bash
make continuous                                       # Follow chain tip
make historical START_BLOCK=1000000 END_BLOCK=2000000 # Bulk index
make maintain                                         # Fix integrity (stop scrapers first)
make auto-maintain                                    # Self-healing (safe while running)
make validate                                         # Check progress
make status                                           # Alias for validate
make live LIVE_START_BLOCK=45300000                    # Near-real-time indexer
```

### Mode Shortcuts
```bash
make minimal        # blocks, transactions, logs
make extra          # contracts, native_transfers, traces
make diffs          # state diffs
make full           # everything
make custom DATASETS=blocks,transactions,traces
```

### Management
```bash
make stop           # Stop all containers
make logs           # Tail logs
make clean          # Remove containers + volumes
make shell          # Open container shell
```

## Database Schema

### Data Tables
| Table | Description | Partitioning |
|-------|-------------|-------------|
| `blocks` | Block headers, metadata | Monthly |
| `transactions` | Transaction data | Monthly |
| `logs` | Event logs | Monthly |
| `contracts` | Contract creations | Monthly |
| `native_transfers` | ETH/xDAI transfers | Monthly |
| `traces` | Execution traces | Monthly |
| `balance_diffs` | Balance changes | Monthly |
| `code_diffs` | Code changes | Monthly |
| `nonce_diffs` | Nonce changes | Monthly |
| `storage_diffs` | Storage changes | Monthly |
| `withdrawals` | Validator withdrawals (auto-populated from blocks) | Monthly |

### State Tables
| Table | Description |
|-------|-------------|
| `indexing_state` | Tracks range completion: `ORDER BY (dataset, start_block, end_block)` |
| `migrations` | Tracks executed migrations |

### Views
| View | Description |
|------|-------------|
| `indexing_progress` | Aggregated progress per dataset |
| `continuous_ranges` | Continuous completed range segments |
| `indexing_summary` | Combined progress + continuity |

All data tables use `ReplacingMergeTree(insert_version)` for deduplication.

## State Management

### How It Works

- **Mode-independent**: State is tracked by `(dataset, start_block, end_block)` only. No mode column in queries.
- **Status flow**: `claim (processing)` → `completed` or `failed`
- **Concurrency**: `claim_range()` checks status before claiming. If already completed or processing, returns False.
- **Withdrawals**: When blocks are deleted (maintain), withdrawals for that range are also deleted via `delete_range_with_related()`.

### indexing_state Table

```sql
ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (dataset, start_block, end_block)
-- No mode column in the sort key
```

## Observability

### Prometheus Metrics (port 9090)

| Metric | Type | Description |
|--------|------|-------------|
| `cryo_blocks_indexed_total` | Counter | Ranges indexed by dataset |
| `cryo_blocks_failed_total` | Counter | Ranges failed by dataset |
| `cryo_range_duration_seconds` | Histogram | Processing time per range |
| `cryo_cryo_extract_duration_seconds` | Histogram | Cryo extraction time |
| `cryo_clickhouse_insert_duration_seconds` | Histogram | ClickHouse insert time |
| `cryo_rows_inserted_total` | Counter | Rows inserted by dataset |
| `cryo_highest_completed_block` | Gauge | Highest completed block |
| `cryo_chain_head_block` | Gauge | Latest block from RPC |
| `cryo_chain_lag_blocks` | Gauge | Blocks behind chain head |
| `cryo_auto_maintain_recovered_total` | Counter | Ranges recovered by reason |

### Health Endpoint

```bash
curl http://localhost:9090/health
# {"status": "ok", "clickhouse_connected": true, "rpc_connected": true, ...}

curl http://localhost:9090/metrics
# Prometheus text format
```

### Grafana Dashboard

Import `dashboards/cryo-indexer-observability.json` into Grafana for panels covering:
- Chain lag, replicas, restarts
- Range processing rate, p95 duration, failure rate
- Cryo extraction time, ClickHouse insert time, rows/sec
- Auto-maintain recovery stats
- CPU/memory usage
- Error logs (via Loki)

## Troubleshooting

### Ranges stuck as 'processing'

Auto-maintain handles this automatically (marks as failed after 2 hours). To fix manually:

```bash
make auto-maintain STUCK_RANGE_TIMEOUT_HOURS=1
```

### False gaps reported by validate

This was caused by mode-dependent state tracking (fixed in v2). If you see gaps that shouldn't exist, check if they were indexed under a different mode in the old `indexing_state_old` table.

### Blocks missing valid timestamps

Process blocks before other datasets:
```bash
make historical START_BLOCK=X END_BLOCK=Y MODE=custom DATASETS=blocks
make maintain
```

### Zero-row ranges keep getting reprocessed

Only `blocks`, `transactions`, `logs`, and `traces` are checked for zero-row anomalies. Datasets like `contracts` can legitimately have empty ranges (no contracts created in that block range).

### Live indexer starts from old block

Set `LIVE_START_BLOCK` near the current chain tip:
```bash
make live LIVE_START_BLOCK=45300000
```

After the first run, it resumes from DB state automatically on restart.

## Repository Structure

```
cryo-indexer/
├── Dockerfile
├── Makefile
├── README.md
├── docker-compose.yml
├── requirements.txt
├── migrations/                    # Migrations for execution DB
│   ├── 001_create_database.sql
│   ├── 002-013_*.sql             # Data tables + withdrawals
│   ├── 014a_create_indexing_state_v2.sql  # New mode-free state table
│   └── 014b_migrate_indexing_state_data.sql  # Backfill + swap
├── migrations_live/               # Migrations for execution_live DB
│   ├── 001_create_database.sql
│   ├── 002-004_*.sql             # blocks/transactions/logs with 2-day TTL
│   ├── 013_create_withdrawals.sql
│   └── 014_create_indexing_state.sql
├── dashboards/
│   └── cryo-indexer-observability.json
├── scripts/
│   └── entrypoint.sh
└── src/
    ├── __main__.py
    ├── config.py                  # Settings + operation types
    ├── indexer.py                 # Main orchestrator (5 operations)
    ├── worker.py                  # Range processing with strict timestamps
    ├── observability.py           # Prometheus metrics + health + JSON logging
    ├── core/
    │   ├── blockchain.py          # RPC client
    │   ├── state_manager.py       # Mode-independent state tracking
    │   └── utils.py
    └── db/
        ├── clickhouse_manager.py  # ClickHouse ops + delete_range_with_related
        ├── clickhouse_pool.py
        └── migrations.py
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment
export ETH_RPC_URL=your_rpc_url
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PASSWORD=password

# Run locally
python -m src

# Test with small range
make test-range START_BLOCK=18000000
```

## License

This project is licensed under the [MIT License](LICENSE).
