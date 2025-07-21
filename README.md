# Cryo Indexer 

![Cryo Indexer](img/header-cryo-indexer.png)

A blockchain indexer using [Cryo](https://github.com/paradigmxyz/cryo) for data extraction and ClickHouse for storage.

## Table of Contents

- [Quick Start](#quick-start)
- [Operations Overview](#operations-overview)
- [Repository Structure](#repository-structure)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Operation Modes](#operation-modes)
- [Indexing Modes](#indexing-modes)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Running with Docker](#running-with-docker)
- [Running with Makefile](#running-with-makefile)
- [State Management](#state-management)
- [Monitoring & Validation](#monitoring--validation)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)
- [Use Case Examples](#use-case-examples)
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

## Operations Overview

The simplified indexer has **3 core operations** designed for clarity and reliability:

| Operation | Purpose | When to Use | Key Features |
|-----------|---------|-------------|--------------|
| **`continuous`** | Real-time blockchain following | Production systems, live data | Polls chain tip, handles reorgs, automatic recovery |
| **`historical`** | Fast bulk indexing of specific ranges | Initial sync, catching up, research | Parallel processing, progress tracking, efficient batching |
| **`maintain`** | Process failed/pending ranges | After failures, fixing incomplete data | Retry failed ranges, process pending work |

### Operation Decision Tree

```
What do you need to do?

🔄 Real-time blockchain following?
└─ Use: continuous

📥 Download specific block range?
├─ Fresh/empty database?
│   └─ Use: historical (most efficient)
└─ Know exact range needed?
    └─ Use: historical

🔧 Fix failed or incomplete data?
├─ Ranges marked as 'failed'?
├─ Ranges stuck as 'pending'?
└─ Use: maintain
```

### Current Implementation vs Roadmap

| Feature | Status | Notes |
|---------|--------|-------|
| `continuous` operation | ✅ Implemented | Real-time blockchain following |
| `historical` operation | ✅ Implemented | Parallel bulk indexing |
| `maintain` operation | ✅ Implemented | Processes failed/pending ranges from state table |
| Automatic gap detection | ❌ Not implemented | Roadmap feature |
| Automatic timestamp fixing | ❌ Not implemented | Roadmap feature |
| Data validation | ✅ Basic validation | Checks for valid timestamps during processing |

## Repository Structure

```
cryo-indexer/
├── Dockerfile                      # Container build configuration
├── LICENSE                        # MIT License
├── Makefile                       # Simplified build and run commands
├── README.md                      # This file
├── data/                          # Local data directory (mounted as volume)
├── docker-compose.yml             # Simplified Docker Compose configuration
├── img/
│   └── header-cryo-indexer.png   # Header image
├── migrations/                    # Database schema migrations
│   ├── 001_create_database.sql
│   ├── 002_create_blocks.sql
│   ├── 003_create_transactions.sql
│   ├── 004_create_logs.sql
│   ├── 005_create_contracts.sql
│   ├── 006_create_native_transfers.sql
│   ├── 007_create_traces.sql
│   ├── 008_create_balance_diffs.sql
│   ├── 009_create_code_diffs.sql
│   ├── 010_create_nonce_diffs.sql
│   ├── 011_create_storage_diffs.sql
│   └── 012_create_indexing_state.sql  # Simplified state management
├── requirements.txt               # Python dependencies
├── scripts/
│   └── entrypoint.sh             # Simplified container entrypoint
└── src/                          # Main application code
    ├── __init__.py
    ├── __main__.py               # Simplified application entry point
    ├── config.py                 # Streamlined configuration (15 settings vs 40+)
    ├── indexer.py                # Main indexer with 3 operations
    ├── worker.py                 # Simplified worker with strict timestamp requirements
    ├── core/                     # Core functionality
    │   ├── __init__.py
    │   ├── blockchain.py         # Blockchain client for RPC calls
    │   ├── state_manager.py      # Simplified state management
    │   └── utils.py              # Utility functions
    └── db/                       # Database components
        ├── __init__.py
        ├── clickhouse_manager.py # Simplified ClickHouse operations
        ├── clickhouse_pool.py    # Connection pooling
        └── migrations.py         # Migration runner
```

## Key Features

### Simplified Design
- **3 Operations**: Down from 8+ complex operations
- **15 Settings**: Down from 40+ configuration options
- **Single State Table**: Simplified from multiple state tracking tables
- **Fail-Fast**: Clear error messages, immediate failure on issues

### Reliability Features
- **Strict Timestamps**: Blocks must have valid timestamps before processing other datasets
- **Atomic Processing**: Complete ranges or fail entirely
- **Automatic Recovery**: Self-healing from crashes and network issues
- **Parallel Processing**: Efficient multi-worker historical indexing

### Performance Improvements
- **Optimized Batching**: Smart batch sizes for different operations
- **Reduced Overhead**: Simpler code paths, less verification complexity
- **Better Resource Usage**: Eliminated redundant operations
- **Faster Startup**: Simpler initialization and state checking

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Blockchain│────▶│    Cryo     │────▶│ ClickHouse  │
│     RPC     │     │  Extractor  │     │  Database   │
└─────────────┘     └─────────────┘     └─────────────┘
                           │                     ▲
                           ▼                     │
                    ┌─────────────┐              │
                    │ Simplified  │──────────────┘
                    │   Worker    │
                    └─────────────┘
```

### Architecture Principles

1. **Blocks First**: Always process blocks before other datasets
2. **Strict Validation**: Fail immediately if timestamps are missing
3. **Single Source of Truth**: Only `indexing_state` table for all state
4. **Clear Separation**: Operations don't overlap in functionality
5. **Atomic Operations**: Complete ranges or rollback entirely

### How It Works

1. **Main Indexer** (`indexer.py`) orchestrates one of 3 operations
2. **Simplified Worker** (`worker.py`) processes ranges with strict timestamp requirements
3. **State Manager** (`state_manager.py`) uses single table for all state tracking
4. **ClickHouse Manager** handles database operations with fail-fast validation

## Operation Modes

### 1. Continuous (Default)
**Real-time blockchain following and indexing**

**Use Case**: Production deployment for real-time data  
**Behavior**: 
- Polls for new blocks every `POLL_INTERVAL` seconds (default: 10s)
- Waits `CONFIRMATION_BLOCKS` (default: 12) before indexing to avoid reorgs
- Processes in small batches (default: 100 blocks) for reliability
- Automatically resumes from last indexed block on restart
- Self-healing: resets stale jobs on startup

**When to Use**:
- ✅ Production systems requiring up-to-date blockchain data
- ✅ Real-time analytics and monitoring
- ✅ DeFi applications needing fresh transaction data
- ✅ After completing historical sync

**Reliability Features**:
- Single-threaded for stability
- Small batch sizes prevent memory issues  
- Automatic stale job cleanup
- Graceful shutdown handling

```bash
# Basic continuous indexing
make continuous

# Continuous with specific mode  
make continuous MODE=full

# Start from specific block
make continuous START_BLOCK=18000000
```

### 2. Historical
**Fast bulk indexing of specific block ranges**

**Use Case**: Initial data loading, catching up, or selective range processing  
**Behavior**:
- Downloads exactly what you specify (start to end block)
- Supports parallel processing with multiple workers
- Automatically divides work into optimal batch sizes
- Built-in progress tracking and ETA calculations
- Strict timestamp validation at each step

**When to Use**:
- ✅ Initial sync of blockchain data
- ✅ Indexing specific periods (e.g., "DeFi Summer 2020")  
- ✅ Catching up after downtime
- ✅ Selective data extraction for research
- ✅ Fresh start with empty database
- ✅ You know exactly what range you need

**Performance Features**:
- Parallel workers for speed
- Optimized batch sizes
- Built-in load balancing
- Progress monitoring

```bash
# Basic historical range
make historical START_BLOCK=1000000 END_BLOCK=2000000

# Parallel processing (recommended for large ranges)
make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8

# Conservative settings for rate-limited RPCs
make historical START_BLOCK=1000000 END_BLOCK=1100000 \
  WORKERS=2 BATCH_SIZE=100 REQUESTS_PER_SECOND=10
```

### 3. Maintain
**Process failed and pending ranges from state table**

**Use Case**: Fix incomplete or failed indexing work  
**Behavior**:
- **Scans State Table**: Looks for ranges marked as 'failed' or 'pending'
- **Retry Processing**: Re-attempts failed ranges with proper error handling
- **Clear Pending**: Processes ranges that were never attempted
- **Progress Reporting**: Shows what was fixed and any remaining issues
- **State-Driven**: Only processes what the state table indicates needs work

**When to Use**:
- ✅ After system failures or crashes
- ✅ Network interruptions during indexing
- ✅ RPC failures that left work incomplete
- ✅ Ranges stuck in 'pending' state
- ✅ Periodic maintenance to clear failed work

**What It Fixes**:
- Ranges marked as 'failed' in indexing_state
- Ranges stuck as 'pending' that were never processed
- Worker crashes that left ranges incomplete

**What It Does NOT Do** (Roadmap Features):
- Automatic gap detection between completed ranges
- Timestamp correction from invalid dates
- Data validation across all tables
- State reconstruction from existing data

```bash
# Process all failed/pending ranges
make maintain

# Process issues for specific range
make maintain START_BLOCK=1000000 END_BLOCK=2000000

# Parallel maintenance
make maintain WORKERS=4
```

#### Maintain Operation Flow

1. **Scan Phase**: Queries `indexing_state` table for failed/pending ranges
2. **Report Phase**: Shows what issues were found
3. **Retry Phase**: Re-processes each failed/pending range individually
4. **Complete Phase**: Marks successfully processed ranges as completed

### 4. Validate (Read-Only)
**Check indexing progress and data integrity**

```bash
# Check overall progress
make status

# Validate specific range
make status START_BLOCK=1000000 END_BLOCK=2000000
```

## Indexing Modes

Choose what blockchain data to extract and index:

### Mode Comparison Table

| Mode | Datasets | Use Case | Performance | Storage (per 1M blocks) |
|------|----------|-----------|-------------|-------------------------|
| **Minimal** | `blocks`, `transactions`, `logs` | Standard DeFi/DApp analysis | Fast | ~50GB |
| **Extra** | `contracts`, `native_transfers`, `traces` | Additional contract & trace data | Moderate | ~100GB |
| **Diffs** | `balance_diffs`, `code_diffs`, `nonce_diffs`, `storage_diffs` | State change analysis | Slow | ~200GB |
| **Full** | All 10 datasets | Complete blockchain analysis | Slowest | ~500GB |
| **Custom** | User-defined | Tailored to specific needs | Variable | Variable |

### Minimal Mode (Default)
**Standard DeFi/DApp analysis setup**

**Datasets**: `blocks`, `transactions`, `logs`  
**Perfect for**:
- DeFi protocols analysis
- Transaction monitoring  
- Event log processing
- Token transfer tracking

```bash
make continuous MODE=minimal
```

### Extra Mode
**Additional blockchain data**

**Datasets**: `contracts`, `native_transfers`, `traces`  
**Perfect for**:
- Contract deployment tracking
- Native ETH transfers
- Internal transaction analysis
- MEV research

```bash
make continuous MODE=extra
```

### Diffs Mode  
**State change tracking**

**Datasets**: `balance_diffs`, `code_diffs`, `nonce_diffs`, `storage_diffs`  
**Perfect for**:
- State analysis
- Account balance tracking
- Storage slot monitoring
- Contract code changes

```bash
make continuous MODE=diffs
```

### Full Mode
**Complete blockchain analysis**

**Datasets**: All 10 datasets  
**Perfect for**:
- Complete audit trails
- Academic research
- Forensic analysis
- Comprehensive blockchain indexing

```bash
make continuous MODE=full
```

### Custom Mode
**Tailored dataset selection**

```bash
# MEV analysis
make continuous MODE=custom DATASETS=blocks,transactions,logs,traces,native_transfers

# Contract focus  
make continuous MODE=custom DATASETS=blocks,transactions,contracts

# State tracking
make continuous MODE=custom DATASETS=blocks,balance_diffs,storage_diffs
```

## Configuration

### Essential Settings
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ETH_RPC_URL` | Blockchain RPC endpoint | - | ✅ |
| `CLICKHOUSE_HOST` | ClickHouse host | - | ✅ |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | - | ✅ |

### Core Settings  
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `NETWORK_NAME` | Network name for Cryo | ethereum | Most networks supported |
| `CLICKHOUSE_USER` | ClickHouse username | default | Usually default |
| `CLICKHOUSE_DATABASE` | Database name | blockchain | Auto-created |
| `CLICKHOUSE_PORT` | ClickHouse port | 8443 | Standard for ClickHouse Cloud |
| `CLICKHOUSE_SECURE` | Use HTTPS | true | Recommended |

### Operation Settings
| Variable | Description | Default | Options |
|----------|-------------|---------|---------|
| `OPERATION` | Operation mode | continuous | continuous, historical, maintain, validate |
| `MODE` | Indexing mode | minimal | minimal, extra, diffs, full, custom |
| `START_BLOCK` | Starting block number | 0 | For historical/maintain |
| `END_BLOCK` | Ending block number | 0 | For historical/maintain |

### Performance Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `WORKERS` | Number of parallel workers | 1 | Use 4-16 for historical |
| `BATCH_SIZE` | Blocks per batch | 100 | Smaller = more reliable |
| `MAX_RETRIES` | Max retry attempts | 3 | Exponential backoff |

### RPC Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `REQUESTS_PER_SECOND` | RPC requests per second | 20 | Conservative default |
| `MAX_CONCURRENT_REQUESTS` | Concurrent RPC requests | 2 | Prevent overload |
| `CRYO_TIMEOUT` | Cryo command timeout (seconds) | 600 | Increase for slow networks |

### Continuous Mode Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `CONFIRMATION_BLOCKS` | Blocks to wait for confirmation | 12 | Reorg protection |
| `POLL_INTERVAL` | Polling interval (seconds) | 10 | How often to check for new blocks |

## Database Schema

The indexer automatically creates these tables in ClickHouse:

### Core Tables
- **`blocks`** - Block headers and metadata with strict timestamp requirements
- **`transactions`** - Transaction data including gas, value, status
- **`logs`** - Event logs from smart contracts
- **`contracts`** - Contract creation data
- **`native_transfers`** - ETH/native token transfers
- **`traces`** - Detailed transaction execution traces

### State Diff Tables
- **`balance_diffs`** - Account balance changes
- **`code_diffs`** - Smart contract code changes
- **`nonce_diffs`** - Account nonce changes
- **`storage_diffs`** - Contract storage changes

### Management Tables
- **`indexing_state`** - Single source of truth for all indexing state
- **`indexing_progress`** - Real-time progress view (materialized view)
- **`migrations`** - Database migration tracking

## Running with Docker

### Prerequisites
- Docker and Docker Compose installed
- ClickHouse instance (local or cloud)
- Blockchain RPC endpoint

### Environment Setup
Create a `.env` file:

```bash
# Required settings
ETH_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
CLICKHOUSE_HOST=your-clickhouse-host.com
CLICKHOUSE_PASSWORD=your-password

# Optional settings (smart defaults)
NETWORK_NAME=ethereum
CLICKHOUSE_DATABASE=blockchain
WORKERS=4
BATCH_SIZE=100
MODE=minimal
```

### Build and Run

```bash
# Build the image
docker-compose build

# Run migrations
docker-compose --profile migrations up migrations

# Start continuous indexing
docker-compose up cryo-indexer-minimal

# Historical indexing (one-shot job)
OPERATION=historical START_BLOCK=18000000 END_BLOCK=18100000 \
docker-compose --profile historical up historical-job

# Maintenance (fix failed/pending ranges)
docker-compose --profile maintain up maintain-job
```

### Running Different Modes
```bash
# Minimal mode (default)
docker-compose up cryo-indexer-minimal

# Extra mode (contracts, transfers, traces)
docker-compose up cryo-indexer-extra

# Diffs mode (state changes)
docker-compose up cryo-indexer-diffs

# Full mode (everything)
docker-compose up cryo-indexer-full

# Custom mode
DATASETS=blocks,transactions,logs,traces \
docker-compose up cryo-indexer-custom
```

## Running with Makefile

### Setup Commands
```bash
make build          # Build Docker image
make run-migrations # Run database migrations
```

### Core Operations
```bash
# Continuous indexing
make continuous     # Start real-time indexing
make start          # Alias for continuous

# Historical indexing
make historical START_BLOCK=1000000 END_BLOCK=2000000
make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8

# Maintenance (process failed/pending ranges)
make maintain       # Fix failed/pending ranges from state table
```

### Quick Operations
```bash
# Different modes
make minimal        # Start minimal mode
make full          # Start full mode
make custom DATASETS=blocks,transactions,traces

# Testing
make test-range START_BLOCK=18000000  # Test with 1000 blocks
```

### Monitoring
```bash
make logs           # View logs
make status         # Check indexing status (uses validate operation)
make ps            # Container status
```

### Utilities
```bash
make stop          # Stop indexer
make clean         # Remove containers and volumes
make shell         # Open container shell
```

## State Management

### Indexing State Tracking
- **Single Source of Truth**: Only the `indexing_state` table is used
- **Fixed Range Size**: 1000-block ranges for predictable processing
- **Simple Statuses**: `pending` → `processing` → `completed` or `failed`
- **Atomic Operations**: Ranges are completed entirely or not at all
- **Automatic Cleanup**: Stale jobs are reset on startup

### State Model
```sql
indexing_state table:
- mode, dataset, start_block, end_block (composite key)
- status: pending | processing | completed | failed
- worker_id, attempt_count, created_at, completed_at
- rows_indexed, error_message
```

### Stale Job Handling
- **On Startup**: All 'processing' jobs are reset to 'pending'
- **No Complex Monitoring**: Simple state-based detection
- **Self-Healing**: System automatically recovers from crashes

## Monitoring & Validation

### Progress Monitoring
```bash
# Check overall progress (uses validate operation)
make status

# View detailed logs
make logs

# Monitor container status
make ps
```

### Validation Reports
The validate operation provides progress reports:

```
=== INDEXING PROGRESS ===

blocks:
  Completed ranges: 1250
  Processing ranges: 0
  Failed ranges: 0
  Pending ranges: 0
  Highest attempted: 18125000
  Total rows: 125,000,000

transactions:
  Completed ranges: 1248
  Processing ranges: 0
  Failed ranges: 1
  Pending ranges: 1
  Highest attempted: 18125000
  Total rows: 45,230,123

=== MAINTENANCE NEEDED ===

transactions: 2 ranges need attention
  Failed range: blocks 18124000-18125000 (1000 blocks)
  Pending range: blocks 18120000-18121000 (1000 blocks)

💡 Run 'make maintain' to process these ranges
```

### Real-time Monitoring
During operations, the system provides clear progress updates:

```
Historical Progress: 45/116 ranges (38.8%) | ✓ 43 | ✗ 2 | Rate: 12.5 ranges/min | ETA: 5.7 min
Maintain Progress: Fixed 12/15 ranges | ✓ 10 | ✗ 2 | Processed: 80%
```

## Performance Tuning

### Simplified Tuning Guidelines

1. **Start with Defaults**: The defaults are optimized for reliability
2. **Scale Workers for Historical**: Use 4-16 workers for large historical jobs
3. **Keep Batches Small**: 100-block batches prevent memory issues
4. **Respect RPC Limits**: Conservative defaults prevent rate limiting

### Operation-Specific Tuning

#### Continuous Mode (Optimized for Reliability)
```bash
# Production settings
WORKERS=1                    # Single worker for stability
BATCH_SIZE=100              # Small batches for low latency
REQUESTS_PER_SECOND=20      # Conservative rate
CONFIRMATION_BLOCKS=12      # Standard reorg protection
POLL_INTERVAL=10           # Regular polling
```

#### Historical Mode (Optimized for Speed)
```bash
# Fast historical (good RPC)
WORKERS=16                  # High parallelism
BATCH_SIZE=500             # Larger batches for efficiency
REQUESTS_PER_SECOND=50     # Higher rate

# Conservative historical (rate-limited RPC)  
WORKERS=4                   # Moderate parallelism
BATCH_SIZE=100             # Reliable batch size
REQUESTS_PER_SECOND=20     # Respect limits
```

#### Maintain Mode (Balanced)
```bash
# Default settings are usually optimal
WORKERS=4                   # Parallel issue fixing
BATCH_SIZE=100             # Reliable processing
```

## Troubleshooting

### Common Issues

#### "Blocks missing valid timestamps"
**Cause**: Trying to process other datasets before blocks are indexed  
**Solution**: 
```bash
# Process blocks first
make historical START_BLOCK=X END_BLOCK=Y MODE=custom DATASETS=blocks
# Then process other datasets
make maintain
```

#### Ranges stuck as 'failed' or 'pending'
**Cause**: Worker crashes, network issues, or processing failures  
**Solution**:
```bash
# Use maintain operation to retry these ranges
make maintain
```

#### Slow Historical Processing
**Solutions**:
```bash
# Increase workers and batch size
make historical START_BLOCK=X END_BLOCK=Y WORKERS=8 BATCH_SIZE=200

# For rate-limited RPCs
make historical START_BLOCK=X END_BLOCK=Y WORKERS=2 REQUESTS_PER_SECOND=10
```

#### RPC Rate Limits
**Solution**: Reduce requests per second and workers
```bash
REQUESTS_PER_SECOND=10 WORKERS=2 make historical
```

#### Out of Memory
**Solution**: Reduce batch size and workers
```bash
BATCH_SIZE=50 WORKERS=2 make historical
```

### Error Resolution Workflow

1. **Check logs**: `make logs`
2. **Check status**: `make status`
3. **Process failed/pending**: `make maintain`
4. **If issues persist**: Reduce batch size/workers and retry

## Use Case Examples

### Complete Deployment Scenarios

#### 1. Fresh Setup (New Deployment)
```bash
# Setup
make build
make run-migrations

# Historical sync  
make historical START_BLOCK=18000000 END_BLOCK=latest WORKERS=8

# Switch to continuous
make continuous
```

#### 2. Existing Data (Recovery from Issues)
```bash
# Setup
make build  
make run-migrations

# Process any failed/pending ranges
make maintain

# Verify everything is working
make status

# Start continuous indexing
make continuous
```

#### 3. Research Project (Specific Time Period)
```bash
# Target specific historical period
make historical START_BLOCK=12000000 END_BLOCK=13000000 MODE=full

# Check for any incomplete ranges
make maintain

# Analyze specific events
make historical START_BLOCK=12500000 END_BLOCK=12600000 \
  MODE=custom DATASETS=blocks,transactions,logs
```

#### 4. Maintenance and Recovery
```bash
# Regular health check and fix failed/pending ranges
make maintain

# After system issues - this processes incomplete work
make maintain

# Verify all issues resolved  
make status
```

#### 5. Debugging Data Issues
```bash
# Check what's wrong
make status

# Fix incomplete ranges in specific area
make maintain START_BLOCK=1000000 END_BLOCK=2000000
```

### Development and Testing

#### Local Development
```bash
# Test with small range
make test-range START_BLOCK=18000000

# Test different modes
make historical START_BLOCK=18000000 END_BLOCK=18001000 MODE=minimal
make historical START_BLOCK=18000000 END_BLOCK=18001000 MODE=full

# Test maintenance
make maintain START_BLOCK=18000000 END_BLOCK=18001000
```

#### Performance Testing
```bash
# Benchmark different settings
time make historical START_BLOCK=18000000 END_BLOCK=18010000 WORKERS=4
time make historical START_BLOCK=18010000 END_BLOCK=18020000 WORKERS=8
```

## Development

### Local Development Setup

1. **Install Dependencies**
```bash
pip install -r requirements.txt
```

2. **Set Environment Variables**
```bash
export ETH_RPC_URL=your_rpc_url
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PASSWORD=password
```

3. **Run Locally**  
```bash
python -m src
```

### Testing
```bash
# Test with small range
make test-range START_BLOCK=18000000

# Test maintenance
make maintain START_BLOCK=18000000 END_BLOCK=18001000

# Validate results
make status
```

## Roadmap Features

The following features are mentioned in the codebase but not yet implemented:

### Automatic Gap Detection
- Detect missing ranges between completed blocks
- Smart gap identification beyond simple state table scanning
- Cross-table validation to find data inconsistencies

### Automatic Timestamp Fixing
- Detect and fix invalid '1970-01-01' timestamps
- Join with blocks table to correct timestamp issues
- Batch timestamp correction operations

### Enhanced Validation
- Deep data validation across all tables
- Completeness checks beyond state table
- Data consistency verification

### State Reconstruction
- Rebuild indexing_state from existing data tables
- Recover from corrupted state tracking
- Automatic state healing

## License

This project is licensed under the [MIT License](LICENSE)