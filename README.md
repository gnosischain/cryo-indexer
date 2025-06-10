![Cryo Indexer](img/header-cryo-indexer.png)

# Cryo Indexer 

A clean, stateless blockchain indexer using [Cryo](https://github.com/paradigmxyz/cryo) for data extraction and ClickHouse for storage. All state is managed in the database, making it robust and scalable.

## Table of Contents

- [Quick Start](#quick-start)
- [Operations Quick Reference](#operations-quick-reference)
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
# Edit .env with your settings

# 3. Run migrations
make run-migrations

# 4. Start indexing
make start
```

## Operations Quick Reference

Understanding when to use each operation is crucial for efficient blockchain data management:

| Operation | Purpose | How it decides what to do | When to use |
|-----------|---------|---------------------------|-------------|
| **`continuous`** | Real-time indexing | Follows chain tip automatically | Production systems, live data feeds |
| **`historical`** | Download specific ranges | You specify exact start/end blocks | Fresh indexing, known ranges, research |
| **`fill-gaps`** | Fix missing data | Smart gap detection in your database | After crashes, network issues, maintenance |
| **`validate`** | Check data integrity | Analyzes completeness and reports issues | Health checks, troubleshooting |
| **`backfill`** | Create state tracking | Scans existing data tables | Migrating from other indexers, corrupted state |

### Operation Decision Tree

```
What do you need to do?

📊 Check data health?
└─ Use: validate

🔄 Real-time blockchain following?
└─ Use: continuous

📥 Download specific block range?
├─ Fresh/empty database?
│   └─ Use: historical (most efficient)
└─ Have some data, missing pieces?
    └─ Use: fill-gaps (smart detection)

🗃️ Have existing blockchain data?
├─ Missing indexing_state entries?
│   └─ Use: backfill (creates state tracking)
└─ Data has gaps/holes?
    └─ Use: fill-gaps (finds and fills missing data)
```

### Key Differences: Fill-Gaps vs Historical vs Backfill

| Aspect | Fill-Gaps | Historical | Backfill |
|--------|-----------|------------|----------|
| **Data Source** | Downloads from blockchain | Downloads from blockchain | Scans existing database |
| **Intelligence** | High - finds gaps automatically | Low - downloads what you specify | High - analyzes existing data |
| **Efficiency** | High - only missing data | Medium - may duplicate work | Highest - no downloads |
| **Use Case** | Fix incomplete data | Get new/specific ranges | Track existing data |

**Examples:**
- **Fill-gaps**: "I have blocks 1M-1.5M and 1.8M-2M, fix the gap"
- **Historical**: "Download blocks 2M-3M exactly as specified"  
- **Backfill**: "I have data in my tables, create state tracking for it"

## Repository Structure

```
cryo-indexer/
├── Dockerfile                      # Container build configuration
├── LICENSE                        # MIT License
├── Makefile                       # Build and run commands
├── README.md                      # This file
├── data/                          # Local data directory (mounted as volume)
├── docker-compose.yml             # Docker Compose configuration
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
│   └── 012_create_indexing_state.sql
├── requirements.txt               # Python dependencies
├── scripts/
│   └── entrypoint.sh             # Container entrypoint script
├── src/                          # Main application code
│   ├── __init__.py
│   ├── __main__.py               # Application entry point
│   ├── config.py                 # Configuration management
│   ├── indexer.py                # Main indexer orchestrator
│   ├── worker.py                 # Individual worker implementation
│   ├── backfill_worker.py        # Backfill operation worker
│   ├── core/                     # Core functionality
│   │   ├── __init__.py
│   │   ├── blockchain.py         # Blockchain client for RPC calls
│   │   ├── state_manager.py      # Database state management
│   │   ├── utils.py              # Utility functions
│   │   └── worker_pool.py        # Multi-threaded worker pool
│   └── db/                       # Database components
│       ├── __init__.py
│       ├── clickhouse_manager.py # ClickHouse database operations
│       ├── clickhouse_pool.py    # Connection pooling
│       └── migrations.py         # Migration runner
```

## Key Features

- **Stateless**: All state stored in ClickHouse, no local state files
- **Simple**: Just 5 operation modes, 4 indexing modes
- **Scalable**: Built-in parallel processing support
- **Robust**: Automatic retry and gap detection
- **Cloud-Ready**: Optimized for ClickHouse Cloud with minimal RAM usage
- **Configurable**: Extensive environment variable configuration
- **Resume-Safe**: Can safely restart and resume indexing from any point
- **Migration-Friendly**: Backfill operation for existing data

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Blockchain│────▶│    Cryo     │────▶│ ClickHouse  │
│     RPC     │     │  Extractor  │     │  Database   │
└─────────────┘     └─────────────┘     └─────────────┘
                           │                     ▲
                           ▼                     │
                    ┌─────────────┐              │
                    │   Worker    │──────────────┘
                    │  Processes  │
                    └─────────────┘
```

### How It Works

1. **Main Indexer** (`indexer.py`) orchestrates the entire process
2. **Workers** (`worker.py`) process individual block ranges using Cryo
3. **Backfill Worker** (`backfill_worker.py`) analyzes existing data and creates state entries
4. **State Manager** (`state_manager.py`) tracks progress in ClickHouse with hybrid gap detection
5. **Worker Pool** (`worker_pool.py`) manages parallel execution
6. **ClickHouse Manager** handles all database operations

## Operation Modes

The indexer supports 5 distinct operation modes, each designed for specific use cases:

### 1. Continuous (Default)
**Real-time blockchain following and indexing**

**Use Case**: Production deployment for real-time data  
**Behavior**: 
- Polls for new blocks every `POLL_INTERVAL` seconds (default: 10s)
- Waits `CONFIRMATION_BLOCKS` (default: 12) before indexing to avoid reorgs
- Automatically resumes from last indexed block on restart
- Handles chain reorganizations gracefully

**When to Use**:
- ✅ Production systems requiring up-to-date blockchain data
- ✅ Real-time analytics and monitoring
- ✅ DeFi applications needing fresh transaction data
- ✅ After completing historical sync

**Setup Requirements**:
- Stable RPC connection
- ClickHouse Cloud or persistent storage
- Sufficient bandwidth for real-time processing

```bash
# Basic continuous indexing
make continuous

# Continuous with specific mode
make continuous MODE=full

# Start from specific block (useful after historical sync)
make continuous START_BLOCK=18000000
```

### 2. Historical
**Bulk indexing of specific block ranges**

**Use Case**: Initial data loading, catching up, or selective range processing  
**Behavior**:
- Downloads exactly what you specify (start to end block)
- Supports parallel processing with multiple workers
- Automatically divides work into optimal batch sizes
- Provides progress tracking and ETA calculations
- May overwrite existing data in the specified range

**When to Use**:
- ✅ Initial sync of blockchain data
- ✅ Indexing specific periods (e.g., "DeFi Summer 2020")
- ✅ Catching up after downtime
- ✅ Selective data extraction for research
- ✅ Fresh start with empty database
- ✅ You know exactly what range you need

**Setup Requirements**:
- Archive node access (for old blocks)
- High-bandwidth RPC connection
- Sufficient resources for parallel processing

```bash
# Basic historical range
make historical START_BLOCK=1000000 END_BLOCK=2000000

# Parallel processing (recommended for large ranges)
make historical START_BLOCK=1000000 END_BLOCK=2000000 WORKERS=8

# Conservative settings for rate-limited RPCs
make historical START_BLOCK=1000000 END_BLOCK=1100000 \
  WORKERS=2 BATCH_SIZE=100 REQUESTS_PER_SECOND=10
```

### 3. Fill Gaps
**Smart detection and filling of missing data**

**Use Case**: Data integrity maintenance and recovery  
**Behavior**:
- Analyzes `indexing_state` to identify missing block ranges automatically
- Uses hybrid detection: checks database state first, falls back to actual table data
- Only processes actually missing data (no unnecessary re-indexing)
- Supports parallel gap filling
- Skips already-indexed ranges automatically

**When to Use**:
- ✅ After system failures or crashes
- ✅ Network interruptions during indexing
- ✅ RPC failures that left gaps
- ✅ Periodic data integrity checks
- ✅ You have partial data with scattered missing pieces
- ✅ Don't know exactly which ranges are missing

**Smart Detection**:
The system uses intelligent gap detection:
1. **Primary**: Checks `indexing_state` table (fast)
2. **Fallback**: If 80%+ appears missing, checks actual table data
3. **Hybrid**: Combines both methods for accuracy

```bash
# Check and fill all gaps automatically
make fill-gaps

# Fill gaps in specific range
make fill-gaps START_BLOCK=1000000 END_BLOCK=2000000

# Parallel gap filling
make fill-gaps WORKERS=4
```

### 4. Validate
**Data completeness checking and progress reporting**

**Use Case**: Monitoring, auditing, and verification  
**Behavior**:
- Analyzes `indexing_state` for progress statistics
- Identifies gaps and missing ranges
- Reports stale jobs and processing issues
- Provides detailed dataset-by-dataset breakdown
- No data downloading - read-only analysis

**When to Use**:
- ✅ Regular health checks
- ✅ Before and after major operations
- ✅ Troubleshooting indexing issues
- ✅ Audit and compliance reporting
- ✅ Performance monitoring

```bash
# Full validation
make validate

# Validate specific range
make validate START_BLOCK=1000000 END_BLOCK=2000000
```

### 5. Backfill
**Populate indexing_state from existing blockchain data**

**Use Case**: Migrating existing data or recovering from state corruption  
**Behavior**:
- Scans existing ClickHouse tables (`blocks`, `transactions`, etc.)
- Identifies continuous block ranges in your data
- Creates appropriate `indexing_state` entries
- Memory-efficient chunked processing
- Validates results automatically
- No blockchain data downloading - works with existing data only
- With `BACKFILL_FORCE=true`, deletes existing state entries before scanning

**When to Use**:
- ✅ **Migration**: You have existing blockchain data from other indexers
- ✅ **Recovery**: `indexing_state` table was corrupted or lost
- ✅ **Integration**: Importing data from CSV, Parquet, or other sources
- ✅ **Audit**: Ensuring state tracking matches actual data
- ✅ **Before fill-gaps**: When gap detection shows everything as missing
- ✅ **Fixing incorrect state**: Use `BACKFILL_FORCE=true` to refresh state

#### How Backfill Works

The backfill operation follows these steps:

1. **Table Scan**: For each dataset, checks if the corresponding table exists
2. **Range Detection**: If `BACKFILL_FORCE=true` and specific range provided, deletes existing state entries first
3. **Data Analysis**: Scans the table to find continuous block ranges
4. **State Creation**: Creates `indexing_state` entries for each continuous range found
5. **Validation**: Verifies that the created state covers all existing data

**Example Flow**:
```
Your blocks table has data:
- Blocks 1000-5000 (continuous)
- Blocks 8000-9000 (continuous)
- Block 9500 (single block)

Backfill will create indexing_state entries:
- Entry 1: blocks 1000-5000, status=completed
- Entry 2: blocks 8000-9000, status=completed  
- Entry 3: blocks 9500-9501, status=completed

Gap detection will then find:
- Gap 1: blocks 5001-7999
- Gap 2: blocks 9001-9499
```

**Critical for Existing Data**:
If you have pre-existing blockchain data, run backfill FIRST:

```bash
# Step 1: Scan existing data and create state entries
make backfill

# Step 2: Verify the backfill worked correctly  
make validate

# Step 3: Now fill-gaps will work correctly
make fill-gaps
```

#### Backfill Options

**Basic Backfill** (entire dataset):
```bash
make backfill
```

**Range-Specific Backfill**:
```bash
# Only scan and backfill specific range
make backfill START_BLOCK=1000000 END_BLOCK=2000000
```

**Force Mode** (delete and recreate state):
```bash
# Delete existing state entries and rescan
make backfill BACKFILL_FORCE=true

# Force mode with specific range
make backfill BACKFILL_FORCE=true START_BLOCK=1000000 END_BLOCK=2000000
```

#### BACKFILL_FORCE Behavior

When `BACKFILL_FORCE=true` is set:

1. **With specific range** (`START_BLOCK` and `END_BLOCK` provided):
   - Deletes ALL existing `indexing_state` entries that overlap with the specified range
   - Scans only the specified range in tables
   - Creates new entries based on what's found

2. **Without range** (full backfill):
   - Scans entire tables to find data ranges
   - Deletes existing entries only for the found ranges
   - Creates fresh entries

This is useful for:
- Fixing corrupted state entries
- Cleaning up after manual data deletion
- Ensuring state matches actual data

**Example: Fixing Deleted Data**
```bash
# You deleted blocks 40374070-40374470 from tables
# But indexing_state still shows them as "completed"

# Fix it with force backfill:
make backfill BACKFILL_FORCE=true START_BLOCK=40374070 END_BLOCK=40374470

# This will:
# 1. Delete the incorrect state entries
# 2. Scan tables and find no data
# 3. Create no new entries
# 4. Now fill-gaps will correctly detect the missing range
```

## Indexing Modes

Choose what blockchain data to extract and index:

### Mode Comparison Table

| Mode | Datasets | Use Case | Performance | Storage (per 1M blocks) |
|------|----------|-----------|-------------|-------------------------|
| **Minimal** | `blocks` | Block-level analysis, lightweight | Fastest | ~5GB |
| **Default** | `blocks`, `transactions`, `logs` | Standard DeFi/DApp analysis | Fast | ~50GB |
| **Full** | All 10 datasets including traces & state diffs | Complete research & analysis | Slowest | ~500GB |
| **Custom** | User-defined | Tailored to specific needs | Variable | Variable |

### Default Mode
**Standard DeFi/DApp analysis setup**

**Datasets**: `blocks`, `transactions`, `logs`  
**Perfect for**:
- DeFi protocols analysis
- Transaction monitoring
- Event log processing
- Token transfer tracking

```bash
make continuous MODE=default
```

### Minimal Mode
**Block-level analysis only**

**Datasets**: `blocks`  
**Perfect for**:
- Block time analysis
- Mining/validator monitoring
- Network health metrics
- Lightweight deployment

```bash
make continuous MODE=minimal
```

### Full Mode
**Complete blockchain analysis**

**Datasets**: `blocks`, `transactions`, `logs`, `contracts`, `native_transfers`, `traces`, `balance_diffs`, `code_diffs`, `nonce_diffs`, `storage_diffs`  
**Perfect for**:
- MEV research
- State analysis
- Complete audit trails
- Academic research
- Forensic analysis

```bash
make continuous MODE=full
```

### Custom Mode
**Tailored dataset selection**

```bash
# MEV analysis
make continuous DATASETS=blocks,transactions,logs,traces,native_transfers

# Contract focus
make continuous DATASETS=blocks,transactions,contracts

# State tracking
make continuous DATASETS=blocks,balance_diffs,storage_diffs
```

## Configuration

All configuration is done through environment variables. Create a `.env` file or set them directly:

### Core Settings
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ETH_RPC_URL` | Blockchain RPC endpoint | - | ✅ (except backfill) |
| `NETWORK_NAME` | Network name for Cryo | ethereum | ❌ |
| `CLICKHOUSE_HOST` | ClickHouse host | - | ✅ |
| `CLICKHOUSE_USER` | ClickHouse username | default | ❌ |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | - | ✅ |
| `CLICKHOUSE_DATABASE` | Database name | blockchain | ❌ |
| `CLICKHOUSE_PORT` | ClickHouse port | 8443 | ❌ |
| `CLICKHOUSE_SECURE` | Use HTTPS | true | ❌ |

### Operation Settings
| Variable | Description | Default | Options |
|----------|-------------|---------|---------|
| `OPERATION` | Operation mode | continuous | continuous, historical, fill_gaps, validate, backfill |
| `MODE` | Indexing mode | default | default, minimal, full, custom |
| `START_BLOCK` | Starting block number | 0 | Any integer |
| `END_BLOCK` | Ending block number | 0 | Any integer |

### Performance Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `WORKERS` | Number of parallel workers | 1 | Use 4-16 for historical |
| `BATCH_SIZE` | Blocks per batch | 1000 | Reduce if memory issues |
| `MAX_RETRIES` | Max retry attempts | 3 | Increase for unreliable RPCs |

### Cryo Performance Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `REQUESTS_PER_SECOND` | RPC requests per second | 50 | Reduce if hitting rate limits |
| `MAX_CONCURRENT_REQUESTS` | Concurrent RPC requests | 5 | Reduce if RPC overloaded |
| `CRYO_TIMEOUT` | Cryo command timeout (seconds) | 300 | Increase for slow networks |

### Continuous Mode Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `CONFIRMATION_BLOCKS` | Blocks to wait for confirmation | 12 | Reorg protection |
| `POLL_INTERVAL` | Polling interval (seconds) | 10 | How often to check for new blocks |

### Backfill Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `BACKFILL_BATCH_SIZE` | Size of indexing_state ranges to create | 1000 | Smaller = more granular tracking |
| `BACKFILL_CHUNK_SIZE` | Size of chunks for scanning tables | 100000 | Larger = faster but more RAM |
| `BACKFILL_FORCE` | Force recreation of existing entries | false | Deletes existing state before scanning |

### Gap Detection Settings
| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `GAP_DETECTION_STATE_CHUNK_SIZE` | Chunk size for state-based gap detection | 100000 | Larger = faster but more memory |
| `GAP_DETECTION_TABLE_CHUNK_SIZE` | Chunk size for table-based gap detection | 10000 | Smaller chunks for actual data scanning |
| `GAP_DETECTION_THRESHOLD` | Threshold for switching to table-based detection | 0.8 | 0.8 = switch if 80%+ appears as gaps |

### Custom Dataset Settings
| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `DATASETS` | Comma-separated dataset list | - | blocks,transactions,logs,traces |

### Logging Settings
| Variable | Description | Default | Options |
|----------|-------------|---------|---------|
| `LOG_LEVEL` | Logging level | INFO | DEBUG, INFO, WARNING, ERROR |

## Database Schema

The indexer automatically creates these tables in ClickHouse:

### Core Tables
- **`blocks`** - Block headers and metadata
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
- **`indexing_state`** - Tracks what ranges have been indexed
- **`sync_position`** - Current sync position for continuous mode
- **`indexing_progress`** - Aggregated progress view (materialized view)
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

# Optional settings
NETWORK_NAME=ethereum
CLICKHOUSE_DATABASE=blockchain
WORKERS=4
BATCH_SIZE=1000
MODE=default
```

### Build and Run

```bash
# Build the image
docker-compose build

# Run migrations
docker-compose --profile migrations up migrations

# Start continuous indexing
docker-compose up cryo-indexer

# Historical indexing
OPERATION=historical START_BLOCK=18000000 END_BLOCK=18100000 \
docker-compose up cryo-indexer

# Backfill existing data
OPERATION=backfill docker-compose up cryo-indexer

# Fill gaps
OPERATION=fill_gaps docker-compose up cryo-indexer

# Validate data
OPERATION=validate docker-compose up cryo-indexer
```

## Running with Makefile

The Makefile provides convenient commands for common operations:

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

# Data integrity
make fill-gaps      # Find and fill missing data
make validate       # Check data completeness
make backfill       # Populate state from existing data
```

### Quick Operations
```bash
# Migration workflow for existing data
make migration-workflow  # backfill → validate → fill-gaps

# Conservative settings for rate-limited RPCs
make conservative-settings
make quick-historical START_BLOCK=1000000 END_BLOCK=1100000
```

### Monitoring
```bash
make logs           # View logs
make status         # Database status query
make ps             # Container status
```

### Utilities
```bash
make stop           # Stop indexer
make clean          # Remove containers and volumes
make shell          # Open container shell
```

## State Management

All indexing state is stored in ClickHouse, making the indexer completely stateless:

### Indexing State Tracking
- **Range Claiming**: Workers atomically claim block ranges to avoid duplicates
- **Progress Tracking**: Real-time tracking of completed, processing, and failed ranges
- **Retry Logic**: Failed ranges are automatically retried with backoff
- **Stale Job Recovery**: Detects and recovers from crashed workers

### Continuous Mode State
- **Sync Position**: Tracks the last processed block for each dataset
- **Reorg Detection**: Compares block hashes to detect chain reorganizations
- **Safe Indexing**: Waits for confirmation blocks before processing

### Hybrid Gap Detection
- **Primary Method**: Checks `indexing_state` table (fast)
- **Fallback Method**: Scans actual table data if most ranges appear missing
- **Smart Logic**: Automatically chooses best method based on data completeness
- **Memory Efficient**: Processes in chunks to handle large ranges

## Monitoring & Validation

### Progress Monitoring
```bash
# Check overall progress
make validate

# View detailed logs
make logs

# Monitor container status
make status
```

### Validation Reports
The validate command provides detailed reports:

```
=== INDEXING PROGRESS ===

blocks:
  Completed ranges: 1250
  Processing ranges: 2
  Failed ranges: 0
  Pending ranges: 0
  Highest block: 18125000
  Total rows: 125,000,000

transactions:
  Completed ranges: 1248
  Processing ranges: 0
  Failed ranges: 1
  Pending ranges: 1
  Highest block: 18124000
  Total rows: 45,230,123

=== CHECKING FOR GAPS (0 to 18125000) ===

blocks: No gaps found ✓
transactions: 2 gaps found
  Gap 1: blocks 18124000-18125000 (1000 blocks)
  Gap 2: blocks 18120000-18121000 (1000 blocks)
```

### Real-time Monitoring
During indexing, the system provides regular progress updates:

```
Progress: 45/116 (38.8%) | Active: 8 | Pending: 32 | Failed: 0 | 
Rate: 12.5 ranges/min | ETA: 5.7 min
```

## Performance Tuning

### General Guidelines

1. **Start Simple**: Begin with default settings and scale up
2. **Monitor Resources**: Watch CPU, memory, and network usage
3. **Test Settings**: Try different configurations on small ranges first
4. **Consider RPC Limits**: Respect your RPC provider's rate limits

### Operation-Specific Tuning

#### Continuous Mode
```bash
# Production continuous settings
WORKERS=1                    # Single worker for real-time
BATCH_SIZE=100              # Small batches for low latency
REQUESTS_PER_SECOND=30      # Conservative for stability
CONFIRMATION_BLOCKS=12      # Standard for most networks
POLL_INTERVAL=10           # Check every 10 seconds
```

#### Historical Mode
```bash
# Fast historical (good RPC)
WORKERS=16                  # High parallelism
BATCH_SIZE=2000            # Large batches for efficiency
REQUESTS_PER_SECOND=100    # Aggressive rate

# Conservative historical (rate-limited RPC)
WORKERS=2                   # Low parallelism
BATCH_SIZE=500             # Medium batches
REQUESTS_PER_SECOND=10     # Respect limits
```

#### Backfill Mode
```bash
# Fast backfill
BACKFILL_CHUNK_SIZE=200000  # Large chunks for speed
BACKFILL_BATCH_SIZE=2000   # Large state ranges

# Memory-conscious backfill
BACKFILL_CHUNK_SIZE=50000   # Smaller chunks
BACKFILL_BATCH_SIZE=500    # Smaller state ranges
```

## Troubleshooting

### Common Issues by Operation

#### Historical Mode Issues
**Symptoms**: Slow progress, timeouts, rate limiting  
**Solutions**:
```bash
# Conservative settings
WORKERS=2 BATCH_SIZE=100 REQUESTS_PER_SECOND=10 make historical

# Increase timeout for slow networks
CRYO_TIMEOUT=600 make historical
```

#### Fill-gaps Issues
**Symptoms**: "Everything appears as gaps"  
**Solutions**:
```bash
# Run backfill first for existing data
make backfill
make validate
make fill-gaps
```

#### Backfill Issues
**Symptoms**: "No data found", incorrect ranges  
**Solutions**:
```bash
# Force recreation of state
make backfill BACKFILL_FORCE=true

# Check table names and data
make shell
# Then: clickhouse-client --query "SHOW TABLES"
```

**Symptoms**: State shows data exists but it's actually deleted  
**Solutions**:
```bash
# Force backfill to rescan and update state
make backfill BACKFILL_FORCE=true START_BLOCK=<deleted_start> END_BLOCK=<deleted_end>

# This will delete the incorrect state entries and create new ones based on actual data
```

### Generic Issues

#### RPC Rate Limits
**Symptoms**: 429 errors, slow progress  
**Solution**: Reduce requests per second and concurrent requests

```bash
REQUESTS_PER_SECOND=10 MAX_CONCURRENT_REQUESTS=2 make historical
```

#### Out of Memory
**Symptoms**: OOM kills, container restarts  
**Solution**: Reduce batch size and workers

```bash
BATCH_SIZE=100 WORKERS=2 MEMORY_LIMIT=2G make continuous
```

#### Network Timeouts
**Symptoms**: Cryo timeouts, connection errors  
**Solution**: Increase timeout and reduce concurrency

```bash
CRYO_TIMEOUT=600 MAX_CONCURRENT_REQUESTS=3 make historical
```

## Use Case Examples

### Complete Deployment Scenarios

#### 1. New Deployment (Fresh Start)
```bash
# Setup
make build
make run-migrations

# Historical sync (choose range)
make historical START_BLOCK=18000000 END_BLOCK=latest WORKERS=8

# Switch to continuous
make continuous
```

#### 2. Existing Data Migration
```bash
# If you have existing blockchain data from other tools
make build
make run-migrations

# Scan existing data and create state tracking
make backfill

# Verify backfill worked
make validate

# Fill any gaps found
make fill-gaps

# Start continuous indexing
make continuous
```

#### 3. Research/Analysis Project
```bash
# Target specific historical period
make historical START_BLOCK=12000000 END_BLOCK=13000000 MODE=full

# Validate completeness
make validate

# Analyze specific events
make historical START_BLOCK=12500000 END_BLOCK=12600000 \
  DATASETS=blocks,transactions,logs
```

#### 4. Production DeFi Monitoring
```bash
# Real-time transaction and event monitoring
make continuous MODE=default BATCH_SIZE=50

# Or custom datasets for specific needs
make continuous DATASETS=blocks,transactions,logs,native_transfers
```

#### 5. MEV Research Setup
```bash
# Complete data including traces
make historical START_BLOCK=15000000 END_BLOCK=16000000 MODE=full

# Focus on MEV-relevant data
make continuous DATASETS=blocks,transactions,logs,traces,native_transfers
```

#### 6. Maintenance and Recovery
```bash
# Regular health check
make validate

# Weekly gap filling
make fill-gaps START_BLOCK=17000000 END_BLOCK=18000000

# After system issues
make fill-gaps  # Find and fix gaps
make validate   # Verify integrity
```

#### 7. Fixing Deleted Data
```bash
# Scenario: You accidentally deleted blocks 40374070-40374470
# But indexing_state still shows them as "completed"

# Step 1: Force backfill to update state
make backfill BACKFILL_FORCE=true START_BLOCK=40374070 END_BLOCK=40374470

# Step 2: Verify state is now correct
make validate

# Step 3: Re-index the missing data
make fill-gaps START_BLOCK=40374070 END_BLOCK=40374470
# Or use historical for immediate re-index:
make historical START_BLOCK=40374070 END_BLOCK=40374470
```

### Development and Testing

#### Local Development
```bash
# Test with small range
make historical START_BLOCK=18000000 END_BLOCK=18001000 WORKERS=1

# Validate results
make validate

# Test continuous mode
make continuous START_BLOCK=18001000
```

#### Performance Testing
```bash
# Conservative test
make conservative-settings
make test-range START_BLOCK=18000000

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
python -m src.indexer
```

### Testing
```bash
# Test with small range
make historical START_BLOCK=18000000 END_BLOCK=18000100 WORKERS=1

# Test backfill
make backfill START_BLOCK=18000000 END_BLOCK=18000100

# Validate results
make validate
```

## License

This project is licensed under the [MIT License](LICENSE)