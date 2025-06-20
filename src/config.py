import os
from typing import List, Dict, Optional
from loguru import logger
from enum import Enum


class IndexMode(Enum):
    """Simplified indexing modes."""
    DEFAULT = "default"      # blocks, transactions, logs
    MINIMAL = "minimal"      # blocks only
    FULL = "full"           # all datasets
    CUSTOM = "custom"       # user-defined datasets
    

class OperationType(Enum):
    """Types of operations."""
    CONTINUOUS = "continuous"    # Follow chain tip
    HISTORICAL = "historical"    # Index specific range
    FILL_GAPS = "fill_gaps"     # Find and fill gaps
    VALIDATE = "validate"        # Check data integrity
    BACKFILL = "backfill"       # Backfill indexing_state from existing data
    FIX_TIMESTAMPS = "fix_timestamps"  # Fix incorrect timestamps 


class IndexerSettings:
    """Simplified indexer settings."""
    
    def __init__(self):
        # Core settings
        self.eth_rpc_url = os.environ.get("ETH_RPC_URL", "")
        self.network_name = os.environ.get("NETWORK_NAME", "gnosis")
        
        # ClickHouse settings
        self.clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "")
        self.clickhouse_user = os.environ.get("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.clickhouse_database = os.environ.get("CLICKHOUSE_DATABASE", "blockchain")
        self.clickhouse_port = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
        self.clickhouse_secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"
        
        # Operation settings
        self.operation = OperationType(os.environ.get("OPERATION", "continuous").lower())
        self.mode = IndexMode(os.environ.get("MODE", "default").lower())
        
        # Block range settings
        self.start_block = int(os.environ.get("START_BLOCK", "0"))
        self.end_block = int(os.environ.get("END_BLOCK", "0"))
        
        # Performance settings
        self.workers = int(os.environ.get("WORKERS", "1"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "1000"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        
        # Fixed range size for all operations
        self.indexing_range_size = int(os.environ.get("INDEXING_RANGE_SIZE", "1000"))
        
        # Continuous mode settings
        self.confirmation_blocks = int(os.environ.get("CONFIRMATION_BLOCKS", "12"))
        self.poll_interval = int(os.environ.get("POLL_INTERVAL", "10"))
        
        # Cryo performance settings
        self.requests_per_second = int(os.environ.get("REQUESTS_PER_SECOND", "50"))
        self.max_concurrent_requests = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "5"))
        self.cryo_timeout = int(os.environ.get("CRYO_TIMEOUT", "300"))  # 5 minutes default
        
        # Backfill settings (note: batch_size is now the fixed range size)
        self.backfill_batch_size = self.indexing_range_size  # Use fixed range size
        self.backfill_chunk_size = int(os.environ.get("BACKFILL_CHUNK_SIZE", "100000"))
        self.backfill_force = os.environ.get("BACKFILL_FORCE", "false").lower() == "true"
        
        # Gap detection settings
        self.gap_detection_state_chunk_size = int(os.environ.get("GAP_DETECTION_STATE_CHUNK_SIZE", "100000"))
        self.gap_detection_table_chunk_size = int(os.environ.get("GAP_DETECTION_TABLE_CHUNK_SIZE", "10000"))
        self.gap_detection_threshold = float(os.environ.get("GAP_DETECTION_THRESHOLD", "0.8"))
        
        # Directories
        self.data_dir = os.environ.get("DATA_DIR", "/app/data")
        self.log_dir = os.environ.get("LOG_DIR", "/app/logs")
        self.migrations_dir = os.environ.get("MIGRATIONS_DIR", "/app/migrations")

        # Timestamp fix settings
        self.timestamp_fix_batch_size = int(os.environ.get("TIMESTAMP_FIX_BATCH_SIZE", "100000"))
        self.strict_timestamp_mode = os.environ.get("STRICT_TIMESTAMP_MODE", "false").lower() == "true"
        
        # Logging
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        
        # Get datasets based on mode
        self.datasets = self._get_datasets()
        
    def _get_datasets(self) -> List[str]:
        """Get datasets based on mode."""
        # Check for custom datasets first
        custom_datasets = os.environ.get("DATASETS", "")
        if custom_datasets:
            return [d.strip() for d in custom_datasets.split(",")]
            
        # Otherwise use mode defaults
        mode_datasets = {
            IndexMode.DEFAULT: ["blocks", "transactions", "logs"],
            IndexMode.MINIMAL: ["blocks"],
            IndexMode.FULL: [
                "blocks", "transactions", "logs", "contracts", 
                "native_transfers", "traces", "balance_diffs", 
                "code_diffs", "nonce_diffs", "storage_diffs"
            ],
            IndexMode.CUSTOM: ["blocks", "transactions", "logs"]
        }
        
        return mode_datasets.get(self.mode, ["blocks", "transactions", "logs"])
    
    def validate(self) -> None:
        """Validate required settings."""
        # RPC not required for backfill operation
        if self.operation != OperationType.BACKFILL and not self.eth_rpc_url:
            raise ValueError("ETH_RPC_URL is required for non-backfill operations")
            
        if not self.clickhouse_host:
            raise ValueError("CLICKHOUSE_HOST is required")
            
        if self.operation == OperationType.HISTORICAL:
            if self.start_block >= self.end_block:
                raise ValueError("For historical operation, END_BLOCK must be greater than START_BLOCK")
                
        if self.workers < 1:
            raise ValueError("WORKERS must be at least 1")
            
        if self.batch_size < 1:
            raise ValueError("BATCH_SIZE must be at least 1")
            
        if self.indexing_range_size < 1:
            raise ValueError("INDEXING_RANGE_SIZE must be at least 1")
            
        if self.requests_per_second < 1:
            raise ValueError("REQUESTS_PER_SECOND must be at least 1")
            
        if self.max_concurrent_requests < 1:
            raise ValueError("MAX_CONCURRENT_REQUESTS must be at least 1")
            
        if self.backfill_chunk_size < 1:
            raise ValueError("BACKFILL_CHUNK_SIZE must be at least 1")
            
        if self.gap_detection_state_chunk_size < 1:
            raise ValueError("GAP_DETECTION_STATE_CHUNK_SIZE must be at least 1")
            
        if self.gap_detection_table_chunk_size < 1:
            raise ValueError("GAP_DETECTION_TABLE_CHUNK_SIZE must be at least 1")
            
        if not 0.0 <= self.gap_detection_threshold <= 1.0:
            raise ValueError("GAP_DETECTION_THRESHOLD must be between 0.0 and 1.0")


# Create global settings instance
settings = IndexerSettings()