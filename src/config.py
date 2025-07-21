import os
from typing import List
from loguru import logger
from enum import Enum


class OperationType(Enum):
    """Simplified operation types."""
    CONTINUOUS = "continuous"    # Follow chain tip
    HISTORICAL = "historical"    # Index specific range
    MAINTAIN = "maintain"        # Fix data integrity issues
    VALIDATE = "validate"        # Check data integrity (read-only)


class IndexMode(Enum):
    """Dataset selection modes."""
    MINIMAL = "minimal"     # blocks, transactions, logs
    EXTRA = "extra"         # contracts, native_transfers, traces  
    DIFFS = "diffs"         # balance_diffs, code_diffs, nonce_diffs, storage_diffs
    FULL = "full"           # all datasets
    CUSTOM = "custom"       # user-defined datasets


class IndexerSettings:
    """Simplified indexer settings with smart defaults."""
    
    def __init__(self):
        # Core settings
        self.eth_rpc_url = os.environ.get("ETH_RPC_URL", "")
        self.network_name = os.environ.get("NETWORK_NAME", "ethereum")
        
        # ClickHouse settings
        self.clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "")
        self.clickhouse_user = os.environ.get("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.clickhouse_database = os.environ.get("CLICKHOUSE_DATABASE", "blockchain")
        self.clickhouse_port = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
        self.clickhouse_secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"
        
        # Operation settings
        self.operation = OperationType(os.environ.get("OPERATION", "continuous").lower())
        
        # Mode handling with validation
        mode_str = os.environ.get("MODE", "minimal").lower()
        try:
            self.mode = IndexMode(mode_str)
        except ValueError:
            logger.warning(f"Invalid mode: {mode_str}, defaulting to 'minimal'")
            self.mode = IndexMode.MINIMAL
        
        # Block range settings (for historical/maintain)
        self.start_block = int(os.environ.get("START_BLOCK", "0"))
        self.end_block = int(os.environ.get("END_BLOCK", "0"))
        
        # Essential performance settings
        self.workers = int(os.environ.get("WORKERS", "1"))
        self.batch_size = int(os.environ.get("BATCH_SIZE", "100"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        
        # Continuous mode settings
        self.confirmation_blocks = int(os.environ.get("CONFIRMATION_BLOCKS", "12"))
        self.poll_interval = int(os.environ.get("POLL_INTERVAL", "10"))
        
        # Essential Cryo settings
        self.requests_per_second = int(os.environ.get("REQUESTS_PER_SECOND", "20"))
        self.max_concurrent_requests = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "2"))
        self.cryo_timeout = int(os.environ.get("CRYO_TIMEOUT", "600"))
        
        # Directories
        self.data_dir = os.environ.get("DATA_DIR", "/app/data")
        self.log_dir = os.environ.get("LOG_DIR", "/app/logs")
        self.migrations_dir = os.environ.get("MIGRATIONS_DIR", "/app/migrations")
        
        # Logging
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        
        # Get datasets based on mode
        self.datasets = self._get_datasets()
        
    def _get_datasets(self) -> List[str]:
        """Get datasets based on mode."""
        # Check for custom datasets first
        custom_datasets = os.environ.get("DATASETS", "")
        if custom_datasets and self.mode == IndexMode.CUSTOM:
            return [d.strip() for d in custom_datasets.split(",")]
            
        # Otherwise use mode defaults
        mode_datasets = {
            IndexMode.MINIMAL: ["blocks", "transactions", "logs"],
            IndexMode.EXTRA: ["contracts", "native_transfers", "traces"],
            IndexMode.DIFFS: ["balance_diffs", "code_diffs", "nonce_diffs", "storage_diffs"],
            IndexMode.FULL: [
                "blocks", "transactions", "logs", "contracts", 
                "native_transfers", "traces", "balance_diffs", 
                "code_diffs", "nonce_diffs", "storage_diffs"
            ]
        }
        
        return mode_datasets.get(self.mode, ["blocks", "transactions", "logs"])
    
    def validate(self) -> None:
        """Validate required settings."""
        if not self.eth_rpc_url:
            raise ValueError("ETH_RPC_URL is required")
            
        if not self.clickhouse_host:
            raise ValueError("CLICKHOUSE_HOST is required")
            
        if self.operation == OperationType.HISTORICAL:
            if self.start_block >= self.end_block:
                raise ValueError("For historical operation, END_BLOCK must be greater than START_BLOCK")
                
        if self.workers < 1:
            raise ValueError("WORKERS must be at least 1")
            
        if self.batch_size < 1:
            raise ValueError("BATCH_SIZE must be at least 1")


# Create global settings instance
settings = IndexerSettings()