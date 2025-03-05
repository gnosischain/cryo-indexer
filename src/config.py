import os
from typing import List, Optional

# Simple settings class without Pydantic
class IndexerSettings:
    """Indexer settings loaded from environment variables."""
    
    def __init__(self):
        # Blockchain settings
        self.eth_rpc_url = os.environ.get("ETH_RPC_URL", "")
        self.chain_id = int(os.environ.get("CHAIN_ID", "1"))
        self.confirmation_blocks = int(os.environ.get("CONFIRMATION_BLOCKS", "20"))
        self.poll_interval = int(os.environ.get("POLL_INTERVAL", "15"))
        self.start_block = int(os.environ.get("START_BLOCK", "0"))
        self.max_blocks_per_batch = int(os.environ.get("MAX_BLOCKS_PER_BATCH", "1000"))
        
        # ClickHouse settings
        self.clickhouse_host = os.environ.get("CLICKHOUSE_HOST", "")
        self.clickhouse_user = os.environ.get("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        self.clickhouse_database = os.environ.get("CLICKHOUSE_DATABASE", "blockchain")
        self.clickhouse_port = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
        self.clickhouse_secure = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"
        
        # Migration settings
        self.run_migrations = os.environ.get("RUN_MIGRATIONS", "true").lower() == "true"
        
        # Paths
        self.data_dir = os.environ.get("DATA_DIR", "/app/data")
        self.state_dir = os.environ.get("STATE_DIR", "/app/state")
        self.log_dir = os.environ.get("LOG_DIR", "/app/logs")
        self.migrations_dir = os.environ.get("MIGRATIONS_DIR", "/app/migrations")
        
        # Logging
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        
        # Datasets
        datasets_str = os.environ.get("DATASETS", "blocks,transactions,logs")
        self.datasets = datasets_str.split(",")

# Create settings instance
settings = IndexerSettings()