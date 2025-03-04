import os
from typing import List, Optional
from pydantic_settings import BaseSettings

class IndexerSettings(BaseSettings):
    """Indexer settings loaded from environment variables."""
    
    # Blockchain settings
    eth_rpc_url: str = os.environ.get("ETH_RPC_URL", "")
    chain_id: int = int(os.environ.get("CHAIN_ID", "1"))
    confirmation_blocks: int = int(os.environ.get("CONFIRMATION_BLOCKS", "20"))
    poll_interval: int = int(os.environ.get("POLL_INTERVAL", "15"))
    start_block: int = int(os.environ.get("START_BLOCK", "0"))
    max_blocks_per_batch: int = int(os.environ.get("MAX_BLOCKS_PER_BATCH", "1000"))
    
    # ClickHouse settings
    clickhouse_host: str = os.environ.get("CLICKHOUSE_HOST", "")
    clickhouse_user: str = os.environ.get("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.environ.get("CLICKHOUSE_PASSWORD", "")
    clickhouse_database: str = os.environ.get("CLICKHOUSE_DATABASE", "blockchain")
    clickhouse_port: int = int(os.environ.get("CLICKHOUSE_PORT", "8443"))
    clickhouse_secure: bool = os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true"
    
    # Migration settings
    run_migrations: bool = os.environ.get("RUN_MIGRATIONS", "true").lower() == "true"
    
    # Paths
    data_dir: str = os.environ.get("DATA_DIR", "/app/data")
    state_dir: str = os.environ.get("STATE_DIR", "/app/state")
    log_dir: str = os.environ.get("LOG_DIR", "/app/logs")
    migrations_dir: str = os.environ.get("MIGRATIONS_DIR", "/app/migrations")
    
    # Logging
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")
    
    # Datasets to index
    datasets: List[str] = os.environ.get(
        "DATASETS", "blocks,transactions,logs"
    ).split(",")
    
    class Config:
        env_file = ".env"
        case_sensitive = False

# Create settings instance
settings = IndexerSettings()