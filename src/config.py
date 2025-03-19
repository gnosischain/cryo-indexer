import os
from typing import List, Dict, Optional

class IndexerMode:
    """Definition of an indexer mode with its specific settings."""
    
    def __init__(
        self,
        name: str,
        datasets: List[str],
        start_block: int,
        description: str = ""
    ):
        self.name = name
        self.datasets = datasets
        self.start_block = start_block
        self.description = description

class IndexerSettings:
    """Indexer settings loaded from environment variables."""
    
    def __init__(self):
        # Blockchain settings
        self.eth_rpc_url = os.environ.get("ETH_RPC_URL", "")
        self.network_name = os.environ.get("NETWORK_NAME", "gnosis")
        self.confirmation_blocks = int(os.environ.get("CONFIRMATION_BLOCKS", "20"))
        self.poll_interval = int(os.environ.get("POLL_INTERVAL", "15"))
        self.max_blocks_per_batch = int(os.environ.get("MAX_BLOCKS_PER_BATCH", "1000"))
        
        # Chain-specific settings
        self.genesis_timestamp = int(os.environ.get("GENESIS_TIMESTAMP", "1539024180"))  # Default for Gnosis chain
        self.seconds_per_block = int(os.environ.get("SECONDS_PER_BLOCK", "5"))  # Default for Gnosis chain
        self.chain_id = int(os.environ.get("CHAIN_ID", "100"))  # Default for Gnosis chain
        
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
        
        # Indexer mode
        self.mode_name = os.environ.get("INDEXER_MODE", "default")
        
        # Default start block (can be overridden by mode)
        self.default_start_block = int(os.environ.get("START_BLOCK", "0"))
        
        # Define indexer modes
        self._define_modes()
        
        # Set datasets and start_block based on the selected mode
        self._configure_from_mode()
    
    def _define_modes(self):
        """Define available indexer modes with their specific configurations."""
        self.available_modes = {
            # Default mode for basic data
            "default": IndexerMode(
                name="default",
                datasets=["blocks", "transactions", "logs"],
                start_block=0,
                description="Default mode for indexing blocks, transactions, and logs"
            ),
            
            # Mode for block data only
            "blocks": IndexerMode(
                name="blocks",
                datasets=["blocks"],
                start_block=0,
                description="Index blocks only"
            ),
            
            # Mode for transaction data
            "transactions": IndexerMode(
                name="transactions",
                datasets=["transactions"],
                start_block=0,
                description="Index transactions only"
            ),
            
            # Mode for logs/events
            "logs": IndexerMode(
                name="logs",
                datasets=["logs"],
                start_block=0,
                description="Index logs/events only"
            ),
            
            # Mode for contract creation data
            "contracts": IndexerMode(
                name="contracts",
                datasets=["contracts"],
                start_block=0,
                description="Index contract creations only"
            ),
            
            # Mode for native ETH transfers
            "transfers": IndexerMode(
                name="transfers",
                datasets=["native_transfers"],
                start_block=0,
                description="Index native ETH transfers only"
            ),
            
            # Mode for all transaction-related data
            "tx_data": IndexerMode(
                name="tx_data", 
                datasets=["blocks", "transactions", "logs", "contracts", "traces", "native_transfers"],
                start_block=0,
                description="Index all transaction-related data"
            ),
            
            # Mode for trace data
            "traces": IndexerMode(
                name="traces",
                datasets=["traces"],
                start_block=0,
                description="Index transaction traces"
            ),
            
            # Mode for state differences
            "state_diffs": IndexerMode(
                name="state_diffs",
                datasets=["balance_diffs", "code_diffs", "nonce_diffs", "storage_diffs"],
                start_block=1,  # Start from block 1 for state diffs
                description="Index state differences (balance, code, nonce, storage)"
            ),
            
            # Mode for ERC20 token data
            "erc20": IndexerMode(
                name="erc20",
                datasets=["erc20_transfers", "erc20_metadata"],
                start_block=0,
                description="Index ERC20 token transfers and metadata"
            ),
            
            # Mode for ERC721 token data
            "erc721": IndexerMode(
                name="erc721",
                datasets=["erc721_transfers", "erc721_metadata"],
                start_block=0,
                description="Index ERC721 token transfers and metadata"
            ),
            
            # Mode for all on-chain data (full indexing)
            "full": IndexerMode(
                name="full",
                datasets=[
                    "blocks", "transactions", "logs", "contracts", 
                    "native_transfers", "traces", "balance_diffs", 
                    "code_diffs", "nonce_diffs", "storage_diffs",
                    "erc20_transfers", "erc20_metadata", 
                    "erc721_transfers", "erc721_metadata"
                ],
                start_block=0,
                description="Index all available data types"
            ),
            
            # Custom mode, configured through environment variables
            "custom": IndexerMode(
                name="custom",
                datasets=os.environ.get("DATASETS", "blocks,transactions,logs").split(","),
                start_block=int(os.environ.get("START_BLOCK", "0")),
                description="Custom indexing mode configured via DATASETS and START_BLOCK"
            )
        }
    
    def _configure_from_mode(self):
        """Configure indexer settings based on the selected mode."""
        # Get the mode configuration (default to custom if not found)
        mode = self.available_modes.get(self.mode_name, self.available_modes["custom"])
        
        # Override datasets if DATASETS environment variable is explicitly set
        if "DATASETS" in os.environ:
            self.datasets = os.environ.get("DATASETS").split(",")
            # Also update the custom mode
            self.available_modes["custom"].datasets = self.datasets
        else:
            self.datasets = mode.datasets
        
        # Override start_block if START_BLOCK environment variable is explicitly set
        if "START_BLOCK" in os.environ:
            self.start_block = int(os.environ.get("START_BLOCK"))
            # Also update the custom mode
            self.available_modes["custom"].start_block = self.start_block
        else:
            self.start_block = mode.start_block
    
    def get_available_modes(self) -> Dict[str, IndexerMode]:
        """Get all available indexer modes."""
        return self.available_modes
    
    def get_current_mode(self) -> IndexerMode:
        """Get the currently active indexer mode."""
        return self.available_modes.get(self.mode_name, self.available_modes["custom"])

# Create settings instance
settings = IndexerSettings()