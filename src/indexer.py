import os
import time
from loguru import logger

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .multiprocess_indexer import MultiprocessIndexer
from .worker import IndexerWorker  # Import the new unified worker
from .utils import setup_logging, load_state, save_state


class CryoIndexer:
    """Main indexer class that orchestrates the indexing process."""
    
    def __init__(self):
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
        # Get the current indexer mode
        current_mode = settings.get_current_mode()
        logger.info(f"Starting indexer in mode: {current_mode.name} - {current_mode.description}")
        logger.info(f"Datasets to index: {current_mode.datasets}")
        logger.info(f"Operation mode: {settings.operation_mode}")
        
        # Initialize components
        self.blockchain = BlockchainClient(settings.eth_rpc_url)
        self.clickhouse = ClickHouseManager(
            host=settings.clickhouse_host,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            port=settings.clickhouse_port,
            secure=settings.clickhouse_secure
        )
        
        # Create necessary directories
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(settings.state_dir, exist_ok=True)
        
        # Use a mode-specific state file if provided in environment
        state_file = os.environ.get("INDEXER_STATE_FILE")
        if state_file:
            logger.info(f"Using custom state file: {state_file}")
            self.state_file = state_file
        else:
            # Otherwise, create a state file specific to this mode
            self.state_file = os.path.join(settings.state_dir, f"indexer_state_{current_mode.name}.json")
            logger.info(f"Using mode-specific state file: {self.state_file}")
        
        # Load state for this specific mode
        self.state = load_state(settings.state_dir, state_file=os.path.basename(self.state_file))
        
        # Add mode info to state if not present
        if "mode" not in self.state:
            self.state["mode"] = current_mode.name
        
        # Initialize starting block based on explicit START_BLOCK if provided
        self._initialize_starting_block(current_mode)
        
        logger.info(f"Indexer initialized with state: {self.state}")
        logger.info(f"Using data directory: {settings.data_dir}")
        
        # Set up tables in ClickHouse
        self.clickhouse.setup_tables()
    
    def _initialize_starting_block(self, current_mode):
        """Initialize the starting block with proper precedence."""
        # Check if START_BLOCK environment variable is explicitly set
        explicit_start_block = os.environ.get("START_BLOCK")
        
        if explicit_start_block is not None and explicit_start_block.strip():
            try:
                explicit_block = int(explicit_start_block)
                logger.info(f"Explicit START_BLOCK={explicit_block} specified")
                
                if explicit_block > 0 or self.state['last_block'] == 0:
                    logger.info(f"Using START_BLOCK={explicit_block} as starting point")
                    self.state['last_block'] = explicit_block
                    save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                else:
                    logger.info(f"Ignoring START_BLOCK=0 since we have existing state: last_block={self.state['last_block']}")
            except ValueError:
                logger.error(f"Invalid START_BLOCK value: {explicit_start_block}")
                self._initialize_from_state_or_db(current_mode)
        else:
            self._initialize_from_state_or_db(current_mode)

    def _initialize_from_state_or_db(self, current_mode):
        """Initialize from state file or database if last_block is 0."""
        if self.state['last_block'] == 0:
            current_last_block = 0
            
            # Only check database if we might need to continue from there
            if current_mode.start_block == 0:
                current_last_block = self._get_latest_block_for_datasets(current_mode.datasets)
            
            # Use the higher of mode's start block or last indexed block
            self.state['last_block'] = max(current_last_block, current_mode.start_block)
            save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
    
    def _get_latest_block_for_datasets(self, datasets):
        """Get the latest block number for the specified datasets."""
        return self.clickhouse.get_latest_processed_block()
    
    def run(self):
        """Main entry point that starts the appropriate indexer based on operation mode."""
        logger.info("Starting indexer...")
        
        # Check if we should run in parallel mode
        if settings.operation_mode == "parallel":
            if settings.parallel_workers < 2:
                logger.warning("Parallel mode selected but PARALLEL_WORKERS is less than 2. Setting to 2.")
                settings.parallel_workers = 2
                
            logger.info(f"Starting in multiprocess parallel mode with {settings.parallel_workers} workers")
            
            # Start multiprocess indexer
            multiprocess_indexer = MultiprocessIndexer()
            multiprocess_indexer.run()
        else:
            # Run in standard sequential mode (using the new unified worker)
            logger.info("Starting in sequential mode")
            self._run_sequential()
    
    def _run_sequential(self):
        """Run the indexer in sequential mode using the unified worker."""
        logger.info("Starting sequential indexer...")
        
        # Get the current mode
        current_mode = settings.get_current_mode()
        
        # Check if we're running with an end block
        historical_mode = settings.end_block > 0
        
        # Create a worker instance for sequential processing
        worker = IndexerWorker(
            worker_id="sequential",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url,
            state=self.state,
            state_file=self.state_file
        )
        
        if historical_mode:
            logger.info(f"Running with fixed block range: {self.state['last_block']} to {settings.end_block}")
        
        while True:
            try:
                # Get the latest block number
                latest_block = self.blockchain.get_latest_block_number()
                
                # Calculate the safe block to process (allowing for potential reorgs)
                safe_block = latest_block - settings.confirmation_blocks
                
                # If end_block is set, cap the safe block
                if historical_mode:
                    safe_block = min(safe_block, settings.end_block)
                    
                logger.debug(f"Latest block: {latest_block}, Safe block: {safe_block}, Last processed: {self.state['last_block']}")
                
                # Check if there are new blocks to process
                if safe_block > self.state['last_block']:
                    # Process new blocks
                    next_block = self.state['last_block']
                    end_block = min(safe_block, next_block + settings.max_blocks_per_batch)
                    
                    logger.info(f"Processing blocks {next_block} to {end_block}")
                    success = worker.process_block_range(next_block, end_block, current_mode.datasets)
                    
                    if not success:
                        logger.error(f"Failed to process blocks {next_block} to {end_block}, retrying in {settings.poll_interval} seconds")
                        time.sleep(settings.poll_interval)
                        continue
                    
                    # Update state in memory (worker already updates the state file)
                    self.state['last_block'] = end_block
                    
                    # Check if we've reached the requested end block in historical mode
                    if historical_mode and self.state['last_block'] >= settings.end_block:
                        logger.info(f"Reached requested end block {settings.end_block}. Indexing complete.")
                        return  # Exit the function
                else:
                    if historical_mode:
                        logger.info(f"No more blocks to process up to requested end block {settings.end_block}. Indexing complete.")
                        return  # Exit the function
                    else:
                        logger.info(f"No new blocks to process. Waiting {settings.poll_interval} seconds...")
                        # Sleep until next check
                        time.sleep(settings.poll_interval)
            except Exception as e:
                logger.error(f"Error in main sequential loop: {e}", exc_info=True)
                time.sleep(settings.poll_interval)


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()