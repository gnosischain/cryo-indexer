import os
import time
import glob
import shutil
import subprocess
import json
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
from datetime import datetime
import time

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .utils import setup_logging, load_state, save_state, find_parquet_files, format_block_range, read_parquet_to_pandas


class CryoIndexer:
    """Main indexer class that orchestrates the indexing process."""
    
    def __init__(self):
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
        # Get the current indexer mode
        current_mode = settings.get_current_mode()
        logger.info(f"Starting indexer in mode: {current_mode.name} - {current_mode.description}")
        logger.info(f"Datasets to index: {current_mode.datasets}")
        logger.info(f"Start block for this mode: {current_mode.start_block}")
        
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
        
        # Verify Cryo installation
        self._verify_cryo()
        
        # Set up tables in ClickHouse
        self.clickhouse.setup_tables()
    
    def _initialize_starting_block(self, current_mode):
        """
        Initialize the starting block with proper precedence:
        1. Explicit START_BLOCK from environment if provided
        2. Otherwise, use the higher of state's last_block or mode's start_block
        """
        # Check if START_BLOCK environment variable is explicitly set
        explicit_start_block = os.environ.get("START_BLOCK")
        
        if explicit_start_block is not None and explicit_start_block.strip():
            try:
                # Parse the explicit start block
                explicit_block = int(explicit_start_block)
                logger.info(f"Explicit START_BLOCK={explicit_block} specified")
                
                # Only use explicit start block if:
                # - It's greater than 0, or
                # - self.state['last_block'] is 0 (new indexing)
                if explicit_block > 0 or self.state['last_block'] == 0:
                    logger.info(f"Using START_BLOCK={explicit_block} as starting point")
                    self.state['last_block'] = explicit_block
                    # Save to state file immediately
                    save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                else:
                    logger.info(f"Ignoring START_BLOCK=0 since we have existing state: last_block={self.state['last_block']}")
            except ValueError:
                logger.error(f"Invalid START_BLOCK value: {explicit_start_block}")
                # Fall back to normal initialization
                self._initialize_from_state_or_db(current_mode)
        else:
            # No explicit start block, use normal initialization
            self._initialize_from_state_or_db(current_mode)

    def _initialize_from_state_or_db(self, current_mode):
        """Initialize from state file or database if last_block is 0"""
        if self.state['last_block'] == 0:
            current_last_block = 0
            
            # Only check database if we might need to continue from there
            if current_mode.start_block == 0:
                current_last_block = self._get_latest_block_for_datasets(current_mode.datasets)
            
            # Use the higher of mode's start block or last indexed block
            self.state['last_block'] = max(current_last_block, current_mode.start_block)
            save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
    
    def _verify_cryo(self):
        """Verify that Cryo is installed and working correctly."""
        try:
            result = subprocess.run(
                ["cryo", "--version"],
                check=False,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                logger.error(f"Cryo verification failed: {result.stderr}")
                raise Exception("Cryo is not properly installed or accessible")
            
            logger.info(f"Cryo version: {result.stdout.strip()}")
        except Exception as e:
            logger.error(f"Error verifying Cryo: {e}")
            raise
    
    def _get_latest_block_for_datasets(self, datasets: List[str]) -> int:
        """Get the latest block number for the specified datasets."""
        max_block = 0
        
        # Map of dataset names to corresponding ClickHouse tables
        dataset_to_table = {
            "blocks": "blocks", 
            "transactions": "transactions", 
            "txs": "transactions", 
            "logs": "logs", 
            "events": "logs", 
            "contracts": "contracts", 
            "native_transfers": "native_transfers", 
            "traces": "traces", 
            "balance_diffs": "balance_diffs", 
            "code_diffs": "code_diffs", 
            "nonce_diffs": "nonce_diffs", 
            "storage_diffs": "storage_diffs", 
            "slot_diffs": "storage_diffs", 
            "balance_reads": "balance_reads", 
            "code_reads": "code_reads", 
            "storage_reads": "storage_reads", 
            "slot_reads": "storage_reads", 
            "erc20_transfers": "erc20_transfers", 
            "erc20_metadata": "erc20_metadata", 
            "erc721_transfers": "erc721_transfers", 
            "erc721_metadata": "erc721_metadata"
        }
        
        # For each dataset, check the corresponding table in ClickHouse
        for dataset in datasets:
            table_name = dataset_to_table.get(dataset)
            if not table_name:
                logger.warning(f"Unknown dataset '{dataset}', skipping in block height check")
                continue
            
            try:
                # Query the table to find the max block number
                block = self.clickhouse.get_latest_block_for_table(table_name)
                if block > max_block:
                    max_block = block
                    logger.info(f"Found latest block {block} in table {table_name}")
            except Exception as e:
                logger.warning(f"Error checking latest block for {dataset}: {e}")
        
        return max_block
    
    def run(self) -> None:
        """Main loop that continuously indexes new blocks."""
        logger.info("Starting indexer...")
        
        # Get the current mode
        current_mode = settings.get_current_mode()
        
        # Check if we're running with an end block
        historical_mode = settings.end_block > 0
        
        # For all modes, query the database to find the actual progress
        # rather than relying on state files that might be lost with container restarts
        # BUT ONLY IF START_BLOCK is not explicitly set
        if "blocks" in current_mode.datasets and "START_BLOCK" not in os.environ:
            db_last_block = self.clickhouse.get_latest_block_for_table("blocks")
            if db_last_block > 0:
                logger.info(f"Found last processed block in database: {db_last_block}")
                
                # Only update state if database shows greater progress than our state file
                # or if we're starting a new run (state['last_block'] == 0)
                if db_last_block > self.state['last_block'] or self.state['last_block'] == 0:
                    self.state['last_block'] = db_last_block
        
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
                    self._process_blocks(next_block, end_block)
                    
                    # Update state in memory
                    self.state['last_block'] = end_block
                    
                    # Save state to file to track progress
                    save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                    
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
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(settings.poll_interval)
        
    def _handle_potential_reorg(self) -> None:
        """Check for and handle any blockchain reorganizations."""
        try:
            # Only check for reorgs if we've processed some blocks already
            if self.state['last_block'] <= 0:
                return
                
            # Get a range of blocks from our database to check against
            check_start = max(0, self.state['last_block'] - 10)
            db_blocks = self.clickhouse.get_blocks_in_range(check_start, self.state['last_block'])
            
            if not db_blocks:
                logger.warning("No blocks found in database for reorg check")
                return
                
            # Check if there's a reorg
            reorg_detected, common_ancestor = self.blockchain.detect_reorg(
                self.state['last_block'], 
                db_blocks
            )
            
            if reorg_detected:
                logger.warning(f"Reorg detected! Rolling back from block {self.state['last_block']} to {common_ancestor}")
                
                # Roll back the database
                success = self.clickhouse.rollback_to_block(common_ancestor)
                
                if success:
                    # Update our state
                    self.state['last_block'] = common_ancestor
                    save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                    logger.info(f"Successfully rolled back to block {common_ancestor}")
                else:
                    logger.error("Failed to roll back database during reorg handling")
        
        except Exception as e:
            logger.error(f"Error handling potential reorg: {e}", exc_info=True)
    
    def _process_blocks(self, start_block: int, end_block: int) -> None:
        try:
            # Handle potential reorgs
            self._handle_potential_reorg()
            
            # Clean up data directory before starting
            self._clean_data_directory()
            
            # Get the current mode and datasets
            current_mode = settings.get_current_mode()
            all_datasets = current_mode.datasets.copy()
            
            # Split datasets into state diff and non-state diff categories
            state_diff_datasets = ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']
            non_state_diff_datasets = [d for d in all_datasets if d not in state_diff_datasets]
            state_diff_only = [d for d in all_datasets if d in state_diff_datasets]
            
            # Always process blocks first for timestamp reference
            if 'blocks' in non_state_diff_datasets:
                blocks_datasets = ['blocks']
                logger.info("Processing blocks first to ensure timestamp accuracy")
                self._run_cryo(start_block, end_block, blocks_datasets)
                self._load_to_clickhouse(blocks_datasets)
                
                logger.info(f"Verifying blocks {start_block}-{end_block}")
                # Verify blocks are loaded before continuing
                self._verify_blocks_loaded(start_block, end_block)
                
                # Remove blocks from the list of datasets to process
                non_state_diff_datasets = [d for d in non_state_diff_datasets if d != 'blocks']
            
            # Process remaining non-state diff datasets
            if non_state_diff_datasets:
                logger.info(f"Processing non-state diff datasets: {non_state_diff_datasets}")
                self._run_cryo(start_block, end_block, non_state_diff_datasets)
                self._load_to_clickhouse(non_state_diff_datasets)
            
            # Process state diff datasets if any (starting from block 1 or higher)
            if state_diff_only:
                # Adjust starting block for state diff datasets
                state_diff_start = max(start_block, 1)  # Ensure state diffs start at least at block 1
                
                if state_diff_start != start_block:
                    logger.info(f"Adjusting state diff starting block from {start_block} to {state_diff_start} (state diffs must start at block 1 or higher)")
                
                if state_diff_start <= end_block:  # Only process if there's a valid range
                    logger.info(f"Processing state diff datasets for blocks {state_diff_start}-{end_block}: {state_diff_only}")
                    self._run_cryo(state_diff_start, end_block, state_diff_only)
                    self._load_to_clickhouse(state_diff_only)
                else:
                    logger.info(f"Skipping state diff datasets - no valid block range after adjustment (start: {state_diff_start}, end: {end_block})")
        
        except Exception as e:
            logger.error(f"Error processing blocks {start_block}-{end_block}: {e}", exc_info=True)
            raise

    def _check_blocks_exist(self, start_block: int, end_block: int) -> bool:
        """Check if all blocks in range already exist in the database."""
        try:
            logger.info(f"Checking if blocks {start_block}-{end_block} already exist in database")
            
            query_result = self.clickhouse.client.query(f"""
                SELECT 
                    COUNT(*) as existing_blocks,
                    COUNT(DISTINCT block_number) as distinct_blocks
                FROM {self.clickhouse.database}.blocks
                WHERE block_number BETWEEN {start_block} AND {end_block} - 1
            """)
            
            if query_result.result_rows:
                existing_blocks = query_result.result_rows[0][0]
                distinct_blocks = query_result.result_rows[0][1]
                expected_count = end_block - start_block
                
                if distinct_blocks == expected_count:
                    logger.info(f"All {distinct_blocks} blocks already exist in database")
                    
                    # Check for duplicates
                    if existing_blocks > distinct_blocks:
                        logger.warning(f"Found {existing_blocks - distinct_blocks} duplicate block entries")
                    
                    return True
                else:
                    logger.info(f"Only {distinct_blocks}/{expected_count} blocks found in database")
                    return False
            else:
                logger.info("No blocks found in database for this range")
                return False
                
        except Exception as e:
            logger.error(f"Error checking if blocks exist: {e}", exc_info=True)
            return False
    
    def _verify_blocks_loaded(self, start_block: int, end_block: int) -> None:
        """Verify that blocks are loaded in the database before proceeding."""
        max_attempts = 5
        attempts = 0
        
        while attempts < max_attempts:
            logger.info(f"Verifying blocks {start_block}-{end_block} are loaded in the database...")
            
            # Modified query to count DISTINCT block numbers
            query_result = self.clickhouse.client.query(f"""
                SELECT 
                    COUNT(DISTINCT block_number) as distinct_blocks,
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM {self.clickhouse.database}.blocks
                WHERE block_number BETWEEN {start_block} AND {end_block} - 1
            """)
            
            if query_result.result_rows:
                distinct_blocks = query_result.result_rows[0][0]
                min_block = query_result.result_rows[0][1]
                max_block = query_result.result_rows[0][2]
                
                expected_count = end_block - start_block
                
                if distinct_blocks == expected_count and min_block == start_block and max_block == end_block - 1:
                    logger.info(f"All {distinct_blocks} blocks verified in database")
                    time.sleep(5)
                    return
                else:
                    logger.warning(f"Only {distinct_blocks}/{expected_count} distinct blocks found in database. Min: {min_block}, Max: {max_block}")
            else:
                logger.warning("No blocks found in database for verification query")
            
            attempts += 1
            logger.info(f"Waiting for blocks to be fully loaded (attempt {attempts}/{max_attempts})...")
            time.sleep(5)  # Wait 5 seconds before checking again
        
        logger.error(f"Failed to verify all blocks {start_block}-{end_block} are in the database after {max_attempts} attempts")
        raise Exception(f"Blocks {start_block}-{end_block} not fully loaded in database")

    def _clean_data_directory(self) -> None:
        """Clean up the data directory before a new extraction."""
        try:
            # Remove all files in the data directory
            files = glob.glob(os.path.join(settings.data_dir, "*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                elif os.path.isdir(f) and not f.endswith(".cryo"):
                    shutil.rmtree(f)
            
            # Make sure .cryo directory exists for reports
            os.makedirs(os.path.join(settings.data_dir, ".cryo", "reports"), exist_ok=True)
            
            logger.debug(f"Cleaned data directory: {settings.data_dir}")
        except Exception as e:
            logger.warning(f"Error cleaning data directory: {e}", exc_info=True)
    
    def _run_cryo(self, start_block: int, end_block: int, datasets: List[str]) -> None:
        """Run Cryo to extract blockchain data for specified datasets."""
        try:
            # Process smaller batch sizes for more reliability
            batch_size = 1000 #min(100, end_block - start_block + 1)
            
            # Process in smaller batches
            current_start = start_block
            while current_start < end_block:
                current_end = min(current_start + batch_size, end_block)
                logger.info(f"Running Cryo for blocks {current_start}-{current_end}")
                
                # Build the command
                cmd = [
                    "cryo",
                    *datasets,
                    "--blocks", format_block_range(current_start, current_end),
                    "--output-dir", settings.data_dir,
                    "--rpc", settings.eth_rpc_url,
                    "--overwrite",
                    "--verbose",
                    "--requests-per-second", "1000",  # Rate limiting to avoid RPC issues
                    "--max-concurrent-requests", "5"  # Limit concurrent requests
                ]
                
                # Add network name if specified
                if settings.network_name:
                    cmd.extend(["--network-name", settings.network_name])
                    logger.debug(f"Using network name: {settings.network_name}")
                
                logger.debug(f"Running Cryo command: {' '.join(cmd)}")
                
                # Run the command with environment variables and output capture
                env = os.environ.copy()
                env["ETH_RPC_URL"] = settings.eth_rpc_url
                
                # Try running with a smaller timeout first
                try:
                    process = subprocess.run(
                        cmd,
                        check=False,
                        capture_output=True,
                        text=True,
                        env=env,
                        timeout=300  # 5 minute timeout
                    )
                except subprocess.TimeoutExpired:
                    logger.warning(f"Cryo command timed out for blocks {current_start}-{current_end}, retrying with --requests-per-second 5")
                    # Try again with lower rate limit
                    cmd = [c if c != "1000" else "5" for c in cmd]
                    process = subprocess.run(
                        cmd,
                        check=False,
                        capture_output=True,
                        text=True,
                        env=env
                    )
                
                logger.debug(f"Cryo stdout: {process.stdout}")
                
                if process.returncode != 0:
                    error_msg = process.stderr.strip() if process.stderr else "No error output captured"
                    logger.error(f"Cryo error: {error_msg}")
                    
                    # Check for common errors and provide better diagnostics
                    if "deserialization error" in process.stdout:
                        logger.error("Deserialization error detected - this may indicate a network name mismatch")
                        # Extract the specific error details from stdout
                        error_lines = [line for line in process.stdout.split('\n') if "error" in line.lower()]
                        for line in error_lines:
                            logger.error(f"Error detail: {line.strip()}")
                    
                    # Check if files were created despite the error
                    created_files = find_parquet_files(settings.data_dir, datasets[0])
                    if created_files:
                        logger.warning("Cryo reported an error but did produce some output files. Continuing.")
                    else:
                        # Try one more time without network name if we have a deserialization error
                        if "deserialization error" in process.stdout and settings.network_name:
                            logger.warning("Trying again without network name...")
                            # Remove the network name option
                            cmd_without_network = [item for i, item in enumerate(cmd) 
                                            if item != "--network-name" and 
                                            (i == 0 or cmd[i-1] != "--network-name")]
                            
                            logger.debug(f"Running command without network name: {' '.join(cmd_without_network)}")
                            retry_process = subprocess.run(
                                cmd_without_network,
                                check=False,
                                capture_output=True,
                                text=True,
                                env=env
                            )
                            
                            if retry_process.returncode == 0:
                                logger.info("Retry without network name succeeded!")
                                logger.debug(f"Cryo stdout: {retry_process.stdout}")
                                # Update current start and continue
                                current_start = current_end
                                continue
                            else:
                                logger.error(f"Retry failed: {retry_process.stderr}")
                                logger.error(f"Retry stdout: {retry_process.stdout}")
                        
                        raise Exception(f"Cryo process failed with return code {process.returncode}")
                
                logger.info(f"Successfully extracted data for blocks {current_start}-{current_end}")
                current_start = current_end
                logger.info(f"Remaining blocks to process: {current_start}-{end_block}")
            
        except subprocess.CalledProcessError as e:
            stderr_output = e.stderr if hasattr(e, 'stderr') else "No stderr captured"
            logger.error(f"Cryo process error: {stderr_output}")
            raise Exception(f"Cryo process failed: {e}")
        except Exception as e:
            logger.error(f"Error running Cryo: {e}", exc_info=True)
            raise
    
    def _load_to_clickhouse(self, datasets: List[str]) -> None:
        """Load extracted data files to ClickHouse."""
        total_rows = 0
        files_processed = 0
        files_failed = 0
        
        # Process each dataset
        for dataset in datasets:
            files = find_parquet_files(settings.data_dir, dataset)
            
            if not files:
                logger.warning(f"No parquet files found for dataset {dataset}")
                continue
                
            logger.info(f"Found {len(files)} parquet files for dataset {dataset}")
            
            # Process each file
            for file_path in files:
                try:
                    # For blocks, check if they exist first
                    if dataset == 'blocks':
                        df = read_parquet_to_pandas(file_path)
                        if not df.empty and 'block_number' in df.columns:
                            min_block = df['block_number'].min()
                            max_block = df['block_number'].max()
                            
                            # Check if any blocks in this range are already in the database
                            query = f"""
                            SELECT COUNT(DISTINCT block_number) as block_count
                            FROM {self.clickhouse.database}.blocks
                            WHERE block_number BETWEEN {min_block} AND {max_block}
                            """
                            result = self.clickhouse.client.query(query)
                            existing_count = result.result_rows[0][0] if result.result_rows else 0
                            expected_count = max_block - min_block + 1
                            
                            if existing_count == expected_count:
                                logger.info(f"All blocks {min_block}-{max_block} already exist in database, skipping insert")
                                continue

                    # Insert the file if not all blocks exist
                    rows = self.clickhouse.insert_parquet_file(file_path)
                    total_rows += rows
                    files_processed += 1
                    logger.info(f"Inserted {rows} rows from {os.path.basename(file_path)}")
                except Exception as e:
                    files_failed += 1
                    logger.error(f"Error loading file {file_path}: {e}", exc_info=True)
        
        logger.info(f"Loaded {total_rows} total rows from {files_processed} files to ClickHouse ({files_failed} files failed)")


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()