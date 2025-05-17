import os
import time
import glob
import shutil
import logging
import subprocess
import concurrent.futures
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .utils import find_parquet_files, format_block_range, read_parquet_to_pandas, save_state


class IndexerWorker:
    """
    Unified worker that processes a specific range of blocks.
    Can be used by both sequential and parallel indexing modes.
    """
    
    def __init__(
        self, 
        worker_id: str,
        blockchain: BlockchainClient,
        clickhouse: ClickHouseManager,
        data_dir: str,
        network_name: str,
        rpc_url: str,
        state: Optional[Dict] = None,
        state_file: Optional[str] = None
    ):
        """
        Initialize the worker with required components.
        
        Args:
            worker_id: Unique identifier for this worker
            blockchain: Blockchain client for RPC interaction
            clickhouse: ClickHouse manager for database interaction
            data_dir: Directory for data storage
            network_name: Name of the blockchain network
            rpc_url: URL for the blockchain RPC endpoint
            state: Optional state dictionary (for sequential mode)
            state_file: Optional state file path (for sequential mode)
        """
        self.worker_id = worker_id
        self.blockchain = blockchain
        self.clickhouse = clickhouse
        self.data_dir = os.path.join(data_dir, f"worker_{worker_id}")
        self.network_name = network_name
        self.rpc_url = rpc_url
        self.state = state
        self.state_file = state_file
        
        # Performance options
        self.max_batch_size = int(os.environ.get("WORKER_CRYO_BATCH_SIZE", "1000"))
        self.max_concurrent_loads = int(os.environ.get("WORKER_CONCURRENT_LOADS", "3"))
        self.parallel_loading = os.environ.get("WORKER_PARALLEL_LOADING", "true").lower() == "true"
        
        # Create worker-specific data directory
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, ".cryo", "reports"), exist_ok=True)
        
        logger.info(f"Worker {worker_id} initialized with data directory: {self.data_dir}")
    
    def process_block_range(self, start_block: int, end_block: int, datasets: List[str]) -> bool:
        """
        Process a specific range of blocks for the given datasets.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number (exclusive)
            datasets: List of dataset names to process
            
        Returns:
            bool: True if processing was successful, False otherwise
        """
        try:
            logger.info(f"Worker {self.worker_id} processing blocks {start_block}-{end_block}")
            
            # First check for potential reorgs if we're in sequential mode
            if self.state is not None:
                self._handle_potential_reorg(start_block)
            
            # Clean up data directory before starting
            self._clean_data_directory()
            
            # Split datasets into categories
            blocks_datasets = ['blocks'] if 'blocks' in datasets else []
            other_datasets = [d for d in datasets if d != 'blocks']
            state_diff_datasets = [d for d in other_datasets if d in ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']]
            non_state_diff_datasets = [d for d in other_datasets if d not in state_diff_datasets]
            
            # Process in smaller batches to improve reliability and performance
            current_start = start_block
            batch_end = min(end_block, current_start + self.max_batch_size)
            
            while current_start < end_block:
                # Always process blocks first for timestamp reference
                if blocks_datasets:
                    logger.info(f"Worker {self.worker_id}: Processing blocks {current_start}-{batch_end}")
                    self._run_cryo(current_start, batch_end, blocks_datasets)
                    self._load_to_clickhouse(blocks_datasets)
                    
                    # Verify blocks are loaded before continuing
                    self._verify_blocks_loaded(current_start, batch_end)
                
                # Process non-state diff datasets 
                if non_state_diff_datasets:
                    logger.info(f"Worker {self.worker_id}: Processing non-state diff datasets: {non_state_diff_datasets}")
                    self._run_cryo(current_start, batch_end, non_state_diff_datasets)
                    self._load_to_clickhouse(non_state_diff_datasets)
                
                # Process state diff datasets (starting from block 1 or higher)
                if state_diff_datasets:
                    state_diff_start = max(current_start, 1)  # Ensure state diffs start at least at block 1
                    
                    if state_diff_start <= batch_end:  # Only process if there's a valid range
                        logger.info(f"Worker {self.worker_id}: Processing state diff datasets for blocks {state_diff_start}-{batch_end}")
                        self._run_cryo(state_diff_start, batch_end, state_diff_datasets)
                        self._load_to_clickhouse(state_diff_datasets)
                
                # Update for next batch
                current_start = batch_end
                batch_end = min(end_block, current_start + self.max_batch_size)
                
                # Update state if we're in sequential mode
                if self.state is not None:
                    self.state['last_block'] = current_start
                    if self.state_file is not None:
                        save_state(self.state, os.path.dirname(self.state_file), os.path.basename(self.state_file))
            
            logger.info(f"Worker {self.worker_id} successfully processed blocks {start_block}-{end_block}")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} error processing blocks {start_block}-{end_block}: {e}", exc_info=True)
            return False
    
    def _clean_data_directory(self) -> None:
        """Clean up the worker's data directory."""
        try:
            # Remove all files in the data directory
            files = glob.glob(os.path.join(self.data_dir, "*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                elif os.path.isdir(f) and not f.endswith(".cryo"):
                    shutil.rmtree(f)
            
            # Make sure .cryo directory exists for reports
            os.makedirs(os.path.join(self.data_dir, ".cryo", "reports"), exist_ok=True)
            
            logger.debug(f"Worker {self.worker_id} cleaned data directory: {self.data_dir}")
        except Exception as e:
            logger.warning(f"Worker {self.worker_id} error cleaning data directory: {e}", exc_info=True)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def _run_cryo(self, start_block: int, end_block: int, datasets: List[str]) -> None:
        """Run Cryo to extract blockchain data for specified datasets."""
        try:
            # Build the command
            cmd = [
                "cryo",
                *datasets,
                "--blocks", format_block_range(start_block, end_block),
                "--output-dir", self.data_dir,
                "--rpc", self.rpc_url,
                "--overwrite",
                "--verbose",
                "--requests-per-second", "50",  # Rate limiting to avoid RPC issues
                "--max-concurrent-requests", "5"  # Limit concurrent requests
            ]
            
            # Add network name if specified
            if self.network_name:
                cmd.extend(["--network-name", self.network_name])
            
            logger.debug(f"Worker {self.worker_id} running Cryo command: {' '.join(cmd)}")
            
            # Run the command with environment variables
            env = os.environ.copy()
            env["ETH_RPC_URL"] = self.rpc_url
            
            # Run the command with a timeout
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
                logger.warning(f"Worker {self.worker_id} Cryo command timed out, retrying with lower rate limit")
                # Try again with lower rate limit
                cmd = [c if c != "500" else "100" for c in cmd]
                process = subprocess.run(
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True,
                    env=env
                )
            
            if process.returncode != 0:
                error_msg = process.stderr.strip() if process.stderr else "No error output captured"
                logger.error(f"Worker {self.worker_id} Cryo error: {error_msg}")
                
                # Check if files were created despite the error
                created_files = find_parquet_files(self.data_dir, datasets[0])
                if not created_files:
                    raise Exception(f"Cryo process failed with return code {process.returncode}")
                else:
                    logger.warning(f"Worker {self.worker_id} Cryo reported an error but produced some output files. Continuing.")
            
            logger.info(f"Worker {self.worker_id} successfully extracted data for blocks {start_block}-{end_block}")
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} error running Cryo: {e}", exc_info=True)
            raise
    
    def _load_file_to_clickhouse(self, file_path: str) -> int:
        """Load a single file to ClickHouse."""
        try:
            rows = self.clickhouse.insert_parquet_file(file_path)
            logger.info(f"Worker {self.worker_id} inserted {rows} rows from {os.path.basename(file_path)}")
            return rows
        except Exception as e:
            logger.error(f"Worker {self.worker_id} error loading file {file_path}: {e}", exc_info=True)
            return 0
    
    def _load_to_clickhouse(self, datasets: List[str]) -> None:
        """Load extracted data files to ClickHouse."""
        total_rows = 0
        files_processed = 0
        files_failed = 0
        
        # Process each dataset
        for dataset in datasets:
            files = find_parquet_files(self.data_dir, dataset)
            
            if not files:
                logger.warning(f"Worker {self.worker_id} no parquet files found for dataset {dataset}")
                continue
                
            logger.info(f"Worker {self.worker_id} found {len(files)} parquet files for dataset {dataset}")
            
            # Process files in parallel or sequentially based on configuration
            if self.parallel_loading and len(files) > 1:
                # Use a thread pool to load files in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent_loads) as executor:
                    future_to_file = {
                        executor.submit(self._load_file_to_clickhouse, file_path): file_path 
                        for file_path in files
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_file):
                        file_path = future_to_file[future]
                        try:
                            rows = future.result()
                            total_rows += rows
                            files_processed += 1
                        except Exception as e:
                            files_failed += 1
                            logger.error(f"Worker {self.worker_id} error loading file {file_path}: {e}", exc_info=True)
            else:
                # Process files sequentially
                for file_path in files:
                    try:
                        rows = self._load_file_to_clickhouse(file_path)
                        total_rows += rows
                        files_processed += 1
                    except Exception as e:
                        files_failed += 1
                        logger.error(f"Worker {self.worker_id} error loading file {file_path}: {e}", exc_info=True)
        
        logger.info(f"Worker {self.worker_id} loaded {total_rows} total rows from {files_processed} files to ClickHouse ({files_failed} files failed)")
    
    def _verify_blocks_loaded(self, start_block: int, end_block: int) -> None:
        """Verify that blocks are loaded in the database before proceeding."""
        max_attempts = 5
        attempts = 0
        
        while attempts < max_attempts:
            logger.info(f"Worker {self.worker_id} verifying blocks {start_block}-{end_block} are loaded...")
            
            try:
                # Modified query to count DISTINCT block numbers
                client = self.clickhouse.connection_pool.get_client()
                query_result = client.query(f"""
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
                        logger.info(f"Worker {self.worker_id} verified all {distinct_blocks} blocks in database")
                        return
                    else:
                        logger.warning(f"Worker {self.worker_id} only {distinct_blocks}/{expected_count} distinct blocks found. Min: {min_block}, Max: {max_block}")
                else:
                    logger.warning(f"Worker {self.worker_id} no blocks found in database for verification query")
            except Exception as e:
                logger.warning(f"Worker {self.worker_id} error verifying blocks: {e}")
            
            attempts += 1
            logger.info(f"Worker {self.worker_id} waiting for blocks to be fully loaded (attempt {attempts}/{max_attempts})...")
            time.sleep(5)  # Wait 5 seconds before checking again
        
        logger.error(f"Worker {self.worker_id} failed to verify all blocks {start_block}-{end_block} are in the database after {max_attempts} attempts")
        raise Exception(f"Blocks {start_block}-{end_block} not fully loaded in database")
    
    def _handle_potential_reorg(self, current_block: int) -> None:
        """
        Check for and handle any blockchain reorganizations.
        Only used in sequential mode.
        """
        if self.state is None or self.blockchain is None or self.clickhouse is None:
            return
            
        try:
            # Only check for reorgs if we've processed some blocks already
            if current_block <= 0:
                return
                
            # Get a range of blocks from our database to check against
            check_start = max(0, current_block - 10)
            db_blocks = self.clickhouse.get_blocks_in_range(check_start, current_block)
            
            if not db_blocks:
                logger.warning(f"Worker {self.worker_id}: No blocks found in database for reorg check")
                return
                
            # Check if there's a reorg
            reorg_detected, common_ancestor = self.blockchain.detect_reorg(
                current_block, 
                db_blocks
            )
            
            if reorg_detected:
                logger.warning(f"Worker {self.worker_id}: Reorg detected! Rolling back from block {current_block} to {common_ancestor}")
                
                # Roll back the database
                success = self.clickhouse.rollback_to_block(common_ancestor)
                
                if success:
                    # Update our state
                    if self.state is not None:
                        self.state['last_block'] = common_ancestor
                        if self.state_file is not None:
                            save_state(self.state, os.path.dirname(self.state_file), os.path.basename(self.state_file))
                        logger.info(f"Worker {self.worker_id}: Successfully rolled back to block {common_ancestor}")
                else:
                    logger.error(f"Worker {self.worker_id}: Failed to roll back database during reorg handling")
        
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error handling potential reorg: {e}", exc_info=True)