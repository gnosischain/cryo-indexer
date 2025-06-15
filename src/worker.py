import os
import time
import glob
import shutil
import subprocess
from typing import Dict, Any, List, Optional
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from .core.blockchain import BlockchainClient
from .core.state_manager import StateManager
from .core.utils import find_parquet_files, format_block_range
from .db.clickhouse_manager import ClickHouseManager
from .config import settings


class IndexerWorker:
    """Stateless worker that processes block ranges."""
    
    def __init__(
        self, 
        worker_id: str,
        blockchain: BlockchainClient,
        clickhouse: ClickHouseManager,
        state_manager: StateManager,
        data_dir: str,
        network_name: str,
        rpc_url: str,
        mode: str,
        batch_id: str = ""
    ):
        self.worker_id = worker_id
        self.blockchain = blockchain
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.data_dir = os.path.join(data_dir, f"worker_{worker_id}")
        self.network_name = network_name
        self.rpc_url = rpc_url
        self.mode = mode
        self.batch_id = batch_id
        
        # Create worker-specific data directory
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, ".cryo", "reports"), exist_ok=True)
        
        logger.info(f"Worker {worker_id} initialized for mode {mode}")
    
    def _verify_blocks_exist(self, start_block: int, end_block: int) -> bool:
        """Check if all blocks in range exist in database with valid timestamps."""
        try:
            client = self.clickhouse._connect()
            query = f"""
            SELECT COUNT(DISTINCT block_number) as count
            FROM {self.clickhouse.database}.blocks
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND timestamp IS NOT NULL
              AND timestamp > 0
            """
            result = client.query(query)
            expected_blocks = end_block - start_block
            actual_blocks = result.result_rows[0][0] if result.result_rows else 0
            
            if actual_blocks < expected_blocks:
                logger.warning(
                    f"Missing blocks: found {actual_blocks}/{expected_blocks} blocks "
                    f"in range {start_block}-{end_block}"
                )
                
            return actual_blocks == expected_blocks
        except Exception as e:
            logger.error(f"Error verifying blocks: {e}")
            return False
    
    def process_range(
        self, 
        start_block: int, 
        end_block: int, 
        datasets: List[str],
        force: bool = False
    ) -> bool:
        """
        Process a block range for the given datasets.
        
        Args:
            start_block: Starting block number
            end_block: Ending block number (exclusive)
            datasets: List of datasets to process
            force: Force processing even if already completed
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Define diff datasets that need special handling
            diff_datasets = ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']
            
            # Check if we need blocks first (unless we're processing blocks)
            if 'blocks' not in datasets and not self._verify_blocks_exist(start_block, end_block):
                if settings.strict_timestamp_mode:
                    logger.error(
                        f"Cannot process {datasets} without blocks {start_block}-{end_block}. "
                        f"Blocks must be indexed first in strict mode."
                    )
                    return False
                else:
                    logger.warning(
                        f"Blocks {start_block}-{end_block} not fully available. "
                        f"Processing anyway, but timestamps may be incorrect."
                    )
            
            # In force mode, process blocks first if not already done
            if force and 'blocks' not in datasets and not self._verify_blocks_exist(start_block, end_block):
                logger.info(f"Force mode: Processing blocks first for range {start_block}-{end_block}")
                
                # Process blocks first
                if not self._process_dataset_range(start_block, end_block, ['blocks']):
                    logger.error(f"Failed to process blocks {start_block}-{end_block}")
                    if settings.strict_timestamp_mode:
                        return False
            
            # In force mode, just process everything without checking
            if force:
                logger.info(f"Worker {self.worker_id}: Force processing {start_block}-{end_block} for datasets: {datasets}")
                
                # Clean data directory
                self._clean_data_directory()
                
                # Extract all datasets at once
                logger.info(f"Worker {self.worker_id}: Extracting all datasets for blocks {start_block}-{end_block}")
                self._run_cryo(start_block, end_block, datasets)
                
                # Load data to ClickHouse
                total_rows = self._load_to_clickhouse(datasets)
                
                # Mark as completed in the state
                for dataset in datasets:
                    # Adjust start block for diff datasets when recording in state
                    dataset_start = start_block
                    if dataset in diff_datasets and start_block == 0:
                        dataset_start = 1
                    
                    self.state_manager.complete_range(
                        self.mode, dataset, dataset_start, end_block, total_rows
                    )
                
                logger.info(f"Worker {self.worker_id}: Successfully force-processed {start_block}-{end_block} ({total_rows} rows)")
                return True
            
            # Non-force mode: check and claim ranges
            # Split datasets into blocks and others
            blocks_dataset = ['blocks'] if 'blocks' in datasets else []
            other_datasets = [d for d in datasets if d != 'blocks']
            
            # Always process blocks first if included
            if blocks_dataset:
                # Check if blocks are already processed
                if not self.state_manager.should_process_range(
                    self.mode, 'blocks', start_block, end_block
                ):
                    logger.info(f"Worker {self.worker_id}: Blocks {start_block}-{end_block} already completed")
                else:
                    # Claim and process blocks
                    if self.state_manager.claim_range(
                        self.mode, 'blocks', start_block, end_block, 
                        self.worker_id, self.batch_id
                    ):
                        logger.info(f"Worker {self.worker_id}: Processing blocks {start_block}-{end_block}")
                        if not self._process_dataset_range(start_block, end_block, blocks_dataset):
                            return False
                    else:
                        logger.warning(f"Worker {self.worker_id}: Could not claim blocks range {start_block}-{end_block}")
                        return True  # Someone else is processing it
            
            # Now process other datasets
            for dataset in other_datasets:
                # Adjust start block for diff datasets
                dataset_start = start_block
                if dataset in diff_datasets and start_block == 0:
                    dataset_start = 1
                    logger.info(f"Worker {self.worker_id}: Adjusted start block for {dataset} from 0 to 1")
                
                # Check if we should process this dataset
                if not self.state_manager.should_process_range(
                    self.mode, dataset, dataset_start, end_block
                ):
                    logger.info(f"Worker {self.worker_id}: {dataset} range {dataset_start}-{end_block} already completed")
                    continue
                
                # Claim the range
                if self.state_manager.claim_range(
                    self.mode, dataset, dataset_start, end_block, 
                    self.worker_id, self.batch_id
                ):
                    logger.info(f"Worker {self.worker_id}: Processing {dataset} {dataset_start}-{end_block}")
                    if not self._process_dataset_range(dataset_start, end_block, [dataset]):
                        # Mark as failed but continue with other datasets
                        self.state_manager.fail_range(
                            self.mode, dataset, dataset_start, end_block, 
                            "Processing failed"
                        )
                else:
                    logger.warning(f"Worker {self.worker_id}: Could not claim {dataset} range {dataset_start}-{end_block}")
                    continue  # Someone else is processing it
            
            logger.info(f"Worker {self.worker_id}: Successfully completed all datasets for {start_block}-{end_block}")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {start_block}-{end_block}: {e}", exc_info=True)
            
            # Define diff datasets here too for error handling
            diff_datasets = ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']
            
            # Mark all datasets as failed if in force mode
            if force:
                for dataset in datasets:
                    try:
                        # Adjust start block for diff datasets
                        dataset_start = start_block
                        if dataset in diff_datasets and start_block == 0:
                            dataset_start = 1
                        
                        self.state_manager.fail_range(
                            self.mode, dataset, dataset_start, end_block, str(e)[:500]
                        )
                    except:
                        pass
            
            return False
    
    def _process_dataset_range(
        self,
        start_block: int,
        end_block: int,
        datasets: List[str]
    ) -> bool:
        """Process a specific dataset range."""
        try:
            # Clean data directory
            self._clean_data_directory()
            
            # Extract data
            logger.info(f"Worker {self.worker_id}: Extracting {datasets} for blocks {start_block}-{end_block}")
            self._run_cryo(start_block, end_block, datasets)
            
            # Load data to ClickHouse
            total_rows = self._load_to_clickhouse(datasets)
            
            # Mark as completed
            for dataset in datasets:
                self.state_manager.complete_range(
                    self.mode, dataset, start_block, end_block, total_rows
                )
            
            logger.info(f"Worker {self.worker_id}: Successfully processed {datasets} for {start_block}-{end_block} ({total_rows} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {datasets} for {start_block}-{end_block}: {e}", exc_info=True)
            return False
    
    def _clean_data_directory(self) -> None:
        """Clean the worker's data directory."""
        try:
            # Remove all parquet files
            for f in glob.glob(os.path.join(self.data_dir, "*.parquet")):
                os.remove(f)
                
            # Clean subdirectories except .cryo
            for d in os.listdir(self.data_dir):
                full_path = os.path.join(self.data_dir, d)
                if os.path.isdir(full_path) and d != ".cryo":
                    shutil.rmtree(full_path)
                    
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Error cleaning directory: {e}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def _run_cryo(self, start_block: int, end_block: int, datasets: List[str]) -> None:
        """Run Cryo to extract blockchain data."""
        try:
            # Define diff datasets
            diff_datasets = ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']
            
            # Adjust start block for diff datasets
            adjusted_start = start_block
            if start_block == 0 and any(d in diff_datasets for d in datasets):
                # Check if we have any diff datasets
                has_diff = False
                has_non_diff = False
                for d in datasets:
                    if d in diff_datasets:
                        has_diff = True
                    else:
                        has_non_diff = True
                
                if has_diff and not has_non_diff:
                    # All datasets are diffs, adjust start
                    adjusted_start = 1
                    logger.info(f"Worker {self.worker_id}: Adjusted start block from 0 to 1 for diff datasets")
                elif has_diff and has_non_diff:
                    # Mixed datasets - need to run cryo twice
                    logger.info(f"Worker {self.worker_id}: Mixed diff and non-diff datasets, running cryo twice")
                    
                    # First run non-diff datasets
                    non_diff_datasets = [d for d in datasets if d not in diff_datasets]
                    if non_diff_datasets:
                        self._run_cryo_command(start_block, end_block, non_diff_datasets)
                    
                    # Then run diff datasets with adjusted start
                    diff_datasets_to_run = [d for d in datasets if d in diff_datasets]
                    if diff_datasets_to_run:
                        self._run_cryo_command(1, end_block, diff_datasets_to_run)
                    
                    return
            
            # Normal case or already adjusted
            self._run_cryo_command(adjusted_start, end_block, datasets)
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Cryo error: {e}")
            raise
    
    def _run_cryo_command(self, start_block: int, end_block: int, datasets: List[str]) -> None:
        """Execute the actual cryo command."""
        cmd = [
            "cryo",
            *datasets,
            "--blocks", format_block_range(start_block, end_block),
            "--output-dir", self.data_dir,
            "--rpc", self.rpc_url,
            "--overwrite",
            "--requests-per-second", str(settings.requests_per_second),
            "--max-concurrent-requests", str(settings.max_concurrent_requests)
        ]
        
        if self.network_name:
            cmd.extend(["--network-name", self.network_name])
        
        logger.debug(f"Worker {self.worker_id}: Running {' '.join(cmd)}")
        
        process = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=settings.cryo_timeout
        )
        
        if process.returncode != 0:
            error_msg = f"Cryo failed with code {process.returncode}: {process.stderr}"
            logger.error(f"Worker {self.worker_id}: {error_msg}")
            logger.error(f"stdout: {process.stdout}")
            raise Exception(error_msg)
    
    def _load_to_clickhouse(self, datasets: List[str]) -> int:
        """Load extracted data to ClickHouse."""
        total_rows = 0
        
        for dataset in datasets:
            files = find_parquet_files(self.data_dir, dataset)
            
            if not files:
                logger.warning(f"Worker {self.worker_id}: No files found for {dataset}")
                continue
                
            for file_path in files:
                try:
                    rows = self.clickhouse.insert_parquet_file(file_path)
                    total_rows += rows
                    logger.info(f"Worker {self.worker_id}: Loaded {rows} rows from {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"Worker {self.worker_id}: Error loading {file_path}: {e}")
                    raise
                    
        return total_rows