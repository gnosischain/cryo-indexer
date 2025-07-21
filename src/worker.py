import os
import time
import glob
import shutil
import subprocess
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from .core.blockchain import BlockchainClient
from .core.state_manager import StateManager
from .core.utils import find_parquet_files, format_block_range
from .db.clickhouse_manager import ClickHouseManager
from .config import settings


class IndexerWorker:
    """Simplified worker for processing block ranges."""
    
    def __init__(
        self, 
        worker_id: str,
        blockchain: BlockchainClient,
        clickhouse: ClickHouseManager,
        state_manager: StateManager,
        data_dir: str,
        network_name: str,
        rpc_url: str,
        mode: str
    ):
        self.worker_id = worker_id
        self.blockchain = blockchain
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.data_dir = os.path.join(data_dir, f"worker_{worker_id}")
        self.network_name = network_name
        self.rpc_url = rpc_url
        self.mode = mode
        
        # Table mappings
        self.table_mappings = {
            'blocks': 'blocks',
            'transactions': 'transactions',
            'logs': 'logs',
            'contracts': 'contracts',
            'native_transfers': 'native_transfers',
            'traces': 'traces',
            'balance_diffs': 'balance_diffs',
            'code_diffs': 'code_diffs',
            'nonce_diffs': 'nonce_diffs',
            'storage_diffs': 'storage_diffs'
        }
        
        # Create worker-specific data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info(f"Worker {worker_id} initialized for mode {mode}")
    
    def process_range(
        self, 
        start_block: int, 
        end_block: int, 
        datasets: List[str]
    ) -> bool:
        """
        Process a block range for the given datasets.
        Simplified: process blocks first, then other datasets.
        """
        try:
            # Step 1: Process blocks if needed
            if 'blocks' in datasets:
                logger.info(f"Worker {self.worker_id}: Processing blocks {start_block}-{end_block}")
                
                if not self._process_single_dataset('blocks', start_block, end_block):
                    logger.error(f"Worker {self.worker_id}: Failed to process blocks")
                    return False
                
                # Verify blocks have valid timestamps
                if not self.state_manager.has_valid_timestamps(start_block, end_block):
                    logger.error(f"Worker {self.worker_id}: Blocks missing valid timestamps")
                    return False
            
            # Step 2: Process other datasets
            other_datasets = [d for d in datasets if d != 'blocks']
            for dataset in other_datasets:
                # Adjust start block for diff datasets
                dataset_start = start_block
                if dataset in ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs'] and start_block == 0:
                    dataset_start = 1
                    logger.info(f"Worker {self.worker_id}: Adjusted start block for {dataset} from 0 to 1")
                
                if not self._process_single_dataset(dataset, dataset_start, end_block):
                    logger.error(f"Worker {self.worker_id}: Failed to process {dataset}")
                    return False
            
            logger.info(f"Worker {self.worker_id}: Successfully processed all datasets for {start_block}-{end_block}")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {start_block}-{end_block}: {e}")
            return False
    
    def _process_single_dataset(
        self, 
        dataset: str, 
        start_block: int, 
        end_block: int
    ) -> bool:
        """Process a single dataset for a block range."""
        try:
            # Check current state
            status = self.state_manager.get_range_status(
                self.mode, dataset, start_block, end_block
            )
            
            # Skip if already completed
            if status == 'completed':
                logger.info(f"Worker {self.worker_id}: {dataset} range {start_block}-{end_block} already completed")
                return True
            
            # Claim the range
            if not self.state_manager.claim_range(
                self.mode, dataset, start_block, end_block, self.worker_id
            ):
                logger.warning(f"Worker {self.worker_id}: Could not claim {dataset} range {start_block}-{end_block}")
                return False
            
            # Process the dataset
            logger.info(f"Worker {self.worker_id}: Processing {dataset} {start_block}-{end_block}")
            success = self._extract_and_load_dataset(start_block, end_block, [dataset])
            
            if not success:
                self.state_manager.fail_range(
                    self.mode, dataset, start_block, end_block, "Processing failed"
                )
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {dataset}: {e}")
            self.state_manager.fail_range(
                self.mode, dataset, start_block, end_block, str(e)
            )
            return False
    
    def _extract_and_load_dataset(
        self,
        start_block: int,
        end_block: int,
        datasets: List[str]
    ) -> bool:
        """Extract data using Cryo and load to ClickHouse."""
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
            
            logger.info(f"Worker {self.worker_id}: Successfully processed {datasets} ({total_rows} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error extracting/loading {datasets}: {e}")
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
            # Handle diff datasets that can't start from block 0
            diff_datasets = ['balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs']
            
            # Adjust start block for diff datasets
            adjusted_start = start_block
            if start_block == 0 and any(d in diff_datasets for d in datasets):
                if all(d in diff_datasets for d in datasets):
                    # All datasets are diffs, adjust start
                    adjusted_start = 1
                    logger.info(f"Worker {self.worker_id}: Adjusted start block from 0 to 1 for diff datasets")
                else:
                    # Mixed datasets - run cryo twice
                    logger.info(f"Worker {self.worker_id}: Mixed datasets, running cryo twice")
                    
                    # First run non-diff datasets
                    non_diff_datasets = [d for d in datasets if d not in diff_datasets]
                    if non_diff_datasets:
                        self._run_cryo_command(start_block, end_block, non_diff_datasets)
                    
                    # Then run diff datasets with adjusted start
                    diff_datasets_to_run = [d for d in datasets if d in diff_datasets]
                    if diff_datasets_to_run:
                        self._run_cryo_command(1, end_block, diff_datasets_to_run)
                    
                    return
            
            # Normal case
            self._run_cryo_command(adjusted_start, end_block, datasets)
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Cryo error: {e}")
            raise
    
    def _run_cryo_command(self, start_block: int, end_block: int, datasets: List[str]) -> None:
        """Execute the cryo command."""
        # Check for unreasonably large ranges
        block_range = end_block - start_block
        if block_range > 10000:
            logger.warning(
                f"Worker {self.worker_id}: Large block range detected ({block_range} blocks). "
                f"Consider using smaller ranges to avoid timeouts."
            )
        
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
        
        try:
            process = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=settings.cryo_timeout
            )
            
            if process.returncode != 0:
                # Check if it's just an empty dataset
                error_output = (process.stderr or "") + (process.stdout or "")
                
                if any(pattern in error_output.lower() for pattern in [
                    "no data", "empty", "zero", "0 items"
                ]):
                    logger.warning(f"Worker {self.worker_id}: No data found for {datasets} in blocks {start_block}-{end_block}")
                    return
                
                # For actual errors, raise
                error_msg = f"Cryo failed with code {process.returncode}: {process.stderr}"
                logger.error(f"Worker {self.worker_id}: {error_msg}")
                logger.error(f"stdout: {process.stdout}")
                raise Exception(error_msg)
                
        except subprocess.TimeoutExpired:
            logger.error(
                f"Worker {self.worker_id}: Cryo command timed out after {settings.cryo_timeout}s "
                f"for range {start_block}-{end_block} ({block_range} blocks). "
                f"Consider reducing batch size or increasing CRYO_TIMEOUT."
            )
            raise
    
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