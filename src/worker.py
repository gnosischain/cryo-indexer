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
        
        # Check if this is a maintenance worker
        self.is_maintenance = worker_id.startswith('maintain')
        
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

        # Define columns for each dataset
        self.dataset_columns = {
            'blocks': [
                'block_hash',
                'parent_hash',
                'uncles_hash',
                'author',
                'state_root',
                'transactions_root',
                'receipts_root',
                'block_number',
                'gas_used',
                'gas_limit',
                'extra_data',
                'logs_bloom',
                'timestamp',
                'size',
                'mix_hash',
                'nonce',
                'base_fee_per_gas',
                'withdrawals_root',
                'withdrawals',
                'chain_id'
            ],
            'transactions': 'all',
            'logs': [
                'block_number',
                'block_hash',
                'transaction_index',
                'log_index',
                'transaction_hash',
                'address',
                'topic0',
                'topic1',
                'topic2',
                'topic3',
                'data',
                'n_data_bytes',
                'chain_id'
            ],
            'contracts': 'all',
            'native_transfers': 'all',
            'traces': 'all',
            'balance_diffs': 'all',
            'code_diffs': 'all',
            'nonce_diffs': 'all',
            'storage_diffs': 'all'
        }
        
        # Create worker-specific data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info(f"Worker {worker_id} initialized for mode {mode} (maintenance: {self.is_maintenance})")
    
    def process_range(
        self, 
        start_block: int, 
        end_block: int, 
        datasets: List[str]
    ) -> bool:
        """
        Process a block range for the given datasets.
        For maintain operations, this is bypassed - we call _extract_and_load_dataset directly.
        """
        try:
            # For maintenance operations, we don't do the normal range processing
            # because the indexer already handles state management
            if self.is_maintenance:
                logger.debug(f"Worker {self.worker_id}: Maintenance mode - skipping normal range processing")
                return True
            
            # Step 1: Process blocks if needed
            blocks_were_processed = False
            if 'blocks' in datasets:
                logger.info(f"Worker {self.worker_id}: Processing blocks {start_block}-{end_block}")
                
                # Check if blocks were actually processed (not just skipped as completed)
                status = self.state_manager.get_range_status(
                    self.mode, 'blocks', start_block, end_block
                )
                
                if status == 'completed':
                    logger.info(f"Worker {self.worker_id}: Blocks range {start_block}-{end_block} already completed")
                    blocks_were_processed = False  # They were already there, not newly processed
                else:
                    # Actually process the blocks
                    success = self._process_single_dataset('blocks', start_block, end_block)
                    if not success:
                        logger.error(f"Worker {self.worker_id}: Failed to process blocks")
                        return False
                    blocks_were_processed = True  # We just processed them
                
                # Only verify timestamps if we just processed blocks OR if they should already be valid
                if not self.state_manager.has_valid_timestamps(start_block, end_block):
                    if blocks_were_processed:
                        logger.error(f"Worker {self.worker_id}: Newly processed blocks missing valid timestamps")
                        return False
                    else:
                        # Blocks were already "completed" but have invalid timestamps
                        # This indicates a data integrity issue that needs to be fixed
                        logger.error(f"Worker {self.worker_id}: Previously completed blocks have invalid timestamps. Range needs reprocessing.")
                        # Mark the blocks range as failed so it gets reprocessed
                        self.state_manager.fail_range(
                            self.mode, 'blocks', start_block, end_block, 
                            "Previously completed blocks have invalid timestamps"
                        )
                        return False
            
            # Step 2: Process other datasets (unchanged)
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
            # For maintenance operations, skip state checking since the indexer handles it
            if not self.is_maintenance:
                # Check current state for normal operations
                status = self.state_manager.get_range_status(
                    self.mode, dataset, start_block, end_block
                )
                
                if status == 'completed':
                    logger.info(f"Worker {self.worker_id}: {dataset} range {start_block}-{end_block} already completed")
                    return True
                
                # Claim the range for normal operations
                if not self.state_manager.claim_range(
                    self.mode, dataset, start_block, end_block, self.worker_id
                ):
                    logger.warning(f"Worker {self.worker_id}: Could not claim {dataset} range {start_block}-{end_block}")
                    return False
            else:
                # For maintenance, we already claimed, so just log that we're processing
                logger.info(f"Worker {self.worker_id}: MAINTENANCE MODE - processing {dataset} {start_block}-{end_block}")
            
            # Process the dataset
            logger.info(f"Worker {self.worker_id}: Processing {dataset} {start_block}-{end_block}")
            success = self._extract_and_load_dataset(start_block, end_block, [dataset])
            
            if not success:
                if not self.is_maintenance:
                    self.state_manager.fail_range(
                        self.mode, dataset, start_block, end_block, "Processing failed"
                    )
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {dataset}: {e}")
            if not self.is_maintenance:
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
            
            # ALWAYS mark as completed when processing succeeds (including maintenance)
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
        
        # Add column specification based on datasets
        columns_to_use = self._determine_columns(datasets)
        if columns_to_use and columns_to_use != 'all':
            # Split the space-separated string and add each column as a separate argument
            cmd.extend(["--columns"] + columns_to_use.split())
            logger.info(f"Worker {self.worker_id}: Using columns specification: {columns_to_use}")
        elif columns_to_use == 'all':
            cmd.extend(["--columns", "all"])

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

    def _determine_columns(self, datasets: List[str]) -> Optional[str]:
        """
        Determine the columns specification for the given datasets.
        Returns None if no specification needed, or a space-separated string of columns.
        """
        # If we have multiple datasets with different column specs, we can't specify columns
        # because cryo processes all datasets with the same column specification
        if len(datasets) > 1:
            # Check if all datasets have the same column specification
            first_spec = self.dataset_columns.get(datasets[0], 'all')
            for dataset in datasets[1:]:
                if self.dataset_columns.get(dataset, 'all') != first_spec:
                    # Different specs, can't use column specification
                    logger.debug(f"Worker {self.worker_id}: Mixed column specifications, using default columns")
                    return None
            
            # All datasets have the same spec
            if isinstance(first_spec, list):
                return " ".join(first_spec)  # SPACE-SEPARATED, not comma-separated!
            elif first_spec == 'all':
                return 'all'
            else:
                return first_spec
        
        # Single dataset
        dataset = datasets[0]
        columns = self.dataset_columns.get(dataset, 'all')
        
        if isinstance(columns, list):
            return " ".join(columns)  # SPACE-SEPARATED, not comma-separated!
        elif columns == 'all':
            return 'all'
        else:
            return columns
    
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