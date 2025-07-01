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
    """Stateless worker that processes block ranges with deduplication support."""
    
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
        
        # Define priority order for datasets
        self.dataset_priority = {
            'blocks': 0,
            'transactions': 1,
            'logs': 2,
            'contracts': 3,
            'native_transfers': 4,
            'traces': 5,
            'balance_diffs': 6,
            'code_diffs': 7,
            'nonce_diffs': 8,
            'storage_diffs': 9
        }
        
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
            'storage_diffs': 'storage_diffs',
            'events': 'logs',  # alias
            'txs': 'transactions',  # alias
        }
        
        # Define columns for each dataset
        # For blocks, we explicitly exclude total_difficulty columns to avoid overflow
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
                'chain_id'
            ],
            # For other datasets, we can use 'all' or specify columns as needed
            'transactions': 'all',
            'logs': 'all',
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
        os.makedirs(os.path.join(self.data_dir, ".cryo", "reports"), exist_ok=True)
        
        logger.info(f"Worker {worker_id} initialized for mode {mode}")
        logger.info(f"Delete before reprocess: {settings.delete_before_reprocess}")
    
    def _check_blocks_completed(self, start_block: int, end_block: int) -> bool:
        """
        Check if blocks are marked as completed in indexing_state (any mode).
        """
        try:
            client = self.clickhouse._connect()
            
            # Check if ANY mode has completed these blocks
            query = f"""
            SELECT COUNT(*) 
            FROM {self.clickhouse.database}.indexing_state
            WHERE dataset = 'blocks'
              AND start_block = {start_block}
              AND end_block = {end_block}
              AND status = 'completed'
            """
            result = client.query(query)
            return result.result_rows[0][0] > 0
            
        except Exception as e:
            logger.error(f"Error checking blocks completion: {e}")
            return False
    
    def _delete_existing_data(self, start_block: int, end_block: int, datasets: List[str]) -> Dict[str, int]:
        """
        Delete existing data in the range before reprocessing.
        Returns dict of dataset -> deleted_rows
        """
        deleted_stats = {}
        
        if not settings.delete_before_reprocess:
            return deleted_stats
            
        try:
            client = self.clickhouse._connect()
            
            for dataset in datasets:
                table_name = self.table_mappings.get(dataset, dataset)
                
                # Check if table exists
                check_query = f"""
                SELECT COUNT(*) FROM system.tables 
                WHERE database = '{self.clickhouse.database}' 
                AND name = '{table_name}'
                """
                result = client.query(check_query)
                
                if result.result_rows[0][0] == 0:
                    logger.debug(f"Table {table_name} doesn't exist, skipping deletion")
                    deleted_stats[dataset] = 0
                    continue
                
                # Count existing rows
                count_query = f"""
                SELECT COUNT(*) 
                FROM {self.clickhouse.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                """
                result = client.query(count_query)
                existing_rows = result.result_rows[0][0]
                deleted_stats[dataset] = existing_rows
                
                if existing_rows > 0:
                    logger.info(
                        f"Worker {self.worker_id}: Deleting {existing_rows} existing rows "
                        f"from {table_name} for range {start_block}-{end_block}"
                    )
                    
                    # Delete existing data
                    delete_query = f"""
                    ALTER TABLE {self.clickhouse.database}.{table_name}
                    DELETE WHERE block_number >= {start_block} 
                      AND block_number < {end_block}
                    """
                    client.command(delete_query)
                    
                    # Wait for deletion to propagate
                    time.sleep(settings.deletion_wait_time)
                else:
                    logger.debug(f"No existing data in {table_name} for range {start_block}-{end_block}")
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error deleting existing data: {e}")
            # Continue anyway - ReplacingMergeTree will handle duplicates eventually
            
        return deleted_stats
    
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
            
            # Sort datasets by priority
            sorted_datasets = sorted(datasets, key=lambda d: self.dataset_priority.get(d, 999))
            
            # If we need blocks but they're not in our dataset list, check if they're completed
            if 'blocks' not in sorted_datasets:
                if not self._check_blocks_completed(start_block, end_block):
                    logger.info(f"Worker {self.worker_id}: Blocks {start_block}-{end_block} not completed, adding to datasets")
                    sorted_datasets.insert(0, 'blocks')
            
            # Process each dataset
            for dataset in sorted_datasets:
                # Adjust start block for diff datasets
                dataset_start = start_block
                if dataset in diff_datasets and start_block == 0:
                    dataset_start = 1
                    logger.info(f"Worker {self.worker_id}: Adjusted start block for {dataset} from 0 to 1")
                
                # Check current state
                status = self.state_manager.get_range_status(
                    self.mode, dataset, dataset_start, end_block
                )
                
                # Skip if already completed (unless force mode)
                if status == 'completed' and not force:
                    logger.info(f"Worker {self.worker_id}: {dataset} range {dataset_start}-{end_block} already completed")
                    continue
                
                # Skip if being processed by another worker (and not stale)
                if status == 'processing':
                    logger.info(f"Worker {self.worker_id}: {dataset} range {dataset_start}-{end_block} being processed by another worker")
                    continue
                
                # If status is 'stale', we can claim it
                if status == 'stale':
                    logger.info(f"Worker {self.worker_id}: {dataset} range {dataset_start}-{end_block} has stale processing status, reclaiming")
                
                # Claim the range
                if not self.state_manager.claim_range(
                    self.mode, dataset, dataset_start, end_block, 
                    self.worker_id, self.batch_id
                ):
                    logger.warning(f"Worker {self.worker_id}: Could not claim {dataset} range {dataset_start}-{end_block}")
                    continue
                
                # Process the dataset
                logger.info(f"Worker {self.worker_id}: Processing {dataset} {dataset_start}-{end_block}")
                success = self._process_dataset_range(dataset_start, end_block, [dataset])
                
                if not success:
                    # Mark as failed but continue with other datasets
                    self.state_manager.fail_range(
                        self.mode, dataset, dataset_start, end_block, 
                        "Processing failed"
                    )
                    logger.error(f"Worker {self.worker_id}: Failed to process {dataset} {dataset_start}-{end_block}")
            
            logger.info(f"Worker {self.worker_id}: Completed processing all datasets for {start_block}-{end_block}")
            return True
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error processing {start_block}-{end_block}: {e}", exc_info=True)
            return False
    
    def _process_dataset_range(
        self,
        start_block: int,
        end_block: int,
        datasets: List[str]
    ) -> bool:
        """Process a specific dataset range with deduplication."""
        try:
            # Delete existing data if configured
            deleted_stats = self._delete_existing_data(start_block, end_block, datasets)
            for dataset, count in deleted_stats.items():
                if count > 0:
                    logger.info(f"Worker {self.worker_id}: Deleted {count} rows from {dataset}")
            
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
        """Execute the actual cryo command with explicit column specification."""
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
                check=False,  # Don't raise on non-zero exit
                capture_output=True,
                text=True,
                timeout=settings.cryo_timeout
            )
            
            if process.returncode != 0:
                # Check if it's just an empty dataset
                error_output = (process.stderr or "") + (process.stdout or "")
                
                # Common patterns for empty datasets
                if any(pattern in error_output.lower() for pattern in [
                    "no data", "empty", "zero", "0 items", "no logs", "no transactions",
                    "no events", "no traces", "no contracts", "no transfers"
                ]):
                    logger.warning(
                        f"Worker {self.worker_id}: No data found for {datasets} in blocks "
                        f"{start_block}-{end_block}, continuing with empty dataset"
                    )
                    # Create empty parquet files or handle as needed
                    return
                
                # For actual errors, raise
                error_msg = f"Cryo failed with code {process.returncode}: {process.stderr}"
                logger.error(f"Worker {self.worker_id}: {error_msg}")
                logger.error(f"stdout: {process.stdout}")
                raise Exception(error_msg)
                
        except subprocess.TimeoutExpired:
            logger.error(f"Worker {self.worker_id}: Cryo command timed out")
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
                # This is OK - the dataset might be empty for this block range
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