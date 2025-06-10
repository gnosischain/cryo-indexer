"""
Backfill Worker - Analyzes existing data and populates indexing_state table.
Supports parallel processing both by dataset and within each dataset.
Uses streaming approach to write entries as they're found, not storing everything in memory.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math

from .core.state_manager import StateManager
from .db.clickhouse_manager import ClickHouseManager
from .config import settings


@dataclass
class BackfillStats:
    """Statistics for backfill operation."""
    dataset: str
    table_name: str
    exists: bool = False
    min_block: Optional[int] = None
    max_block: Optional[int] = None
    total_rows: int = 0
    continuous_ranges: int = 0  # Changed from List to count
    created_ranges: int = 0
    skipped_ranges: int = 0
    deleted_ranges: int = 0
    duration: float = 0.0
    worker_count: int = 1
    
    def __post_init__(self):
        pass


@dataclass
class DatasetWorkItem:
    """Work item for parallel processing within a dataset."""
    dataset: str
    table_name: str
    start_block: int
    end_block: int
    force: bool
    worker_id: int


class BackfillWorker:
    """Worker that analyzes existing data and creates indexing_state entries."""
    
    def __init__(
        self,
        clickhouse: ClickHouseManager,
        state_manager: StateManager,
        mode: str
    ):
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.mode = mode
        self.database = clickhouse.database
        
        # Table mappings
        self.dataset_tables = {
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
            # Aliases for compatibility
            'events': 'logs',
            'txs': 'transactions',
        }
        
        # Thread-local storage for database connections
        self._thread_local = threading.local()
        
        # Shared statistics lock
        self._stats_lock = threading.Lock()
        
        logger.info(f"BackfillWorker initialized for mode {mode}")
    
    def _get_thread_clickhouse(self) -> ClickHouseManager:
        """Get a thread-local ClickHouse connection."""
        if not hasattr(self._thread_local, 'clickhouse'):
            # Create a new connection for this thread
            self._thread_local.clickhouse = ClickHouseManager(
                host=self.clickhouse.host,
                user=self.clickhouse.user,
                password=self.clickhouse.password,
                database=self.clickhouse.database,
                port=self.clickhouse.port,
                secure=self.clickhouse.secure
            )
        return self._thread_local.clickhouse
    
    def backfill_datasets_parallel(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False,
        max_workers: Optional[int] = None
    ) -> Dict[str, BackfillStats]:
        """
        Backfill indexing_state for specified datasets in parallel.
        Supports multiple workers per dataset.
        
        Args:
            datasets: List of datasets to backfill
            start_block: Optional start block (None = from beginning)
            end_block: Optional end block (None = to end)
            force: Force recreation of existing entries
            max_workers: Maximum number of parallel workers
            
        Returns:
            Dictionary of dataset -> BackfillStats
        """
        results = {}
        
        # Determine number of workers
        if max_workers is None:
            max_workers = settings.workers
        
        logger.info(f"Starting parallel backfill for {len(datasets)} datasets with {max_workers} workers")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block} to {end_block}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        start_time = time.time()
        
        # First pass: analyze all datasets to find work
        dataset_work = {}
        for dataset in datasets:
            work_info = self._analyze_dataset(dataset, start_block, end_block, force)
            if work_info:
                dataset_work[dataset] = work_info
        
        if not dataset_work:
            logger.warning("No work found for any dataset")
            return results
        
        # Distribute workers across datasets
        work_items = self._distribute_work(dataset_work, max_workers)
        
        logger.info(f"Created {len(work_items)} work items across {len(dataset_work)} datasets")
        
        # Initialize results
        for dataset in dataset_work:
            results[dataset] = BackfillStats(
                dataset=dataset,
                table_name=self.dataset_tables.get(dataset, dataset),
                exists=True,
                min_block=dataset_work[dataset]['min_block'],
                max_block=dataset_work[dataset]['max_block'],
                total_rows=dataset_work[dataset]['total_rows'],
                worker_count=sum(1 for item in work_items if item.dataset == dataset)
            )
        
        # Process all work items in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_work = {
                executor.submit(self._process_work_item_streaming, item): item
                for item in work_items
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_work):
                work_item = future_to_work[future]
                completed += 1
                
                try:
                    created, skipped, ranges_found = future.result()
                    
                    # Update statistics
                    with self._stats_lock:
                        results[work_item.dataset].created_ranges += created
                        results[work_item.dataset].skipped_ranges += skipped
                        results[work_item.dataset].continuous_ranges += ranges_found
                    
                    logger.info(
                        f"[{completed}/{len(work_items)}] {work_item.dataset} "
                        f"worker {work_item.worker_id}: Created {created}, Skipped {skipped}, "
                        f"Ranges found: {ranges_found}"
                    )
                    
                except Exception as e:
                    logger.error(
                        f"Error processing {work_item.dataset} worker {work_item.worker_id}: {e}",
                        exc_info=True
                    )
        
        # Calculate durations
        total_duration = time.time() - start_time
        for stats in results.values():
            stats.duration = total_duration
        
        logger.info(f"Parallel backfill completed in {total_duration:.2f}s")
        
        # Print summary
        self._print_backfill_summary(results)
        return results
    
    def _analyze_dataset(
        self,
        dataset: str,
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> Optional[Dict]:
        """Analyze a dataset to find work to be done."""
        table_name = self.dataset_tables.get(dataset, dataset)
        
        # Check if table exists
        if not self._table_exists(table_name):
            logger.warning(f"{dataset}: Table {table_name} does not exist")
            return None
        
        # Get data range and statistics
        table_stats = self._get_table_stats(table_name, start_block, end_block)
        if not table_stats:
            logger.warning(f"No data found in {table_name}")
            return None
        
        min_block, max_block, total_rows = table_stats
        
        logger.info(
            f"{dataset}: blocks {min_block}-{max_block}, "
            f"{total_rows:,} rows"
        )
        
        # If force mode, handle deletions
        if force:
            if start_block is not None and end_block is not None:
                self._delete_existing_entries_in_range(dataset, start_block, end_block)
            else:
                self._delete_existing_entries(dataset, min_block, max_block)
        
        return {
            'dataset': dataset,
            'table_name': table_name,
            'min_block': min_block,
            'max_block': max_block,
            'total_rows': total_rows,
            'force': force
        }
    
    def _distribute_work(
        self,
        dataset_work: Dict[str, Dict],
        max_workers: int
    ) -> List[DatasetWorkItem]:
        """Distribute work across workers."""
        work_items = []
        
        # Calculate total block ranges
        total_blocks = sum(
            info['max_block'] - info['min_block'] + 1 
            for info in dataset_work.values()
        )
        
        if total_blocks == 0:
            return work_items
        
        # Distribute workers proportionally to block ranges
        for dataset, info in dataset_work.items():
            dataset_blocks = info['max_block'] - info['min_block'] + 1
            
            # Calculate workers for this dataset
            dataset_worker_count = max(1, int(
                (dataset_blocks / total_blocks) * max_workers
            ))
            
            # Ensure we don't exceed max_workers
            dataset_worker_count = min(dataset_worker_count, max_workers)
            
            logger.info(
                f"{dataset}: Assigning {dataset_worker_count} workers "
                f"for {dataset_blocks:,} blocks"
            )
            
            # Divide block range among workers
            blocks_per_worker = math.ceil(dataset_blocks / dataset_worker_count)
            
            for worker_id in range(dataset_worker_count):
                worker_start = info['min_block'] + (worker_id * blocks_per_worker)
                worker_end = min(
                    worker_start + blocks_per_worker - 1,
                    info['max_block']
                )
                
                if worker_start > info['max_block']:
                    break
                
                work_item = DatasetWorkItem(
                    dataset=dataset,
                    table_name=info['table_name'],
                    start_block=worker_start,
                    end_block=worker_end,
                    force=info['force'],
                    worker_id=worker_id
                )
                
                work_items.append(work_item)
        
        return work_items
    
    def _process_work_item_streaming(
        self, 
        work_item: DatasetWorkItem
    ) -> Tuple[int, int, int]:
        """Process a work item using streaming approach."""
        logger.info(
            f"Processing {work_item.dataset} worker {work_item.worker_id}: "
            f"blocks {work_item.start_block:,} to {work_item.end_block:,}"
        )
        
        # Use thread-local connection
        thread_clickhouse = self._get_thread_clickhouse()
        
        # Process using streaming approach
        created, skipped, ranges_found = self._find_continuous_ranges_streaming(
            thread_clickhouse,
            work_item.table_name,
            work_item.dataset,
            work_item.start_block,
            work_item.end_block,
            work_item.force
        )
        
        return created, skipped, ranges_found
    
    def _find_continuous_ranges_streaming(
        self,
        clickhouse: ClickHouseManager,
        table_name: str,
        dataset: str,
        min_block: int,
        max_block: int,
        force: bool
    ) -> Tuple[int, int, int]:
        """Find continuous ranges and write them immediately as found."""
        try:
            client = clickhouse._connect()
            chunk_size = settings.backfill_chunk_size
            
            created = 0
            skipped = 0
            ranges_found = 0
            current_range_start = None
            current_range_end = None
            
            logger.info(f"Scanning and writing blocks for {table_name} from {min_block:,} to {max_block:,}")
            
            current = min_block
            while current <= max_block:
                chunk_end = min(current + chunk_size - 1, max_block)
                
                # Get blocks in this chunk
                result = client.query(f"""
                SELECT DISTINCT block_number 
                FROM {self.database}.{table_name}
                WHERE block_number >= {current} 
                  AND block_number <= {chunk_end}
                  AND block_number IS NOT NULL
                ORDER BY block_number
                """)
                
                chunk_blocks = [row[0] for row in result.result_rows]
                logger.debug(f"Chunk {current}-{chunk_end}: {len(chunk_blocks)} blocks")
                
                # Process blocks in this chunk
                for block in chunk_blocks:
                    if current_range_start is None:
                        # Start new range
                        current_range_start = block
                        current_range_end = block
                    elif block == current_range_end + 1:
                        # Extend current range
                        current_range_end = block
                    else:
                        # Gap found - write current range
                        c, s = self._write_range_entries(
                            clickhouse, dataset, current_range_start, current_range_end, force
                        )
                        created += c
                        skipped += s
                        ranges_found += 1
                        
                        # Start new range
                        current_range_start = block
                        current_range_end = block
                
                # Check if we should close the range at chunk boundary
                if current_range_start is not None and chunk_end < max_block:
                    # Check if the range continues in the next chunk
                    next_block = chunk_end + 1
                    check_result = client.query(f"""
                    SELECT COUNT(*) 
                    FROM {self.database}.{table_name}
                    WHERE block_number = {next_block}
                    """)
                    
                    if check_result.result_rows[0][0] == 0:
                        # No block at start of next chunk, close range
                        c, s = self._write_range_entries(
                            clickhouse, dataset, current_range_start, current_range_end, force
                        )
                        created += c
                        skipped += s
                        ranges_found += 1
                        current_range_start = None
                        current_range_end = None
                
                current = chunk_end + 1
            
            # Write final range if exists
            if current_range_start is not None:
                c, s = self._write_range_entries(
                    clickhouse, dataset, current_range_start, current_range_end, force
                )
                created += c
                skipped += s
                ranges_found += 1
            
            logger.info(
                f"Completed scanning {table_name}: "
                f"Created {created}, Skipped {skipped}, Ranges found: {ranges_found}"
            )
            return created, skipped, ranges_found
            
        except Exception as e:
            logger.error(f"Error in streaming scan for {table_name}: {e}")
            return 0, 0, 0
    
    def _write_range_entries(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        start: int,
        end: int,
        force: bool
    ) -> Tuple[int, int]:
        """Write indexing_state entries for a continuous range."""
        created = 0
        skipped = 0
        
        logger.debug(f"Writing range entries for {dataset}: {start:,} to {end:,}")
        
        # Calculate batch ranges based on backfill_batch_size
        current = start
        while current <= end:
            batch_end = min(current + settings.backfill_batch_size - 1, end)
            
            # Check if entry already exists
            if not force and self._entry_exists_thread_safe(
                clickhouse, dataset, current, batch_end + 1
            ):
                skipped += 1
                current = batch_end + 1
                continue
            
            # Count rows in this range
            rows_count = self._count_rows_in_range_thread_safe(
                clickhouse,
                self.dataset_tables[dataset], 
                current, 
                batch_end + 1
            )
            
            # Create the entry
            if self._create_entry_thread_safe(
                clickhouse, dataset, current, batch_end + 1, rows_count
            ):
                created += 1
                logger.debug(
                    f"Created entry for {dataset} {current:,}-{batch_end + 1:,} "
                    f"({rows_count:,} rows)"
                )
            else:
                logger.error(
                    f"Failed to create entry for {dataset} {current:,}-{batch_end + 1:,}"
                )
            
            current = batch_end + 1
        
        return created, skipped
    
    def _entry_exists_thread_safe(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        start_block: int,
        end_block: int
    ) -> bool:
        """Thread-safe check if an indexing_state entry already exists."""
        try:
            client = clickhouse._connect()
            result = client.query(f"""
            SELECT COUNT() 
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
              AND status = 'completed'
            """)
            return result.result_rows[0][0] > 0
        except Exception as e:
            logger.error(f"Error checking entry existence: {e}")
            return False
    
    def _count_rows_in_range_thread_safe(
        self,
        clickhouse: ClickHouseManager,
        table_name: str,
        start_block: int,
        end_block: int
    ) -> int:
        """Thread-safe version of count_rows_in_range."""
        try:
            client = clickhouse._connect()
            result = client.query(f"""
            SELECT COUNT(*) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_number IS NOT NULL
            """)
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.error(f"Error counting rows in range: {e}")
            return 0
    
    def _create_entry_thread_safe(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        start_block: int,
        end_block: int,
        rows_count: int
    ) -> bool:
        """Thread-safe version of create_entry."""
        try:
            client = clickhouse._connect()
            client.command(f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, completed_at, rows_indexed, batch_id)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             'completed', now(), {rows_count}, 'backfill')
            """)
            return True
        except Exception as e:
            logger.error(f"Error creating indexing_state entry: {e}")
            return False
    
    def backfill_datasets(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, BackfillStats]:
        """
        Backfill indexing_state for specified datasets (single-threaded).
        Uses streaming approach for memory efficiency.
        """
        results = {}
        
        logger.info(f"Starting backfill for datasets: {datasets}")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block} to {end_block}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        for dataset in datasets:
            logger.info(f"Processing dataset: {dataset}")
            start_time = time.time()
            stats = self._backfill_dataset(dataset, start_block, end_block, force)
            stats.duration = time.time() - start_time
            results[dataset] = stats
            
            if stats.exists:
                logger.info(
                    f"{dataset}: Created {stats.created_ranges} ranges, "
                    f"skipped {stats.skipped_ranges} existing ranges "
                    f"in {stats.duration:.2f}s"
                )
                if force and stats.deleted_ranges > 0:
                    logger.info(f"{dataset}: Deleted {stats.deleted_ranges} existing ranges")
            else:
                logger.warning(f"{dataset}: Table does not exist")
        
        # Print summary
        self._print_backfill_summary(results)
        return results
    
    def _backfill_dataset(
        self,
        dataset: str,
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> BackfillStats:
        """Backfill a single dataset using streaming approach."""
        stats = BackfillStats(dataset=dataset, table_name="")
        
        # Get table name
        table_name = self.dataset_tables.get(dataset, dataset)
        stats.table_name = table_name
        
        # Check if table exists
        if not self._table_exists(table_name):
            stats.exists = False
            return stats
        
        stats.exists = True
        
        # If force mode and specific range provided, delete existing entries first
        if force and start_block is not None and end_block is not None:
            deleted_count = self._delete_existing_entries_in_range(
                dataset, start_block, end_block
            )
            stats.deleted_ranges = deleted_count
            if deleted_count > 0:
                logger.info(f"{dataset}: Deleted {deleted_count} existing entries in range {start_block}-{end_block}")
        
        # Get data range and statistics
        table_stats = self._get_table_stats(table_name, start_block, end_block)
        if not table_stats:
            logger.warning(f"No data found in {table_name}")
            return stats
        
        stats.min_block, stats.max_block, stats.total_rows = table_stats
        logger.info(
            f"{dataset}: blocks {stats.min_block}-{stats.max_block}, "
            f"{stats.total_rows:,} rows"
        )
        
        # If force mode and no specific range was provided, delete existing entries in found data range
        if force and (start_block is None or end_block is None):
            deleted_count = self._delete_existing_entries(
                dataset, stats.min_block, stats.max_block
            )
            stats.deleted_ranges = deleted_count
            if deleted_count > 0:
                logger.info(f"{dataset}: Deleted {deleted_count} existing entries")
        
        # Use streaming approach to find and write ranges
        created, skipped, ranges_found = self._find_continuous_ranges_streaming(
            self.clickhouse,
            table_name,
            dataset,
            stats.min_block,
            stats.max_block,
            force
        )
        
        stats.created_ranges = created
        stats.skipped_ranges = skipped
        stats.continuous_ranges = ranges_found
        
        return stats
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            client = self.clickhouse._connect()
            result = client.query(f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            """)
            return result.result_rows[0][0] > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def _get_table_stats(
        self,
        table_name: str,
        start_block: Optional[int],
        end_block: Optional[int]
    ) -> Optional[Tuple[int, int, int]]:
        """Get min/max block and row count from table."""
        try:
            client = self.clickhouse._connect()
            
            # Build WHERE clause for block range
            where_clause = "WHERE block_number IS NOT NULL"
            if start_block is not None:
                where_clause += f" AND block_number >= {start_block}"
            if end_block is not None:
                where_clause += f" AND block_number <= {end_block}"
            
            result = client.query(f"""
            SELECT 
                MIN(block_number) as min_block,
                MAX(block_number) as max_block,
                COUNT() as total_rows
            FROM {self.database}.{table_name}
            {where_clause}
            """)
            
            if not result.result_rows or result.result_rows[0][0] is None:
                return None
                
            return result.result_rows[0]
            
        except Exception as e:
            logger.error(f"Error getting table stats for {table_name}: {e}")
            return None
    
    def _delete_existing_entries_in_range(
        self,
        dataset: str,
        start_block: int,
        end_block: int
    ) -> int:
        """Delete existing indexing_state entries in the specified range."""
        try:
            client = self.clickhouse._connect()
            
            # First count how many entries we're deleting
            count_query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND ((start_block >= {start_block} AND start_block < {end_block})
                OR (end_block > {start_block} AND end_block <= {end_block})
                OR (start_block <= {start_block} AND end_block >= {end_block}))
            """
            result = client.query(count_query)
            count = result.result_rows[0][0] if result.result_rows else 0
            
            if count > 0:
                # Delete the entries
                delete_query = f"""
                ALTER TABLE {self.database}.indexing_state
                DELETE WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                  AND ((start_block >= {start_block} AND start_block < {end_block})
                    OR (end_block > {start_block} AND end_block <= {end_block})
                    OR (start_block <= {start_block} AND end_block >= {end_block}))
                """
                client.command(delete_query)
                logger.debug(f"Deleted {count} existing entries for {dataset} in range {start_block}-{end_block}")
            
            return count
            
        except Exception as e:
            logger.error(f"Error deleting existing entries: {e}")
            return 0
    
    def _delete_existing_entries(
        self,
        dataset: str,
        min_block: int,
        max_block: int
    ) -> int:
        """Delete existing indexing_state entries in the range."""
        try:
            client = self.clickhouse._connect()
            
            # First count how many entries we're deleting
            count_query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block >= {min_block}
              AND end_block <= {max_block}
              AND status = 'completed'
            """
            result = client.query(count_query)
            count = result.result_rows[0][0] if result.result_rows else 0
            
            if count > 0:
                # Delete the entries
                delete_query = f"""
                ALTER TABLE {self.database}.indexing_state
                DELETE WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                  AND start_block >= {min_block}
                  AND end_block <= {max_block}
                """
                client.command(delete_query)
                logger.debug(f"Deleted {count} existing entries for {dataset} in range {min_block}-{max_block}")
            
            return count
            
        except Exception as e:
            logger.error(f"Error deleting existing entries: {e}")
            return 0
    
    def _entry_exists(self, dataset: str, start_block: int, end_block: int) -> bool:
        """Check if an indexing_state entry already exists."""
        try:
            client = self.clickhouse._connect()
            result = client.query(f"""
            SELECT COUNT() 
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
              AND status = 'completed'
            """)
            return result.result_rows[0][0] > 0
        except Exception as e:
            logger.error(f"Error checking entry existence: {e}")
            return False
    
    def _create_entry(
        self,
        dataset: str,
        start_block: int,
        end_block: int,
        rows_count: int
    ) -> bool:
        """Create a single indexing_state entry."""
        try:
            client = self.clickhouse._connect()
            client.command(f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, completed_at, rows_indexed, batch_id)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             'completed', now(), {rows_count}, 'backfill')
            """)
            return True
        except Exception as e:
            logger.error(f"Error creating indexing_state entry: {e}")
            return False
    
    def _count_rows_in_range(self, table_name: str, start_block: int, end_block: int) -> int:
        """Count rows in a specific block range."""
        try:
            client = self.clickhouse._connect()
            result = client.query(f"""
            SELECT COUNT(*) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_number IS NOT NULL
            """)
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.error(f"Error counting rows in range: {e}")
            return 0
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 60)
        print("BACKFILL SUMMARY")
        print("=" * 60)
        
        total_created = 0
        total_skipped = 0
        total_deleted = 0
        total_duration = 0
        
        for dataset, stats in results.items():
            if not stats.exists:
                print(f"\n{dataset}: TABLE NOT FOUND")
                continue
            
            print(f"\n{dataset}:")
            print(f"  Table: {stats.table_name}")
            print(f"  Block range: {stats.min_block:,} - {stats.max_block:,}")
            print(f"  Total rows: {stats.total_rows:,}")
            print(f"  Continuous ranges found: {stats.continuous_ranges}")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            if stats.deleted_ranges > 0:
                print(f"  Deleted entries: {stats.deleted_ranges}")
            print(f"  Duration: {stats.duration:.2f}s")
            if stats.worker_count > 1:
                print(f"  Workers used: {stats.worker_count}")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
            total_deleted += stats.deleted_ranges
            total_duration = max(total_duration, stats.duration)  # Max because parallel
        
        print(f"\nTOTAL: Created {total_created}, Skipped {total_skipped}")
        if total_deleted > 0:
            print(f"       Deleted {total_deleted} (force mode)")
        print(f"Total Duration: {total_duration:.2f}s")
        print("=" * 60 + "\n")
    
    def validate_backfill(self, datasets: List[str]) -> bool:
        """Validate that backfill was successful."""
        logger.info("Validating backfill results...")
        
        success = True
        for dataset in datasets:
            table_name = self.dataset_tables.get(dataset, dataset)
            
            if not self._table_exists(table_name):
                continue
            
            # Get data range from table
            table_stats = self._get_table_stats(table_name, None, None)
            if not table_stats:
                continue
            
            min_block, max_block, total_rows = table_stats
            
            # Check indexing_state coverage
            try:
                client = self.clickhouse._connect()
                result = client.query(f"""
                SELECT 
                    MIN(start_block) as min_indexed,
                    MAX(end_block) as max_indexed,
                    COUNT() as range_count
                FROM {self.database}.indexing_state
                WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                  AND status = 'completed'
                """)
                
                if result.result_rows and result.result_rows[0][0] is not None:
                    min_indexed, max_indexed, range_count = result.result_rows[0]
                    
                    if min_indexed <= min_block and max_indexed >= max_block:
                        logger.info(f"✓ {dataset}: Properly covered by {range_count} ranges")
                    else:
                        logger.warning(
                            f"✗ {dataset}: Coverage gap - table has {min_block}-{max_block}, "
                            f"indexing_state has {min_indexed}-{max_indexed}"
                        )
                        success = False
                else:
                    logger.warning(f"✗ {dataset}: No indexing_state entries found")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error validating {dataset}: {e}")
                success = False
        
        return success