"""
Backfill Worker - Analyzes existing data and populates indexing_state table.
Uses fixed-size ranges for predictable and efficient processing.
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
    total_ranges: int = 0
    completed_ranges: int = 0
    incomplete_ranges: int = 0
    created_ranges: int = 0
    skipped_ranges: int = 0
    deleted_ranges: int = 0
    duration: float = 0.0
    worker_count: int = 1


@dataclass
class DatasetWorkItem:
    """Work item for parallel processing within a dataset."""
    dataset: str
    table_name: str
    start_block: int
    end_block: int
    force: bool
    worker_id: int
    range_size: int


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
        
        # Fixed range size from settings
        self.range_size = getattr(settings, 'indexing_range_size', 1000)
        
        logger.info(f"BackfillWorker initialized for mode {mode} with range size {self.range_size}")
    
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
        Uses fixed-size ranges for all datasets.
        
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
        logger.info(f"Using fixed range size: {self.range_size} blocks")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block:,} to {end_block:,}")
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
                executor.submit(self._process_work_item_fixed_ranges, item): item
                for item in work_items
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_work):
                work_item = future_to_work[future]
                completed += 1
                
                try:
                    created, skipped, complete_count, incomplete_count = future.result()
                    
                    # Update statistics
                    with self._stats_lock:
                        results[work_item.dataset].created_ranges += created
                        results[work_item.dataset].skipped_ranges += skipped
                        results[work_item.dataset].completed_ranges += complete_count
                        results[work_item.dataset].incomplete_ranges += incomplete_count
                        results[work_item.dataset].total_ranges += (created + skipped)
                    
                    logger.info(
                        f"[{completed}/{len(work_items)}] {work_item.dataset} "
                        f"worker {work_item.worker_id}: Created {created}, Skipped {skipped}, "
                        f"Complete: {complete_count}, Incomplete: {incomplete_count}"
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
        
        # Align to range boundaries
        aligned_min = (min_block // self.range_size) * self.range_size
        aligned_max = ((max_block // self.range_size) + 1) * self.range_size
        
        logger.info(
            f"{dataset}: blocks {min_block:,}-{max_block:,} "
            f"(aligned: {aligned_min:,}-{aligned_max:,}), "
            f"{total_rows:,} rows"
        )
        
        # If force mode, handle deletions
        if force:
            if start_block is not None and end_block is not None:
                self._delete_existing_entries_in_range(dataset, start_block, end_block)
            else:
                self._delete_existing_entries(dataset, aligned_min, aligned_max)
        
        return {
            'dataset': dataset,
            'table_name': table_name,
            'min_block': aligned_min,
            'max_block': aligned_max,
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
        
        # Calculate total ranges
        total_ranges = sum(
            (info['max_block'] - info['min_block']) // self.range_size
            for info in dataset_work.values()
        )
        
        if total_ranges == 0:
            return work_items
        
        # Distribute workers proportionally to ranges
        for dataset, info in dataset_work.items():
            dataset_ranges = (info['max_block'] - info['min_block']) // self.range_size
            
            # Calculate workers for this dataset
            dataset_worker_count = max(1, int(
                (dataset_ranges / total_ranges) * max_workers
            ))
            
            # Ensure we don't exceed max_workers
            dataset_worker_count = min(dataset_worker_count, max_workers, dataset_ranges)
            
            logger.info(
                f"{dataset}: Assigning {dataset_worker_count} workers "
                f"for {dataset_ranges} ranges"
            )
            
            # Divide ranges among workers
            ranges_per_worker = math.ceil(dataset_ranges / dataset_worker_count)
            
            for worker_id in range(dataset_worker_count):
                worker_start = info['min_block'] + (worker_id * ranges_per_worker * self.range_size)
                worker_end = min(
                    worker_start + (ranges_per_worker * self.range_size),
                    info['max_block']
                )
                
                if worker_start >= info['max_block']:
                    break
                
                work_item = DatasetWorkItem(
                    dataset=dataset,
                    table_name=info['table_name'],
                    start_block=worker_start,
                    end_block=worker_end,
                    force=info['force'],
                    worker_id=worker_id,
                    range_size=self.range_size
                )
                
                work_items.append(work_item)
        
        return work_items
    
    def _process_work_item_fixed_ranges(
        self, 
        work_item: DatasetWorkItem
    ) -> Tuple[int, int, int, int]:
        """Process a work item using fixed ranges."""
        logger.info(
            f"Processing {work_item.dataset} worker {work_item.worker_id}: "
            f"blocks {work_item.start_block:,} to {work_item.end_block:,}"
        )
        
        # Use thread-local connection
        thread_clickhouse = self._get_thread_clickhouse()
        
        # Process using fixed ranges
        created, skipped, complete_count, incomplete_count = self._process_dataset_fixed_ranges(
            thread_clickhouse,
            work_item.dataset,
            work_item.table_name,
            work_item.start_block,
            work_item.end_block,
            work_item.force,
            work_item.range_size
        )
        
        return created, skipped, complete_count, incomplete_count
    
    def _process_dataset_fixed_ranges(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        table_name: str,
        min_block: int,
        max_block: int,
        force: bool,
        range_size: int
    ) -> Tuple[int, int, int, int]:
        """Process dataset using fixed-size ranges."""
        created = 0
        skipped = 0
        complete_count = 0
        incomplete_count = 0
        
        logger.info(f"Processing {dataset} with fixed {range_size}-block ranges")
        
        # Process in fixed-size chunks
        current = min_block
        while current < max_block:
            range_end = min(current + range_size, max_block)
            
            # Check if entry already exists
            if not force and self._entry_exists_thread_safe(
                clickhouse, dataset, current, range_end
            ):
                skipped += 1
                logger.debug(f"Range {current}-{range_end} already exists, skipping")
                current = range_end
                continue
            
            # Count distinct blocks in this range
            distinct_blocks = self._count_distinct_blocks_thread_safe(
                clickhouse, table_name, current, range_end
            )
            
            # Count total rows (for tracking)
            total_rows = self._count_rows_in_range_thread_safe(
                clickhouse, table_name, current, range_end
            )
            
            # Determine if range is complete
            expected_blocks = range_end - current
            is_complete = (distinct_blocks == expected_blocks)
            
            # For blocks dataset, it MUST be complete to be marked as completed
            if dataset == 'blocks' and not is_complete:
                status = 'incomplete'
                incomplete_count += 1
            else:
                # For other datasets, mark as completed if we've indexed it
                # This prevents re-processing sparse data
                status = 'completed'
                complete_count += 1
            
            # Create the entry
            if self._create_range_entry_thread_safe(
                clickhouse=clickhouse,
                dataset=dataset,
                start_block=current,
                end_block=range_end,
                status=status,
                rows_count=total_rows,
                blocks_count=distinct_blocks,
                expected_blocks=expected_blocks
            ):
                created += 1
                logger.debug(
                    f"Created entry for {dataset} {current:,}-{range_end:,}: "
                    f"{distinct_blocks}/{expected_blocks} blocks, "
                    f"{total_rows:,} rows, status={status}"
                )
            else:
                logger.error(f"Failed to create entry for {dataset} {current:,}-{range_end:,}")
            
            current = range_end
        
        return created, skipped, complete_count, incomplete_count
    
    def _count_distinct_blocks_thread_safe(
        self,
        clickhouse: ClickHouseManager,
        table_name: str,
        start_block: int,
        end_block: int
    ) -> int:
        """Count distinct block numbers in range."""
        try:
            client = clickhouse._connect()
            result = client.query(f"""
            SELECT COUNT(DISTINCT block_number) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_number IS NOT NULL
            """)
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.error(f"Error counting distinct blocks: {e}")
            return 0
    
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
    
    def _create_range_entry_thread_safe(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        start_block: int,
        end_block: int,
        status: str,
        rows_count: int,
        blocks_count: int,
        expected_blocks: int
    ) -> bool:
        """Create an indexing_state entry with metadata."""
        try:
            client = clickhouse._connect()
            
            # Store metadata in error_message field
            metadata = f"blocks:{blocks_count}/{expected_blocks},rows:{rows_count}"
            
            client.command(f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, 
             completed_at, rows_indexed, batch_id, error_message)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             '{status}', now(), {rows_count}, 'backfill-fixed', '{metadata}')
            """)
            return True
        except Exception as e:
            logger.error(f"Error creating indexing_state entry: {e}")
            return False
    
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
            """)
            return result.result_rows[0][0] > 0
        except Exception as e:
            logger.error(f"Error checking entry existence: {e}")
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
        Uses fixed-size ranges for predictable processing.
        """
        results = {}
        
        logger.info(f"Starting backfill for datasets: {datasets}")
        logger.info(f"Using fixed range size: {self.range_size} blocks")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block:,} to {end_block:,}")
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
                    f"({stats.completed_ranges} complete, {stats.incomplete_ranges} incomplete) "
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
        """Backfill a single dataset using fixed ranges."""
        stats = BackfillStats(dataset=dataset, table_name="")
        
        # Get table name
        table_name = self.dataset_tables.get(dataset, dataset)
        stats.table_name = table_name
        
        # Check if table exists
        if not self._table_exists(table_name):
            stats.exists = False
            return stats
        
        stats.exists = True
        
        # Get data range and statistics
        table_stats = self._get_table_stats(table_name, start_block, end_block)
        if not table_stats:
            logger.warning(f"No data found in {table_name}")
            return stats
        
        min_block, max_block, total_rows = table_stats
        
        # Align to range boundaries
        aligned_min = (min_block // self.range_size) * self.range_size
        aligned_max = ((max_block // self.range_size) + 1) * self.range_size
        
        stats.min_block = min_block
        stats.max_block = max_block
        stats.total_rows = total_rows
        
        # If force mode, delete existing entries
        if force:
            if start_block is not None and end_block is not None:
                deleted_count = self._delete_existing_entries_in_range(
                    dataset, start_block, end_block
                )
            else:
                deleted_count = self._delete_existing_entries(
                    dataset, aligned_min, aligned_max
                )
            stats.deleted_ranges = deleted_count
            if deleted_count > 0:
                logger.info(f"{dataset}: Deleted {deleted_count} existing entries")
        
        # Process with fixed ranges
        created, skipped, complete_count, incomplete_count = self._process_dataset_fixed_ranges(
            self.clickhouse,
            dataset,
            table_name,
            aligned_min,
            aligned_max,
            force,
            self.range_size
        )
        
        stats.created_ranges = created
        stats.skipped_ranges = skipped
        stats.completed_ranges = complete_count
        stats.incomplete_ranges = incomplete_count
        stats.total_ranges = created + skipped
        
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
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 80)
        print("BACKFILL SUMMARY (Fixed Ranges)")
        print("=" * 80)
        
        total_created = 0
        total_skipped = 0
        total_deleted = 0
        total_complete = 0
        total_incomplete = 0
        total_duration = 0
        
        for dataset, stats in results.items():
            if not stats.exists:
                print(f"\n{dataset}: TABLE NOT FOUND")
                continue
            
            print(f"\n{dataset}:")
            print(f"  Table: {stats.table_name}")
            print(f"  Block range: {stats.min_block:,} - {stats.max_block:,}")
            print(f"  Total rows: {stats.total_rows:,}")
            print(f"  Range size: {self.range_size} blocks")
            print(f"  Total ranges: {stats.total_ranges}")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            print(f"  Complete ranges: {stats.completed_ranges}")
            print(f"  Incomplete ranges: {stats.incomplete_ranges}")
            if stats.deleted_ranges > 0:
                print(f"  Deleted entries: {stats.deleted_ranges}")
            print(f"  Duration: {stats.duration:.2f}s")
            if stats.worker_count > 1:
                print(f"  Workers used: {stats.worker_count}")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
            total_deleted += stats.deleted_ranges
            total_complete += stats.completed_ranges
            total_incomplete += stats.incomplete_ranges
            total_duration = max(total_duration, stats.duration)
        
        print(f"\nTOTAL:")
        print(f"  Created: {total_created}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Complete: {total_complete}")
        print(f"  Incomplete: {total_incomplete}")
        if total_deleted > 0:
            print(f"  Deleted: {total_deleted} (force mode)")
        print(f"  Duration: {total_duration:.2f}s")
        print("=" * 80 + "\n")
    
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
                
                # For fixed ranges, check coverage differently
                result = client.query(f"""
                SELECT 
                    MIN(start_block) as min_indexed,
                    MAX(end_block) as max_indexed,
                    COUNT() as range_count,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as complete_count,
                    SUM(CASE WHEN status = 'incomplete' THEN 1 ELSE 0 END) as incomplete_count
                FROM {self.database}.indexing_state
                WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                """)
                
                if result.result_rows and result.result_rows[0][0] is not None:
                    min_indexed, max_indexed, range_count, complete_count, incomplete_count = result.result_rows[0]
                    
                    # For blocks dataset, check if there are incomplete ranges
                    if dataset == 'blocks' and incomplete_count > 0:
                        logger.warning(
                            f"! {dataset}: Has {incomplete_count} incomplete ranges that need processing"
                        )
                        success = False
                    elif min_indexed <= min_block and max_indexed >= max_block:
                        logger.info(
                            f"✓ {dataset}: Properly covered by {range_count} ranges "
                            f"({complete_count} complete, {incomplete_count} incomplete)"
                        )
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