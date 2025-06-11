"""
Backfill Worker - Original logic with partition-based processing.
Uses fixed-size ranges for predictable and efficient processing.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
from datetime import datetime, timedelta

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
class PartitionInfo:
    """Information about a partition."""
    partition_month: datetime
    min_block: int
    max_block: int
    block_count: int


@dataclass
class DatasetWorkItem:
    """Work item for parallel processing within a dataset."""
    dataset: str
    table_name: str
    start_block: int
    end_block: int
    partition_month: datetime
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
        logger.info("Using partition-based processing to avoid memory issues")
    
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
        Uses partition-based processing to avoid memory issues.
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
        
        # First, get all partitions from blocks table
        partitions = self._get_partitions_from_blocks(start_block, end_block)
        if not partitions:
            logger.error("No partitions found in blocks table")
            return results
        
        logger.info(f"Found {len(partitions)} partitions to process")
        
        # Create work items for each dataset and partition
        work_items = []
        for dataset in datasets:
            table_name = self.dataset_tables.get(dataset, dataset)
            
            # Check if table exists
            if not self._table_exists(table_name):
                logger.warning(f"{dataset}: Table {table_name} does not exist")
                results[dataset] = BackfillStats(dataset=dataset, table_name=table_name, exists=False)
                continue
            
            # Create work items for each partition
            for partition in partitions:
                # Determine block range for this partition
                partition_start = partition.min_block
                partition_end = partition.max_block
                
                # Apply constraints if specified
                if start_block is not None and partition_start < start_block:
                    partition_start = start_block
                if end_block is not None and partition_end > end_block:
                    partition_end = end_block
                
                # Skip if partition is outside requested range
                if partition_start > partition_end:
                    continue
                
                # Align to range boundaries
                aligned_start = (partition_start // self.range_size) * self.range_size
                aligned_end = ((partition_end // self.range_size) + 1) * self.range_size
                
                work_item = DatasetWorkItem(
                    dataset=dataset,
                    table_name=table_name,
                    start_block=aligned_start,
                    end_block=aligned_end,
                    partition_month=partition.partition_month,
                    force=force,
                    worker_id=0,  # Will be assigned later
                    range_size=self.range_size
                )
                work_items.append(work_item)
            
            # Initialize results for this dataset
            results[dataset] = BackfillStats(
                dataset=dataset,
                table_name=table_name,
                exists=True
            )
        
        if not work_items:
            logger.warning("No work items created")
            return results
        
        logger.info(f"Created {len(work_items)} work items")
        
        # Process work items with limited parallelism
        # Limit workers to avoid overwhelming the system
        actual_workers = min(max_workers, len(work_items), 4)  # Max 4 parallel to be safe
        
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Submit all tasks
            future_to_work = {
                executor.submit(self._process_work_item_partition, item): item
                for item in work_items
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_work):
                work_item = future_to_work[future]
                completed += 1
                
                try:
                    created, skipped, complete_count, incomplete_count, rows = future.result()
                    
                    # Update statistics
                    with self._stats_lock:
                        stats = results[work_item.dataset]
                        stats.created_ranges += created
                        stats.skipped_ranges += skipped
                        stats.completed_ranges += complete_count
                        stats.incomplete_ranges += incomplete_count
                        stats.total_ranges += (created + skipped)
                        stats.total_rows += rows
                        
                        # Update min/max blocks
                        if created > 0 or skipped > 0:
                            if stats.min_block is None or work_item.start_block < stats.min_block:
                                stats.min_block = work_item.start_block
                            if stats.max_block is None or work_item.end_block > stats.max_block:
                                stats.max_block = work_item.end_block
                    
                    if completed % 10 == 0 or completed == len(work_items):
                        logger.info(
                            f"Progress: {completed}/{len(work_items)} work items completed"
                        )
                    
                except Exception as e:
                    logger.error(
                        f"Error processing {work_item.dataset} partition {work_item.partition_month.strftime('%Y-%m')}: {e}",
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
    
    def _get_partitions_from_blocks(
        self,
        start_block: Optional[int],
        end_block: Optional[int]
    ) -> List[PartitionInfo]:
        """Get partition information from blocks table."""
        try:
            client = self.clickhouse._connect()
            
            # Build WHERE clause
            where_parts = []
            if start_block is not None:
                where_parts.append(f"block_number >= {start_block}")
            if end_block is not None:
                where_parts.append(f"block_number <= {end_block}")
            
            where_clause = ""
            if where_parts:
                where_clause = "WHERE " + " AND ".join(where_parts)
            
            # Get partitions with their block ranges
            query = f"""
            SELECT 
                toStartOfMonth(block_timestamp) as partition_month,
                MIN(block_number) as min_block,
                MAX(block_number) as max_block,
                COUNT() as block_count
            FROM {self.database}.blocks
            {where_clause}
            GROUP BY partition_month
            ORDER BY partition_month
            """
            
            logger.info("Getting partitions from blocks table...")
            result = client.query(query)
            
            partitions = []
            for row in result.result_rows:
                partitions.append(PartitionInfo(
                    partition_month=row[0],
                    min_block=row[1],
                    max_block=row[2],
                    block_count=row[3]
                ))
                logger.info(
                    f"Partition {row[0].strftime('%Y-%m')}: "
                    f"blocks {row[1]:,}-{row[2]:,} ({row[3]:,} blocks)"
                )
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions from blocks: {e}")
            return []
    
    def _process_work_item_partition(
        self, 
        work_item: DatasetWorkItem
    ) -> Tuple[int, int, int, int, int]:
        """Process a work item for a specific partition."""
        logger.debug(
            f"Processing {work_item.dataset} partition {work_item.partition_month.strftime('%Y-%m')}: "
            f"blocks {work_item.start_block:,} to {work_item.end_block:,}"
        )
        
        # Use thread-local connection
        thread_clickhouse = self._get_thread_clickhouse()
        
        # Get timestamp range for this partition
        month_start = work_item.partition_month
        # Get last second of the month
        if work_item.partition_month.month == 12:
            month_end = datetime(work_item.partition_month.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            month_end = datetime(work_item.partition_month.year, work_item.partition_month.month + 1, 1) - timedelta(seconds=1)
        
        # Process using fixed ranges with partition filtering
        created, skipped, complete_count, incomplete_count, total_rows = self._process_dataset_fixed_ranges_partition(
            thread_clickhouse,
            work_item.dataset,
            work_item.table_name,
            work_item.start_block,
            work_item.end_block,
            month_start,
            month_end,
            work_item.force,
            work_item.range_size
        )
        
        return created, skipped, complete_count, incomplete_count, total_rows
    
    def _process_dataset_fixed_ranges_partition(
        self,
        clickhouse: ClickHouseManager,
        dataset: str,
        table_name: str,
        min_block: int,
        max_block: int,
        month_start: datetime,
        month_end: datetime,
        force: bool,
        range_size: int
    ) -> Tuple[int, int, int, int, int]:
        """Process dataset using fixed-size ranges with partition filtering."""
        created = 0
        skipped = 0
        complete_count = 0
        incomplete_count = 0
        total_rows = 0
        
        # Process in fixed-size chunks
        current = min_block
        while current < max_block:
            range_end = min(current + range_size, max_block)
            
            # Check if entry already exists
            if not force and self._entry_exists_thread_safe(
                clickhouse, dataset, current, range_end
            ):
                skipped += 1
                current = range_end
                continue
            
            # Count distinct blocks in this range WITH PARTITION FILTERING
            distinct_blocks = self._count_distinct_blocks_partition(
                clickhouse, table_name, current, range_end, month_start, month_end
            )
            
            # Count total rows WITH PARTITION FILTERING
            rows_in_range = self._count_rows_in_range_partition(
                clickhouse, table_name, current, range_end, month_start, month_end
            )
            
            total_rows += rows_in_range
            
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
                rows_count=rows_in_range,
                blocks_count=distinct_blocks,
                expected_blocks=expected_blocks,
                partition=month_start.strftime('%Y-%m')
            ):
                created += 1
                if created % 100 == 0:
                    logger.debug(
                        f"{dataset} partition {month_start.strftime('%Y-%m')}: "
                        f"Created {created} entries so far"
                    )
            else:
                logger.error(f"Failed to create entry for {dataset} {current:,}-{range_end:,}")
            
            current = range_end
        
        return created, skipped, complete_count, incomplete_count, total_rows
    
    def _count_distinct_blocks_partition(
        self,
        clickhouse: ClickHouseManager,
        table_name: str,
        start_block: int,
        end_block: int,
        month_start: datetime,
        month_end: datetime
    ) -> int:
        """Count distinct block numbers in range with partition filtering."""
        try:
            client = clickhouse._connect()
            
            # For blocks table, no need for timestamp filter
            if table_name == 'blocks':
                query = f"""
                SELECT COUNT(DISTINCT block_number) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                """
            else:
                # For other tables, use partition filtering
                query = f"""
                SELECT COUNT(DISTINCT block_number) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                  AND toStartOfMonth(block_timestamp) = '{month_start.strftime('%Y-%m-%d')}'
                """
            
            result = client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
            
        except Exception as e:
            logger.error(f"Error counting distinct blocks: {e}")
            return 0
    
    def _count_rows_in_range_partition(
        self,
        clickhouse: ClickHouseManager,
        table_name: str,
        start_block: int,
        end_block: int,
        month_start: datetime,
        month_end: datetime
    ) -> int:
        """Count rows in range with partition filtering."""
        try:
            client = clickhouse._connect()
            
            # For blocks table, no need for timestamp filter
            if table_name == 'blocks':
                query = f"""
                SELECT COUNT(*) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                """
            else:
                # For other tables, use partition filtering
                query = f"""
                SELECT COUNT(*) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                  AND toStartOfMonth(block_timestamp) = '{month_start.strftime('%Y-%m-%d')}'
                """
            
            result = client.query(query)
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
        expected_blocks: int,
        partition: str = ""
    ) -> bool:
        """Create an indexing_state entry with metadata."""
        try:
            client = clickhouse._connect()
            
            # Store metadata in error_message field
            metadata = f"blocks:{blocks_count}/{expected_blocks},rows:{rows_count},partition:{partition}"
            
            client.command(f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, 
             completed_at, rows_indexed, batch_id, error_message)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             '{status}', now(), {rows_count}, 'backfill-partition', '{metadata}')
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
        Just calls the parallel version with 1 worker.
        """
        return self.backfill_datasets_parallel(
            datasets=datasets,
            start_block=start_block,
            end_block=end_block,
            force=force,
            max_workers=1
        )
    
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
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 80)
        print("BACKFILL SUMMARY (Partition-based Processing)")
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
            if stats.min_block is not None and stats.max_block is not None:
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
            
            try:
                client = self.clickhouse._connect()
                
                # Check if we have entries
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
                    
                    logger.info(
                        f"✓ {dataset}: {range_count} ranges "
                        f"({complete_count} complete, {incomplete_count} incomplete), "
                        f"covering blocks {min_indexed:,}-{max_indexed:,}"
                    )
                else:
                    logger.warning(f"✗ {dataset}: No indexing_state entries found")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error validating {dataset}: {e}")
                success = False
        
        return success