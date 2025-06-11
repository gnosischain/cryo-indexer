"""
Backfill Worker - Analyzes existing data and populates indexing_state table.
Uses fixed-size ranges for predictable and efficient processing.
Processes one partition at a time to avoid memory issues.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
from datetime import datetime

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
        """
        # For now, just use single-threaded to avoid overwhelming the system
        return self.backfill_datasets(datasets, start_block, end_block, force)
    
    def backfill_datasets(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, BackfillStats]:
        """
        Backfill indexing_state for specified datasets (single-threaded).
        Processes each partition separately to avoid memory issues.
        """
        results = {}
        
        logger.info(f"Starting backfill for datasets: {datasets}")
        logger.info(f"Using fixed range size: {self.range_size} blocks")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block:,} to {end_block:,}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        # First, get the list of partitions from blocks table
        partitions = self._get_partitions_list(start_block, end_block)
        if not partitions:
            logger.error("No partitions found in blocks table")
            return results
        
        logger.info(f"Found {len(partitions)} partitions to process")
        
        for dataset in datasets:
            logger.info(f"Processing dataset: {dataset}")
            start_time = time.time()
            stats = self._backfill_dataset_by_partitions(
                dataset, partitions, start_block, end_block, force
            )
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
    
    def _get_partitions_list(
        self,
        start_block: Optional[int],
        end_block: Optional[int]
    ) -> List[Dict]:
        """Get list of partitions from blocks table."""
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
            
            # Get partitions
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
                partition = {
                    'month': row[0],
                    'min_block': row[1],
                    'max_block': row[2],
                    'block_count': row[3]
                }
                partitions.append(partition)
                logger.info(
                    f"Partition {row[0].strftime('%Y-%m')}: blocks {row[1]:,}-{row[2]:,} ({row[3]:,} blocks)"
                )
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions: {e}")
            return []
    
    def _backfill_dataset_by_partitions(
        self,
        dataset: str,
        partitions: List[Dict],
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> BackfillStats:
        """Backfill a single dataset by processing each partition separately."""
        stats = BackfillStats(dataset=dataset, table_name="")
        
        # Get table name
        table_name = self.dataset_tables.get(dataset, dataset)
        stats.table_name = table_name
        
        # Check if table exists
        if not self._table_exists(table_name):
            stats.exists = False
            return stats
        
        stats.exists = True
        
        # Process each partition
        for partition in partitions:
            partition_month = partition['month']
            partition_min = partition['min_block']
            partition_max = partition['max_block']
            
            # Apply range constraints if specified
            if start_block is not None and partition_min < start_block:
                partition_min = start_block
            if end_block is not None and partition_max > end_block:
                partition_max = end_block
            
            # Skip if partition is outside range
            if partition_min > partition_max:
                continue
            
            logger.info(
                f"Processing {dataset} partition {partition_month.strftime('%Y-%m')}: "
                f"blocks {partition_min:,}-{partition_max:,}"
            )
            
            # Process this partition
            created, skipped, complete_count, incomplete_count, rows = self._process_partition(
                dataset, table_name, partition_month, partition_min, partition_max, force
            )
            
            stats.created_ranges += created
            stats.skipped_ranges += skipped
            stats.completed_ranges += complete_count
            stats.incomplete_ranges += incomplete_count
            stats.total_rows += rows
            
            # Update min/max
            if stats.min_block is None or partition_min < stats.min_block:
                stats.min_block = partition_min
            if stats.max_block is None or partition_max > stats.max_block:
                stats.max_block = partition_max
        
        stats.total_ranges = stats.created_ranges + stats.skipped_ranges
        
        return stats
    
    def _process_partition(
        self,
        dataset: str,
        table_name: str,
        partition_month: datetime,
        min_block: int,
        max_block: int,
        force: bool
    ) -> Tuple[int, int, int, int, int]:
        """Process a single partition."""
        created = 0
        skipped = 0
        complete_count = 0
        incomplete_count = 0
        total_rows = 0
        
        # Align to range boundaries
        aligned_min = (min_block // self.range_size) * self.range_size
        aligned_max = ((max_block // self.range_size) + 1) * self.range_size
        
        # If force mode, delete existing entries
        if force:
            self._delete_existing_entries_in_range(dataset, aligned_min, aligned_max)
        
        # Process in fixed-size chunks
        current = aligned_min
        while current < aligned_max:
            range_end = min(current + self.range_size, aligned_max)
            
            # Check if entry already exists
            if not force and self._entry_exists_thread_safe(
                self.clickhouse, dataset, current, range_end
            ):
                skipped += 1
                current = range_end
                continue
            
            # Count distinct blocks in this range - WITH PARTITION FILTER
            distinct_blocks = self._count_distinct_blocks_partition(
                self.clickhouse, table_name, current, range_end, partition_month
            )
            
            # Count total rows - WITH PARTITION FILTER
            rows_count = self._count_rows_in_range_partition(
                self.clickhouse, table_name, current, range_end, partition_month
            )
            
            total_rows += rows_count
            
            # Determine if range is complete
            expected_blocks = range_end - current
            is_complete = (distinct_blocks == expected_blocks)
            
            # For blocks dataset, it MUST be complete to be marked as completed
            if dataset == 'blocks' and not is_complete:
                status = 'incomplete'
                incomplete_count += 1
            else:
                # For other datasets, mark as completed if we've indexed it
                status = 'completed'
                complete_count += 1
            
            # Create the entry
            if self._create_range_entry_thread_safe(
                clickhouse=self.clickhouse,
                dataset=dataset,
                start_block=current,
                end_block=range_end,
                status=status,
                rows_count=rows_count,
                blocks_count=distinct_blocks,
                expected_blocks=expected_blocks
            ):
                created += 1
                if created % 100 == 0:
                    logger.debug(f"Created {created} entries for {dataset} partition {partition_month.strftime('%Y-%m')}")
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
        partition_month: datetime
    ) -> int:
        """Count distinct block numbers in range for a specific partition."""
        try:
            client = clickhouse._connect()
            
            if table_name == 'blocks':
                # Blocks table doesn't need partition filter
                query = f"""
                SELECT COUNT(DISTINCT block_number) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                  AND toStartOfMonth(block_timestamp) = '{partition_month.strftime('%Y-%m-%d')}'
                """
            else:
                # Other tables use partition filter
                query = f"""
                SELECT COUNT(DISTINCT block_number) 
                FROM {self.database}.{table_name}
                WHERE block_number >= {start_block} 
                  AND block_number < {end_block}
                  AND block_number IS NOT NULL
                  AND toStartOfMonth(block_timestamp) = '{partition_month.strftime('%Y-%m-%d')}'
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
        partition_month: datetime
    ) -> int:
        """Count rows in range for a specific partition."""
        try:
            client = clickhouse._connect()
            
            query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_number IS NOT NULL
              AND toStartOfMonth(block_timestamp) = '{partition_month.strftime('%Y-%m-%d')}'
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
            
            try:
                client = self.clickhouse._connect()
                
                # Check coverage
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
                    elif range_count > 0:
                        logger.info(
                            f"✓ {dataset}: Properly covered by {range_count} ranges "
                            f"({complete_count} complete, {incomplete_count} incomplete)"
                        )
                    else:
                        logger.warning(f"✗ {dataset}: No ranges found")
                        success = False
                else:
                    logger.warning(f"✗ {dataset}: No indexing_state entries found")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error validating {dataset}: {e}")
                success = False
        
        return success