"""
Backfill Worker - Uses blocks table to drive partition-based processing.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time
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
class PartitionInfo:
    """Information about a partition."""
    partition_month: datetime
    min_block: int
    max_block: int
    block_count: int


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
        }
        
        # Fixed range size from settings
        self.range_size = getattr(settings, 'indexing_range_size', 1000)
        
        logger.info(f"BackfillWorker initialized for mode {mode} with range size {self.range_size}")
        logger.info("Using blocks table to drive partition-based processing")
    
    def backfill_datasets_parallel(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False,
        max_workers: Optional[int] = None
    ) -> Dict[str, BackfillStats]:
        """Just calls the single-threaded version."""
        return self.backfill_datasets(datasets, start_block, end_block, force)
    
    def backfill_datasets(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, BackfillStats]:
        """
        Backfill indexing_state for specified datasets using blocks table partitions.
        """
        results = {}
        
        logger.info(f"Starting blocks-driven backfill for datasets: {datasets}")
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
        
        # Process each dataset
        for dataset in datasets:
            logger.info(f"Processing dataset: {dataset}")
            stats = self._backfill_dataset_using_partitions(
                dataset, partitions, start_block, end_block, force
            )
            stats.duration = time.time() - start_time
            results[dataset] = stats
        
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
                logger.debug(
                    f"Partition {row[0].strftime('%Y-%m')}: "
                    f"blocks {row[1]:,}-{row[2]:,} ({row[3]:,} blocks)"
                )
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions from blocks: {e}")
            return []
    
    def _backfill_dataset_using_partitions(
        self,
        dataset: str,
        partitions: List[PartitionInfo],
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> BackfillStats:
        """Backfill a dataset using the partition information."""
        stats = BackfillStats(dataset=dataset, table_name="")
        
        # Get table name
        table_name = self.dataset_tables.get(dataset, dataset)
        stats.table_name = table_name
        
        # Check if table exists
        if not self._table_exists(table_name):
            stats.exists = False
            logger.warning(f"{dataset}: Table {table_name} does not exist")
            return stats
        
        stats.exists = True
        
        # Process each partition
        for partition in partitions:
            logger.info(
                f"Processing {dataset} for partition {partition.partition_month.strftime('%Y-%m')}"
            )
            
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
            
            # Process this partition's ranges
            created, skipped = self._process_partition_ranges(
                dataset,
                table_name,
                partition.partition_month,
                aligned_start,
                aligned_end,
                force
            )
            
            stats.created_ranges += created
            stats.skipped_ranges += skipped
            
            # Update min/max blocks
            if stats.min_block is None or partition.min_block < stats.min_block:
                stats.min_block = partition.min_block
            if stats.max_block is None or partition.max_block > stats.max_block:
                stats.max_block = partition.max_block
        
        stats.total_ranges = stats.created_ranges + stats.skipped_ranges
        stats.completed_ranges = stats.created_ranges
        stats.incomplete_ranges = 0
        
        return stats
    
    def _process_partition_ranges(
        self,
        dataset: str,
        table_name: str,
        partition_month: datetime,
        start_block: int,
        end_block: int,
        force: bool
    ) -> Tuple[int, int]:
        """Process ranges for a single partition."""
        created = 0
        skipped = 0
        
        # If force mode, delete existing entries for this range
        if force:
            self._delete_existing_entries_in_range(dataset, start_block, end_block)
        
        # Get timestamp range for this partition
        month_start = partition_month
        # Get last second of the month
        if partition_month.month == 12:
            month_end = datetime(partition_month.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            month_end = datetime(partition_month.year, partition_month.month + 1, 1) - timedelta(seconds=1)
        
        # Process in fixed-size chunks
        current = start_block
        while current < end_block:
            range_end = min(current + self.range_size, end_block)
            
            try:
                # Check if entry already exists
                if not force and self._entry_exists(dataset, current, range_end):
                    skipped += 1
                    current = range_end
                    continue
                
                # For blocks dataset, check completeness
                if dataset == 'blocks':
                    block_count = self._count_blocks_in_range(
                        table_name, current, range_end, month_start, month_end
                    )
                    expected_blocks = range_end - current
                    is_complete = (block_count == expected_blocks)
                    status = 'completed' if is_complete else 'incomplete'
                else:
                    # For other datasets, check if any data exists
                    has_data = self._check_data_exists(
                        table_name, current, range_end, month_start, month_end
                    )
                    status = 'completed' if has_data else 'pending'
                
                # Only create entry if there's data or it's incomplete
                if status != 'pending' or dataset == 'blocks':
                    if self._create_range_entry(
                        dataset=dataset,
                        start_block=current,
                        end_block=range_end,
                        status=status,
                        metadata=f"partition:{partition_month.strftime('%Y-%m')}"
                    ):
                        created += 1
                        logger.debug(
                            f"Created entry for {dataset} {current:,}-{range_end:,} "
                            f"(status={status})"
                        )
                
            except Exception as e:
                logger.error(f"Error processing range {current}-{range_end}: {e}")
            
            current = range_end
        
        return created, skipped
    
    def _count_blocks_in_range(
        self,
        table_name: str,
        start_block: int,
        end_block: int,
        month_start: datetime,
        month_end: datetime
    ) -> int:
        """Count blocks in a range with partition filtering."""
        try:
            client = self.clickhouse._connect()
            
            query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_timestamp >= '{month_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND block_timestamp <= '{month_end.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            
            result = client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
            
        except Exception as e:
            logger.warning(f"Error counting blocks: {e}")
            return 0
    
    def _check_data_exists(
        self,
        table_name: str,
        start_block: int,
        end_block: int,
        month_start: datetime,
        month_end: datetime
    ) -> bool:
        """Check if any data exists in range with partition filtering."""
        try:
            client = self.clickhouse._connect()
            
            query = f"""
            SELECT 1
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_timestamp >= '{month_start.strftime('%Y-%m-%d %H:%M:%S')}'
              AND block_timestamp <= '{month_end.strftime('%Y-%m-%d %H:%M:%S')}'
            LIMIT 1
            """
            
            result = client.query(query)
            return len(result.result_rows) > 0
            
        except Exception as e:
            logger.warning(f"Error checking data existence: {e}")
            return False
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            client = self.clickhouse._connect()
            query = f"""
            SELECT 1 FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            LIMIT 1
            """
            result = client.query(query)
            return len(result.result_rows) > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def _entry_exists(
        self,
        dataset: str,
        start_block: int,
        end_block: int
    ) -> bool:
        """Check if an indexing_state entry exists."""
        try:
            client = self.clickhouse._connect()
            query = f"""
            SELECT 1
            FROM {self.database}.indexing_state
            WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
            LIMIT 1
            """
            result = client.query(query)
            return len(result.result_rows) > 0
        except Exception as e:
            logger.error(f"Error checking entry existence: {e}")
            return False
    
    def _create_range_entry(
        self,
        dataset: str,
        start_block: int,
        end_block: int,
        status: str,
        metadata: str = ""
    ) -> bool:
        """Create an indexing_state entry."""
        try:
            client = self.clickhouse._connect()
            
            query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, 
             completed_at, rows_indexed, batch_id, error_message)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             '{status}', now(), 0, 'backfill-blocks', '{metadata}')
            """
            
            client.command(query)
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexing_state entry: {e}")
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
            
            delete_query = f"""
            ALTER TABLE {self.database}.indexing_state
            DELETE WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND ((start_block >= {start_block} AND start_block < {end_block})
                OR (end_block > {start_block} AND end_block <= {end_block})
                OR (start_block <= {start_block} AND end_block >= {end_block}))
            """
            
            client.command(delete_query)
            logger.info(f"Deleted existing entries for {dataset} in range {start_block}-{end_block}")
            return 0
            
        except Exception as e:
            logger.error(f"Error deleting existing entries: {e}")
            return 0
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 80)
        print("BACKFILL SUMMARY (Blocks-driven)")
        print("=" * 80)
        
        total_created = 0
        total_skipped = 0
        
        for dataset, stats in results.items():
            if not stats.exists:
                print(f"\n{dataset}: TABLE NOT FOUND")
                continue
            
            print(f"\n{dataset}:")
            print(f"  Table: {stats.table_name}")
            if stats.min_block is not None and stats.max_block is not None:
                print(f"  Block range: {stats.min_block:,} - {stats.max_block:,}")
            print(f"  Range size: {self.range_size} blocks")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            print(f"  Duration: {stats.duration:.2f}s")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
        
        print(f"\nTOTAL:")
        print(f"  Created: {total_created}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Duration: {results[list(results.keys())[0]].duration:.2f}s if results else 0")
        print("=" * 80 + "\n")
    
    def validate_backfill(self, datasets: List[str]) -> bool:
        """Validate backfill results."""
        logger.info("Validating backfill results...")
        
        success = True
        for dataset in datasets:
            table_name = self.dataset_tables.get(dataset, dataset)
            
            try:
                client = self.clickhouse._connect()
                
                # Just check if we have any entries
                query = f"""
                SELECT COUNT(*)
                FROM {self.database}.indexing_state
                WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                """
                
                result = client.query(query)
                count = result.result_rows[0][0] if result.result_rows else 0
                
                if count > 0:
                    logger.info(f"✓ {dataset}: Has {count} indexing_state entries")
                else:
                    logger.warning(f"✗ {dataset}: No indexing_state entries found")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error validating {dataset}: {e}")
                success = False
        
        return success