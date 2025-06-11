"""
Backfill Worker - Partition-aware version that processes one month at a time.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time
from datetime import datetime, timedelta
import calendar

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
        logger.info("Using partition-aware processing")
    
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
        Backfill indexing_state for specified datasets using partition-aware processing.
        """
        results = {}
        
        logger.info(f"Starting partition-aware backfill for datasets: {datasets}")
        logger.info(f"Using fixed range size: {self.range_size} blocks")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block:,} to {end_block:,}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        start_time = time.time()
        
        for dataset in datasets:
            logger.info(f"Processing dataset: {dataset}")
            stats = self._backfill_dataset_by_partition(dataset, start_block, end_block, force)
            stats.duration = time.time() - start_time
            results[dataset] = stats
        
        # Print summary
        self._print_backfill_summary(results)
        return results
    
    def _backfill_dataset_by_partition(
        self,
        dataset: str,
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> BackfillStats:
        """Backfill a single dataset by processing one partition at a time."""
        stats = BackfillStats(dataset=dataset, table_name="")
        
        # Get table name
        table_name = self.dataset_tables.get(dataset, dataset)
        stats.table_name = table_name
        
        # Check if table exists using system.tables
        try:
            client = self.clickhouse._connect()
            result = client.query(f"""
            SELECT 1 FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            LIMIT 1
            """)
            if len(result.result_rows) == 0:
                stats.exists = False
                logger.warning(f"{dataset}: Table {table_name} does not exist")
                return stats
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            stats.exists = False
            return stats
        
        stats.exists = True
        
        # Get list of partitions from system.parts
        try:
            partitions = self._get_partitions_list(table_name, start_block, end_block)
            if not partitions:
                logger.warning(f"No partitions found for {dataset}")
                return stats
            
            logger.info(f"{dataset}: Found {len(partitions)} partitions to process")
            
            # Process each partition separately
            for partition_info in partitions:
                partition = partition_info['partition']
                partition_min_block = partition_info['min_block']
                partition_max_block = partition_info['max_block']
                partition_rows = partition_info['rows']
                
                logger.info(
                    f"Processing partition {partition}: blocks {partition_min_block:,}-{partition_max_block:,}, "
                    f"~{partition_rows:,} rows"
                )
                
                # Apply block range constraints if specified
                process_start = partition_min_block
                process_end = partition_max_block
                
                if start_block is not None and process_start < start_block:
                    process_start = start_block
                if end_block is not None and process_end > end_block:
                    process_end = end_block
                
                # Skip if partition is outside requested range
                if process_start > process_end:
                    logger.debug(f"Skipping partition {partition} - outside requested range")
                    continue
                
                # Align to range boundaries
                aligned_start = (process_start // self.range_size) * self.range_size
                aligned_end = ((process_end // self.range_size) + 1) * self.range_size
                
                # Process this partition's ranges
                created, skipped = self._process_partition_ranges(
                    dataset, table_name, partition, aligned_start, aligned_end, force
                )
                
                stats.created_ranges += created
                stats.skipped_ranges += skipped
                stats.total_rows += partition_rows
                
                # Update min/max blocks
                if stats.min_block is None or partition_min_block < stats.min_block:
                    stats.min_block = partition_min_block
                if stats.max_block is None or partition_max_block > stats.max_block:
                    stats.max_block = partition_max_block
            
            stats.total_ranges = stats.created_ranges + stats.skipped_ranges
            stats.completed_ranges = stats.created_ranges
            stats.incomplete_ranges = 0
            
        except Exception as e:
            logger.error(f"Error processing dataset {dataset}: {e}", exc_info=True)
        
        return stats
    
    def _get_partitions_list(
        self,
        table_name: str,
        start_block: Optional[int],
        end_block: Optional[int]
    ) -> List[Dict]:
        """Get list of partitions with their block ranges."""
        try:
            client = self.clickhouse._connect()
            
            # Build WHERE clause
            where_parts = [
                f"database = '{self.database}'",
                f"table = '{table_name}'",
                "active = 1"
            ]
            
            if start_block is not None:
                where_parts.append(f"max_block_number >= {start_block}")
            if end_block is not None:
                where_parts.append(f"min_block_number <= {end_block}")
            
            where_clause = " AND ".join(where_parts)
            
            # Get partition statistics
            query = f"""
            SELECT 
                partition,
                MIN(min_block_number) as min_block,
                MAX(max_block_number) as max_block,
                SUM(rows) as total_rows,
                COUNT() as parts_count
            FROM system.parts
            WHERE {where_clause}
            GROUP BY partition
            ORDER BY partition
            """
            
            result = client.query(query)
            
            partitions = []
            for row in result.result_rows:
                partitions.append({
                    'partition': row[0],
                    'min_block': row[1],
                    'max_block': row[2],
                    'rows': row[3],
                    'parts_count': row[4]
                })
            
            return partitions
            
        except Exception as e:
            logger.error(f"Error getting partitions list: {e}")
            return []
    
    def _process_partition_ranges(
        self,
        dataset: str,
        table_name: str,
        partition: str,
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
        
        # For blocks dataset, we need to check completeness
        # For other datasets, we assume if partition has data, the ranges are complete
        is_blocks_dataset = (dataset == 'blocks')
        
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
                
                # For blocks dataset, check if range is complete
                if is_blocks_dataset:
                    block_count = self._count_blocks_in_range_for_partition(
                        table_name, partition, current, range_end
                    )
                    expected_blocks = range_end - current
                    is_complete = (block_count == expected_blocks)
                    status = 'completed' if is_complete else 'incomplete'
                else:
                    # For other datasets, trust that partition data is complete
                    status = 'completed'
                
                # Create the entry
                if self._create_range_entry(
                    dataset=dataset,
                    start_block=current,
                    end_block=range_end,
                    status=status,
                    metadata=f"partition:{partition}"
                ):
                    created += 1
                    logger.debug(f"Created entry for {dataset} {current:,}-{range_end:,} (status={status})")
                
            except Exception as e:
                logger.error(f"Error processing range {current}-{range_end}: {e}")
            
            current = range_end
        
        return created, skipped
    
    def _count_blocks_in_range_for_partition(
        self,
        table_name: str,
        partition: str,
        start_block: int,
        end_block: int
    ) -> int:
        """Count blocks in a range for a specific partition (efficient query)."""
        try:
            client = self.clickhouse._connect()
            
            # Convert partition (YYYYMM) to date range for efficient querying
            year = int(partition[:4])
            month = int(partition[4:6])
            
            # Get first and last day of the month
            first_day = datetime(year, month, 1)
            last_day = datetime(year, month, calendar.monthrange(year, month)[1], 23, 59, 59)
            
            # Query with partition pruning
            query = f"""
            SELECT COUNT(DISTINCT block_number) 
            FROM {self.database}.{table_name}
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND block_timestamp >= '{first_day.strftime('%Y-%m-%d %H:%M:%S')}'
              AND block_timestamp <= '{last_day.strftime('%Y-%m-%d %H:%M:%S')}'
            SETTINGS max_memory_usage = 1000000000
            """
            
            result = client.query(query)
            return result.result_rows[0][0] if result.result_rows else 0
            
        except Exception as e:
            logger.warning(f"Error counting blocks (assuming incomplete): {e}")
            return 0  # Assume incomplete on error
    
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
             '{status}', now(), 0, 'backfill-partition', '{metadata}')
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
        print("BACKFILL SUMMARY (Partition-aware Mode)")
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
            print(f"  Total rows: ~{stats.total_rows:,}")
            print(f"  Range size: {self.range_size} blocks")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            print(f"  Status: Marked as 'completed' (partition-based)")
            print(f"  Duration: {stats.duration:.2f}s")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
        
        print(f"\nTOTAL:")
        print(f"  Created: {total_created}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Duration: {results[list(results.keys())[0]].duration:.2f}s if results else 0")
        print("\nNote: Processed by partition to avoid memory issues.")
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
                LIMIT 1
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