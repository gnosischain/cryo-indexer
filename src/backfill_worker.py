"""
Backfill Worker - Analyzes existing data and populates indexing_state table.
"""
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from loguru import logger
import time

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
    continuous_ranges: List[Tuple[int, int]] = None
    created_ranges: int = 0
    skipped_ranges: int = 0
    deleted_ranges: int = 0
    
    def __post_init__(self):
        if self.continuous_ranges is None:
            self.continuous_ranges = []


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
        
        logger.info(f"BackfillWorker initialized for mode {mode}")
    
    def backfill_datasets(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, BackfillStats]:
        """
        Backfill indexing_state for specified datasets.
        
        Args:
            datasets: List of datasets to backfill
            start_block: Optional start block (None = from beginning)
            end_block: Optional end block (None = to end)
            force: Force recreation of existing entries
            
        Returns:
            Dictionary of dataset -> BackfillStats
        """
        results = {}
        
        logger.info(f"Starting backfill for datasets: {datasets}")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block} to {end_block}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        for dataset in datasets:
            logger.info(f"Processing dataset: {dataset}")
            stats = self._backfill_dataset(dataset, start_block, end_block, force)
            results[dataset] = stats
            
            if stats.exists:
                logger.info(
                    f"{dataset}: Created {stats.created_ranges} ranges, "
                    f"skipped {stats.skipped_ranges} existing ranges"
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
        """Backfill a single dataset."""
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
        
        # Find continuous ranges
        continuous_ranges = self._find_continuous_ranges(
            table_name, stats.min_block, stats.max_block
        )
        stats.continuous_ranges = continuous_ranges
        logger.info(f"{dataset}: Found {len(continuous_ranges)} continuous ranges")
        
        # Create indexing_state entries
        created, skipped = self._create_indexing_state_entries(
            dataset, continuous_ranges, force
        )
        stats.created_ranges = created
        stats.skipped_ranges = skipped
        
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
    
    def _find_continuous_ranges(
        self,
        table_name: str,
        min_block: int,
        max_block: int
    ) -> List[Tuple[int, int]]:
        """Find continuous block ranges in a table."""
        try:
            client = self.clickhouse._connect()
            
            # Use chunked approach for large ranges
            chunk_size = settings.backfill_chunk_size
            all_blocks = set()
            
            logger.debug(f"Scanning blocks in {table_name} from {min_block} to {max_block}")
            
            current = min_block
            while current <= max_block:
                chunk_end = min(current + chunk_size, max_block)
                
                # Get distinct blocks in this chunk
                result = client.query(f"""
                SELECT DISTINCT block_number 
                FROM {self.database}.{table_name}
                WHERE block_number >= {current} 
                  AND block_number <= {chunk_end}
                  AND block_number IS NOT NULL
                ORDER BY block_number
                """)
                
                chunk_blocks = [row[0] for row in result.result_rows]
                all_blocks.update(chunk_blocks)
                
                logger.debug(f"Chunk {current}-{chunk_end}: {len(chunk_blocks)} blocks")
                current = chunk_end + 1
            
            # Convert to sorted list
            sorted_blocks = sorted(all_blocks)
            
            if not sorted_blocks:
                return []
            
            # Find continuous ranges
            ranges = []
            start = sorted_blocks[0]
            end = sorted_blocks[0]
            
            for i in range(1, len(sorted_blocks)):
                if sorted_blocks[i] == end + 1:
                    end = sorted_blocks[i]
                else:
                    ranges.append((start, end))
                    start = sorted_blocks[i]
                    end = sorted_blocks[i]
            
            ranges.append((start, end))
            
            logger.debug(f"Found {len(ranges)} continuous ranges in {table_name}")
            return ranges
            
        except Exception as e:
            logger.error(f"Error finding continuous ranges in {table_name}: {e}")
            return []
    
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
    
    def _create_indexing_state_entries(
        self,
        dataset: str,
        continuous_ranges: List[Tuple[int, int]],
        force: bool
    ) -> Tuple[int, int]:
        """Create indexing_state entries for continuous ranges."""
        created = 0
        skipped = 0
        
        for start, end in continuous_ranges:
            # Check if entry already exists
            if not force and self._entry_exists(dataset, start, end + 1):
                skipped += 1
                logger.debug(f"Entry already exists for {dataset} {start}-{end + 1}, skipping")
                continue
            
            # Calculate batch ranges based on backfill_batch_size
            current = start
            while current <= end:
                batch_end = min(current + settings.backfill_batch_size, end + 1)
                
                # Count rows in this range for accurate tracking
                rows_count = self._count_rows_in_range(
                    self.dataset_tables[dataset], 
                    current, 
                    batch_end
                )
                
                # Create the entry
                if self._create_entry(dataset, current, batch_end, rows_count):
                    created += 1
                    logger.debug(f"Created entry for {dataset} {current}-{batch_end} ({rows_count} rows)")
                else:
                    logger.error(f"Failed to create entry for {dataset} {current}-{batch_end}")
                
                current = batch_end
        
        return created, skipped
    
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
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 60)
        print("BACKFILL SUMMARY")
        print("=" * 60)
        
        total_created = 0
        total_skipped = 0
        total_deleted = 0
        
        for dataset, stats in results.items():
            if not stats.exists:
                print(f"\n{dataset}: TABLE NOT FOUND")
                continue
            
            print(f"\n{dataset}:")
            print(f"  Table: {stats.table_name}")
            print(f"  Block range: {stats.min_block} - {stats.max_block}")
            print(f"  Total rows: {stats.total_rows:,}")
            print(f"  Continuous ranges: {len(stats.continuous_ranges)}")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            if stats.deleted_ranges > 0:
                print(f"  Deleted entries: {stats.deleted_ranges}")
            
            if stats.continuous_ranges:
                print("  Ranges:")
                for i, (start, end) in enumerate(stats.continuous_ranges[:5]):
                    print(f"    {i+1}. {start:,} - {end:,} ({end-start+1:,} blocks)")
                if len(stats.continuous_ranges) > 5:
                    print(f"    ... and {len(stats.continuous_ranges) - 5} more ranges")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
            total_deleted += stats.deleted_ranges
        
        print(f"\nTOTAL: Created {total_created}, Skipped {total_skipped}")
        if total_deleted > 0:
            print(f"       Deleted {total_deleted} (force mode)")
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