"""
Backfill Worker - Simplified version that avoids memory-intensive queries.
Creates indexing_state entries based on system.parts metadata only.
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
        Simplified version that uses system.parts for metadata.
        """
        results = {}
        
        # Determine number of workers
        if max_workers is None:
            max_workers = settings.workers
        
        logger.info(f"Starting parallel backfill for {len(datasets)} datasets with {max_workers} workers")
        logger.info(f"Using fixed range size: {self.range_size} blocks")
        logger.info("Using simplified mode to avoid memory issues")
        if start_block is not None and end_block is not None:
            logger.info(f"Block range: {start_block:,} to {end_block:,}")
        if force:
            logger.info("FORCE MODE: Will delete and recreate existing entries")
        
        start_time = time.time()
        
        # First pass: analyze all datasets to find work
        dataset_work = {}
        for dataset in datasets:
            work_info = self._analyze_dataset_simple(dataset, start_block, end_block, force)
            if work_info:
                dataset_work[dataset] = work_info
        
        if not dataset_work:
            logger.warning("No work found for any dataset")
            return results
        
        # For simplified mode, process sequentially to avoid overwhelming the system
        logger.info(f"Processing {len(dataset_work)} datasets sequentially")
        
        for dataset, work_info in dataset_work.items():
            stats = BackfillStats(
                dataset=dataset,
                table_name=work_info['table_name'],
                exists=True,
                min_block=work_info['min_block'],
                max_block=work_info['max_block'],
                total_rows=work_info['total_rows']
            )
            
            # Process this dataset
            try:
                created, skipped = self._process_dataset_simple(
                    dataset,
                    work_info['table_name'],
                    work_info['min_block'],
                    work_info['max_block'],
                    force
                )
                
                stats.created_ranges = created
                stats.skipped_ranges = skipped
                stats.total_ranges = created + skipped
                
                # All marked as completed since we have existing data
                stats.completed_ranges = created
                stats.incomplete_ranges = 0
                    
            except Exception as e:
                logger.error(f"Error processing {dataset}: {e}")
                stats.created_ranges = 0
                stats.skipped_ranges = 0
            
            results[dataset] = stats
        
        # Calculate durations
        total_duration = time.time() - start_time
        for stats in results.values():
            stats.duration = total_duration
        
        logger.info(f"Backfill completed in {total_duration:.2f}s")
        
        # Print summary
        self._print_backfill_summary(results)
        return results
    
    def _analyze_dataset_simple(
        self,
        dataset: str,
        start_block: Optional[int],
        end_block: Optional[int],
        force: bool
    ) -> Optional[Dict]:
        """Analyze a dataset using system.parts to avoid memory issues."""
        table_name = self.dataset_tables.get(dataset, dataset)
        
        # Check if table exists
        if not self._table_exists(table_name):
            logger.warning(f"{dataset}: Table {table_name} does not exist")
            return None
        
        # Get data range from system.parts
        try:
            client = self.clickhouse._connect()
            
            # Build WHERE clause
            where_parts = [
                f"database = '{self.database}'",
                f"table = '{table_name}'",
                "active = 1"
            ]
            
            # For specific ranges, filter parts
            if start_block is not None:
                where_parts.append(f"max_block_number >= {start_block}")
            if end_block is not None:
                where_parts.append(f"min_block_number <= {end_block}")
            
            where_clause = " AND ".join(where_parts)
            
            # Get range from system.parts
            query = f"""
            SELECT 
                COALESCE(MIN(min_block_number), 0) as min_block,
                COALESCE(MAX(max_block_number), 0) as max_block,
                COALESCE(SUM(rows), 0) as total_rows
            FROM system.parts
            WHERE {where_clause}
            """
            
            result = client.query(query)
            if not result.result_rows or result.result_rows[0][0] == 0:
                logger.warning(f"No data found for {dataset} in system.parts")
                return None
            
            min_block, max_block, total_rows = result.result_rows[0]
            
            # Apply requested range constraints
            if start_block is not None and min_block < start_block:
                min_block = start_block
            if end_block is not None and max_block > end_block:
                max_block = end_block
            
            # Align to range boundaries
            aligned_min = (min_block // self.range_size) * self.range_size
            aligned_max = ((max_block // self.range_size) + 1) * self.range_size
            
            logger.info(
                f"{dataset}: blocks {min_block:,}-{max_block:,} "
                f"(aligned: {aligned_min:,}-{aligned_max:,}), "
                f"~{total_rows:,} rows (from system.parts)"
            )
            
            # If force mode, handle deletions
            if force:
                if start_block is not None and end_block is not None:
                    self._delete_existing_entries_simple(dataset, start_block, end_block)
                else:
                    self._delete_existing_entries_simple(dataset, aligned_min, aligned_max)
            
            return {
                'dataset': dataset,
                'table_name': table_name,
                'min_block': aligned_min,
                'max_block': aligned_max,
                'total_rows': total_rows,
                'force': force
            }
            
        except Exception as e:
            logger.error(f"Error analyzing dataset {dataset}: {e}")
            return None
    
    def _process_dataset_simple(
        self,
        dataset: str,
        table_name: str,
        min_block: int,
        max_block: int,
        force: bool
    ) -> Tuple[int, int]:
        """Process dataset using simplified approach."""
        created = 0
        skipped = 0
        
        logger.info(f"Processing {dataset} with simplified approach")
        
        # Get parts information for this dataset
        parts_info = self._get_parts_info(table_name, min_block, max_block)
        
        # Process in fixed-size chunks
        current = min_block
        while current < max_block:
            range_end = min(current + self.range_size, max_block)
            
            try:
                # Check if entry already exists (simple query)
                if not force and self._entry_exists_simple(dataset, current, range_end):
                    skipped += 1
                    current = range_end
                    continue
                
                # Check if we have any parts covering this range
                has_data = any(
                    part['min_block'] <= current < part['max_block'] or
                    part['min_block'] < range_end <= part['max_block'] or
                    (current <= part['min_block'] and range_end >= part['max_block'])
                    for part in parts_info
                )
                
                if has_data:
                    # Since we have existing data and can't verify due to memory constraints,
                    # mark as 'completed' to avoid re-downloading and creating duplicates
                    status = 'completed'
                    
                    # Create the entry
                    if self._create_range_entry_simple(
                        dataset=dataset,
                        start_block=current,
                        end_block=range_end,
                        status=status
                    ):
                        created += 1
                        logger.debug(f"Created entry for {dataset} {current:,}-{range_end:,} (status={status})")
                else:
                    logger.debug(f"No data found for {dataset} {current:,}-{range_end:,}, skipping")
                
            except Exception as e:
                logger.error(f"Error processing range {current}-{range_end}: {e}")
            
            current = range_end
        
        return created, skipped
    
    def _get_parts_info(
        self,
        table_name: str,
        min_block: int,
        max_block: int
    ) -> List[Dict]:
        """Get parts information for a table in the given range."""
        try:
            client = self.clickhouse._connect()
            
            query = f"""
            SELECT 
                partition,
                min_block_number,
                max_block_number,
                rows
            FROM system.parts
            WHERE database = '{self.database}'
              AND table = '{table_name}'
              AND active = 1
              AND max_block_number >= {min_block}
              AND min_block_number <= {max_block}
            ORDER BY min_block_number
            """
            
            result = client.query(query)
            
            parts = []
            for row in result.result_rows:
                parts.append({
                    'partition': row[0],
                    'min_block': row[1],
                    'max_block': row[2],
                    'rows': row[3]
                })
            
            return parts
            
        except Exception as e:
            logger.error(f"Error getting parts info: {e}")
            return []
    
    def _entry_exists_simple(
        self,
        dataset: str,
        start_block: int,
        end_block: int
    ) -> bool:
        """Simple check if an indexing_state entry exists."""
        try:
            client = self.clickhouse._connect()
            
            # Use EXISTS for efficiency
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
    
    def _create_range_entry_simple(
        self,
        dataset: str,
        start_block: int,
        end_block: int,
        status: str
    ) -> bool:
        """Create an indexing_state entry without expensive queries."""
        try:
            client = self.clickhouse._connect()
            
            # Simple metadata
            metadata = f"backfill-simple,range:{self.range_size}"
            
            query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, 
             completed_at, rows_indexed, batch_id, error_message)
            VALUES
            ('{self.mode}', '{dataset}', {start_block}, {end_block}, 
             '{status}', now(), 0, 'backfill-simple', '{metadata}')
            """
            
            client.command(query)
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexing_state entry: {e}")
            return False
    
    def _delete_existing_entries_simple(
        self,
        dataset: str,
        min_block: int,
        max_block: int
    ) -> int:
        """Delete existing indexing_state entries in the range."""
        try:
            client = self.clickhouse._connect()
            
            # Delete without counting first (to avoid memory issues)
            delete_query = f"""
            ALTER TABLE {self.database}.indexing_state
            DELETE WHERE mode = '{self.mode}'
              AND dataset = '{dataset}'
              AND start_block >= {min_block}
              AND end_block <= {max_block}
            """
            
            client.command(delete_query)
            logger.info(f"Deleted existing entries for {dataset} in range {min_block}-{max_block}")
            return 0  # Don't count to avoid memory issues
            
        except Exception as e:
            logger.error(f"Error deleting existing entries: {e}")
            return 0
    
    def backfill_datasets(
        self,
        datasets: List[str],
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        force: bool = False
    ) -> Dict[str, BackfillStats]:
        """Single-threaded version - just calls the parallel version with 1 worker."""
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
            
            # Use EXISTS to avoid counting
            query = f"""
            SELECT 1
            FROM system.tables 
            WHERE database = '{self.database}' 
              AND name = '{table_name}'
            LIMIT 1
            """
            
            result = client.query(query)
            return len(result.result_rows) > 0
            
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def _print_backfill_summary(self, results: Dict[str, BackfillStats]) -> None:
        """Print a summary of the backfill operation."""
        print("\n" + "=" * 80)
        print("BACKFILL SUMMARY (Simplified Mode)")
        print("=" * 80)
        
        total_created = 0
        total_skipped = 0
        total_complete = 0
        total_incomplete = 0
        
        for dataset, stats in results.items():
            if not stats.exists:
                print(f"\n{dataset}: TABLE NOT FOUND")
                continue
            
            print(f"\n{dataset}:")
            print(f"  Table: {stats.table_name}")
            print(f"  Block range: {stats.min_block:,} - {stats.max_block:,}")
            print(f"  Approximate rows: ~{stats.total_rows:,}")
            print(f"  Range size: {self.range_size} blocks")
            print(f"  Created entries: {stats.created_ranges}")
            print(f"  Skipped entries: {stats.skipped_ranges}")
            print(f"  Status: All marked as 'completed' (trusting existing data)")
            print(f"  Duration: {stats.duration:.2f}s")
            
            total_created += stats.created_ranges
            total_skipped += stats.skipped_ranges
            total_complete += stats.completed_ranges
            total_incomplete += stats.incomplete_ranges
        
        print(f"\nTOTAL:")
        print(f"  Created: {total_created}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Complete: {total_complete}")
        print(f"  Incomplete: {total_incomplete}")
        print(f"  Duration: {results[list(results.keys())[0]].duration:.2f}s")
        print("\nNote: This is a simplified backfill that avoids memory-intensive queries.")
        print("Run 'validate' and 'fill-gaps' operations to verify and complete the data.")
        print("=" * 80 + "\n")
    
    def validate_backfill(self, datasets: List[str]) -> bool:
        """Simplified validation."""
        logger.info("Validating backfill results...")
        
        success = True
        for dataset in datasets:
            table_name = self.dataset_tables.get(dataset, dataset)
            
            if not self._table_exists(table_name):
                continue
            
            try:
                client = self.clickhouse._connect()
                
                # Just check if we have any entries
                query = f"""
                SELECT 1
                FROM {self.database}.indexing_state
                WHERE mode = '{self.mode}'
                  AND dataset = '{dataset}'
                LIMIT 1
                """
                
                result = client.query(query)
                
                if len(result.result_rows) > 0:
                    logger.info(f"✓ {dataset}: Has indexing_state entries")
                else:
                    logger.warning(f"✗ {dataset}: No indexing_state entries found")
                    success = False
                    
            except Exception as e:
                logger.error(f"Error validating {dataset}: {e}")
                success = False
        
        return success