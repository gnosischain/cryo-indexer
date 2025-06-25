"""
Timestamp Fixer - Fixes incorrect timestamps in blockchain data tables.
Handles cases where block_timestamp is '1970-01-01 00:00:00' due to missing block data.
"""
import os
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from loguru import logger
import time
from datetime import datetime

from .db.clickhouse_manager import ClickHouseManager
from .core.state_manager import StateManager
from .config import settings


@dataclass
class TimestampFixResult:
    """Result of timestamp fix operation for a dataset."""
    dataset: str
    total_affected: int
    total_fixed: int
    failed_ranges: List[Tuple[int, int]]
    duration: float
    error: Optional[str] = None


class TimestampFixer:
    """Fixes incorrect timestamps in blockchain data tables."""
    
    def __init__(
        self,
        clickhouse: ClickHouseManager,
        state_manager: StateManager
    ):
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.database = clickhouse.database
        
        # Tables that can have timestamp issues (excluding blocks)
        self.fixable_tables = [
            'transactions',
            'logs',
            'contracts',
            'native_transfers',
            'traces',
            'balance_diffs',
            'code_diffs',
            'nonce_diffs',
            'storage_diffs'
        ]
        
        # Batch size for processing
        self.fix_batch_size = int(os.environ.get("TIMESTAMP_FIX_BATCH_SIZE", "100000"))
        
        logger.info(f"TimestampFixer initialized with batch size {self.fix_batch_size}")
    
    def find_affected_datasets(self) -> Dict[str, int]:
        """Find all datasets with incorrect timestamps."""
        affected = {}
        
        logger.info("Scanning for datasets with incorrect timestamps...")
        
        for table in self.fixable_tables:
            if not self._table_exists(table):
                continue
                
            try:
                client = self.clickhouse._connect()
                
                # Count affected rows
                query = f"""
                SELECT COUNT(*) as cnt
                FROM {self.database}.{table}
                WHERE block_timestamp = '1970-01-01 00:00:00'
                """
                
                result = client.query(query)
                count = result.result_rows[0][0] if result.result_rows else 0
                
                if count > 0:
                    affected[table] = count
                    logger.warning(f"Found {count:,} rows with incorrect timestamps in {table}")
                else:
                    logger.info(f"✓ {table} has no timestamp issues")
                    
            except Exception as e:
                logger.error(f"Error checking {table}: {e}")
        
        return affected
    
    def fix_timestamps(self, datasets: Optional[List[str]] = None) -> Dict[str, TimestampFixResult]:
        """
        Fix timestamps for specified datasets or all affected datasets.
        
        Args:
            datasets: List of dataset names to fix. If None, fix all affected datasets.
            
        Returns:
            Dictionary mapping dataset name to fix results
        """
        results = {}
        
        # If no datasets specified, find all affected
        if datasets is None:
            affected = self.find_affected_datasets()
            datasets = list(affected.keys())
        
        if not datasets:
            logger.info("No datasets with timestamp issues found")
            return results
        
        logger.info(f"Starting timestamp fix for {len(datasets)} datasets")
        
        # Process each dataset
        for dataset in datasets:
            logger.info(f"\nProcessing {dataset}...")
            start_time = time.time()
            
            try:
                result = self._fix_dataset_timestamps(dataset)
                result.duration = time.time() - start_time
                results[dataset] = result
                
                if result.total_fixed == result.total_affected:
                    logger.info(f"✓ Successfully fixed all {result.total_fixed:,} timestamps in {dataset}")
                else:
                    logger.warning(
                        f"⚠ Fixed {result.total_fixed:,}/{result.total_affected:,} "
                        f"timestamps in {dataset}"
                    )
                    
            except Exception as e:
                logger.error(f"Error fixing {dataset}: {e}")
                results[dataset] = TimestampFixResult(
                    dataset=dataset,
                    total_affected=0,
                    total_fixed=0,
                    failed_ranges=[],
                    duration=time.time() - start_time,
                    error=str(e)
                )
        
        return results
    
    def _fix_dataset_timestamps(self, dataset: str) -> TimestampFixResult:
        """Fix timestamps for a single dataset."""
        # First, find affected ranges
        affected_ranges = self._find_affected_ranges(dataset)
        
        if not affected_ranges:
            return TimestampFixResult(
                dataset=dataset,
                total_affected=0,
                total_fixed=0,
                failed_ranges=[],
                duration=0.0
            )
        
        total_affected = sum(r[2] for r in affected_ranges)
        total_fixed = 0
        failed_ranges = []
        
        logger.info(f"Found {len(affected_ranges)} affected ranges with {total_affected:,} rows total")
        
        # Process each range
        for i, (start_block, end_block, count) in enumerate(affected_ranges):
            logger.info(
                f"[{i+1}/{len(affected_ranges)}] Fixing range {start_block:,}-{end_block:,} "
                f"({count:,} rows)"
            )
            
            try:
                fixed_count = self._fix_range_timestamps(dataset, start_block, end_block)
                total_fixed += fixed_count
                
                if fixed_count < count:
                    logger.warning(
                        f"Only fixed {fixed_count}/{count} rows in range {start_block}-{end_block}"
                    )
                    
            except Exception as e:
                logger.error(f"Failed to fix range {start_block}-{end_block}: {e}")
                failed_ranges.append((start_block, end_block))
        
        # Record fix operation in tracking table
        self._record_fix_operation(dataset, total_affected, total_fixed, failed_ranges)
        
        return TimestampFixResult(
            dataset=dataset,
            total_affected=total_affected,
            total_fixed=total_fixed,
            failed_ranges=failed_ranges,
            duration=0.0  # Will be set by caller
        )
    
    def _find_affected_ranges(self, dataset: str) -> List[Tuple[int, int, int]]:
        """Find block ranges with incorrect timestamps."""
        ranges = []
        
        try:
            client = self.clickhouse._connect()
            
            # Find affected ranges using window functions
            query = f"""
            WITH affected_blocks AS (
                SELECT 
                    block_number,
                    block_number - ROW_NUMBER() OVER (ORDER BY block_number) AS grp
                FROM {self.database}.{dataset}
                WHERE block_timestamp = '1970-01-01 00:00:00'
                  AND block_number IS NOT NULL
            )
            SELECT 
                MIN(block_number) as start_block,
                MAX(block_number) as end_block,
                COUNT(*) as count
            FROM affected_blocks
            GROUP BY grp
            ORDER BY start_block
            """
            
            result = client.query(query)
            
            for row in result.result_rows:
                ranges.append((row[0], row[1], row[2]))
                
        except Exception as e:
            logger.error(f"Error finding affected ranges: {e}")
            
        return ranges
    
    def _fix_range_timestamps(self, dataset: str, start_block: int, end_block: int) -> int:
        """Fix timestamps for a specific block range."""
        try:
            client = self.clickhouse._connect()
            
            # Since we cannot UPDATE key columns in ClickHouse, we need to:
            # 1. Delete the affected rows
            # 2. Re-insert them with correct timestamps
            
            logger.debug(f"Fixing timestamps for {dataset} range {start_block}-{end_block}")
            
            # First, count affected rows
            count_query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.{dataset}
            WHERE block_number >= {start_block}
              AND block_number <= {end_block}
              AND block_timestamp = '1970-01-01 00:00:00'
            """
            result = client.query(count_query)
            affected_count = result.result_rows[0][0] if result.result_rows else 0
            
            if affected_count == 0:
                return 0
            
            # Get the columns of the table (excluding MATERIALIZED columns)
            columns_query = f"""
            SELECT name 
            FROM system.columns 
            WHERE database = '{self.database}' 
              AND table = '{dataset}'
              AND default_kind NOT IN ('MATERIALIZED', 'ALIAS')
            ORDER BY position
            """
            result = client.query(columns_query)
            columns = [row[0] for row in result.result_rows]
            
            # Build the SELECT part for re-insertion
            select_parts = []
            for col in columns:
                if col == 'block_timestamp':
                    # Use timestamp from blocks table
                    select_parts.append("toDateTime(b.timestamp) as block_timestamp")
                else:
                    select_parts.append(f"t.{col}")
            
            # Store the data to be fixed in memory (for small batches)
            # This approach works well for the small number of affected rows
            if affected_count <= 10000:  # Small enough to handle in memory
                # Get the data with corrected timestamps
                select_query = f"""
                SELECT {', '.join(select_parts)}
                FROM {self.database}.{dataset} t
                INNER JOIN {self.database}.blocks b ON t.block_number = b.block_number
                WHERE t.block_number >= {start_block}
                  AND t.block_number <= {end_block}
                  AND t.block_timestamp = '1970-01-01 00:00:00'
                  AND b.timestamp IS NOT NULL
                  AND b.timestamp > 0
                """
                
                result = client.query(select_query)
                rows_to_insert = result.result_rows
                
                if rows_to_insert:
                    # Delete the bad rows
                    delete_query = f"""
                    ALTER TABLE {self.database}.{dataset}
                    DELETE WHERE block_number >= {start_block}
                      AND block_number <= {end_block}
                      AND block_timestamp = '1970-01-01 00:00:00'
                    """
                    client.command(delete_query)
                    
                    # Wait for deletion to propagate
                    time.sleep(1)
                    
                    # Convert rows to DataFrame for easier insertion
                    import pandas as pd
                    df = pd.DataFrame(rows_to_insert, columns=columns)
                    
                    # Insert the corrected data
                    client.insert_df(f"{self.database}.{dataset}", df)
                    
                    logger.debug(f"Fixed {len(rows_to_insert)} timestamps in {dataset}")
                    return len(rows_to_insert)
            else:
                # For larger datasets, process in chunks
                fixed_count = 0
                chunk_size = 5000
                
                for offset in range(0, affected_count, chunk_size):
                    # Get chunk of data with corrected timestamps
                    select_query = f"""
                    SELECT {', '.join(select_parts)}
                    FROM {self.database}.{dataset} t
                    INNER JOIN {self.database}.blocks b ON t.block_number = b.block_number
                    WHERE t.block_number >= {start_block}
                      AND t.block_number <= {end_block}
                      AND t.block_timestamp = '1970-01-01 00:00:00'
                      AND b.timestamp IS NOT NULL
                      AND b.timestamp > 0
                    ORDER BY t.block_number, t.transaction_index
                    LIMIT {chunk_size} OFFSET {offset}
                    """
                    
                    result = client.query(select_query)
                    rows_to_insert = result.result_rows
                    
                    if rows_to_insert:
                        # Get the block range for this chunk
                        chunk_blocks = [row[columns.index('block_number')] for row in rows_to_insert]
                        chunk_min_block = min(chunk_blocks)
                        chunk_max_block = max(chunk_blocks)
                        
                        # Delete this chunk's bad rows
                        delete_query = f"""
                        ALTER TABLE {self.database}.{dataset}
                        DELETE WHERE block_number >= {chunk_min_block}
                          AND block_number <= {chunk_max_block}
                          AND block_timestamp = '1970-01-01 00:00:00'
                        """
                        client.command(delete_query)
                        
                        # Wait briefly
                        time.sleep(0.5)
                        
                        # Convert to DataFrame and insert
                        import pandas as pd
                        df = pd.DataFrame(rows_to_insert, columns=columns)
                        client.insert_df(f"{self.database}.{dataset}", df)
                        
                        fixed_count += len(rows_to_insert)
                        logger.debug(f"Fixed chunk {offset//chunk_size + 1}: {len(rows_to_insert)} rows")
                
                return fixed_count
            
            return 0
            
        except Exception as e:
            logger.error(f"Error fixing timestamps: {e}")
            raise
    
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
    
    def _record_fix_operation(
        self, 
        dataset: str, 
        total_affected: int, 
        total_fixed: int,
        failed_ranges: List[Tuple[int, int]]
    ):
        """Record the fix operation in the tracking table."""
        try:
            client = self.clickhouse._connect()
            
            # Check if timestamp_fixes table exists
            table_exists_query = f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' AND name = 'timestamp_fixes'
            """
            result = client.query(table_exists_query)
            
            if result.result_rows[0][0] == 0:
                logger.warning("timestamp_fixes table does not exist. Skipping recording.")
                return
            
            status = 'completed' if total_fixed == total_affected else 'partial'
            if total_fixed == 0:
                status = 'failed'
            
            error_message = ''
            if failed_ranges:
                error_message = f"Failed ranges: {failed_ranges[:5]}"
                if len(failed_ranges) > 5:
                    error_message += f" and {len(failed_ranges) - 5} more"
            
            query = f"""
            INSERT INTO {self.database}.timestamp_fixes
            (table_name, total_affected, total_fixed, status, error_message)
            VALUES
            ('{dataset}', {total_affected}, {total_fixed}, '{status}', '{error_message}')
            """
            
            client.command(query)
            
        except Exception as e:
            logger.error(f"Error recording fix operation: {e}")
    
    def print_fix_summary(self, results: Dict[str, TimestampFixResult]):
        """Print a summary of the fix operations."""
        print("\n" + "=" * 80)
        print("TIMESTAMP FIX SUMMARY")
        print("=" * 80)
        
        total_affected = 0
        total_fixed = 0
        total_duration = 0
        
        for dataset, result in results.items():
            print(f"\n{dataset}:")
            
            if result.error:
                print(f"  ERROR: {result.error}")
                continue
                
            print(f"  Affected rows: {result.total_affected:,}")
            print(f"  Fixed rows: {result.total_fixed:,}")
            
            if result.failed_ranges:
                print(f"  Failed ranges: {len(result.failed_ranges)}")
                for start, end in result.failed_ranges[:3]:
                    print(f"    - {start:,} to {end:,}")
                if len(result.failed_ranges) > 3:
                    print(f"    ... and {len(result.failed_ranges) - 3} more")
                    
            print(f"  Duration: {result.duration:.2f}s")
            
            if result.total_fixed == result.total_affected:
                print(f"  Status: ✓ SUCCESS")
            elif result.total_fixed > 0:
                print(f"  Status: ⚠ PARTIAL ({result.total_fixed/result.total_affected*100:.1f}%)")
            else:
                print(f"  Status: ✗ FAILED")
            
            total_affected += result.total_affected
            total_fixed += result.total_fixed
            total_duration += result.duration
        
        print(f"\nTOTAL:")
        print(f"  Affected: {total_affected:,}")
        print(f"  Fixed: {total_fixed:,}")
        if total_affected > 0:
            print(f"  Success rate: {total_fixed/total_affected*100:.1f}%")
        print(f"  Duration: {total_duration:.2f}s")
        print("=" * 80 + "\n")
    
    def verify_fixes(self, datasets: Optional[List[str]] = None) -> Dict[str, Dict]:
        """Verify that timestamp fixes were successful."""
        if datasets is None:
            datasets = self.fixable_tables
            
        verification = {}
        
        for dataset in datasets:
            if not self._table_exists(dataset):
                continue
                
            try:
                client = self.clickhouse._connect()
                
                # Check for remaining bad timestamps
                query = f"""
                SELECT 
                    COUNT(*) as bad_timestamps,
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM {self.database}.{dataset}
                WHERE block_timestamp = '1970-01-01 00:00:00'
                """
                
                result = client.query(query)
                row = result.result_rows[0] if result.result_rows else (0, None, None)
                
                verification[dataset] = {
                    'remaining_bad': row[0],
                    'min_block': row[1],
                    'max_block': row[2],
                    'status': 'clean' if row[0] == 0 else 'needs_fix'
                }
                
            except Exception as e:
                logger.error(f"Error verifying {dataset}: {e}")
                verification[dataset] = {'error': str(e)}
        
        return verification