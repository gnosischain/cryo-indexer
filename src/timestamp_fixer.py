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
            
            # Process in smaller chunks to avoid memory issues
            fixed_count = 0
            current_block = start_block
            
            while current_block <= end_block:
                chunk_end = min(current_block + self.fix_batch_size, end_block + 1)
                
                # First, verify we have the block data
                verify_query = f"""
                SELECT COUNT(DISTINCT block_number) as cnt
                FROM {self.database}.blocks
                WHERE block_number >= {current_block}
                  AND block_number < {chunk_end}
                  AND timestamp IS NOT NULL
                  AND timestamp > 0
                """
                
                result = client.query(verify_query)
                available_blocks = result.result_rows[0][0] if result.result_rows else 0
                expected_blocks = chunk_end - current_block
                
                if available_blocks < expected_blocks:
                    logger.warning(
                        f"Only {available_blocks}/{expected_blocks} blocks available "
                        f"for range {current_block}-{chunk_end}"
                    )
                
                # Perform the update using ALTER TABLE UPDATE
                update_query = f"""
                ALTER TABLE {self.database}.{dataset}
                UPDATE block_timestamp = (
                    SELECT toDateTime(timestamp)
                    FROM {self.database}.blocks b
                    WHERE b.block_number = {dataset}.block_number
                    LIMIT 1
                )
                WHERE block_number >= {current_block}
                  AND block_number < {chunk_end}
                  AND block_timestamp = '1970-01-01 00:00:00'
                  AND EXISTS (
                    SELECT 1 
                    FROM {self.database}.blocks b 
                    WHERE b.block_number = {dataset}.block_number
                      AND b.timestamp IS NOT NULL
                      AND b.timestamp > 0
                  )
                """
                
                client.command(update_query)
                
                # Wait for mutation to complete
                time.sleep(0.5)
                
                # Count how many were actually fixed
                count_query = f"""
                SELECT COUNT(*) 
                FROM {self.database}.{dataset}
                WHERE block_number >= {current_block}
                  AND block_number < {chunk_end}
                  AND block_timestamp != '1970-01-01 00:00:00'
                """
                
                result = client.query(count_query)
                chunk_fixed = result.result_rows[0][0] if result.result_rows else 0
                fixed_count += chunk_fixed
                
                logger.debug(f"Fixed {chunk_fixed} timestamps in chunk {current_block}-{chunk_end}")
                
                current_block = chunk_end
                
            return fixed_count
            
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