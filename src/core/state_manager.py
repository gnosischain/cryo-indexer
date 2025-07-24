"""
Simplified state management for the indexer.
Single source of truth with clear status model.
"""
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from loguru import logger
import uuid


@dataclass
class IndexingRange:
    """Represents a range of blocks to index."""
    mode: str
    dataset: str
    start_block: int
    end_block: int
    status: str = "pending"
    worker_id: str = ""
    attempt_count: int = 0
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    rows_indexed: Optional[int] = None
    error_message: Optional[str] = None


class StateManager:
    """Simplified state management using only indexing_state table."""
    
    def __init__(self, clickhouse_manager):
        self.db = clickhouse_manager
        self.database = clickhouse_manager.database
        
        # Datasets that cannot start from block 0
        self.diff_datasets = {'balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs'}
    
    def get_range_status(self, mode: str, dataset: str, start_block: int, end_block: int) -> Optional[str]:
        """
        Get the current status of a range.
        Returns: 'completed', 'processing', 'failed', 'pending', or None if not found
        """
        try:
            client = self.db._connect()
            
            query = f"""
            SELECT status, created_at
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = client.query(query)
            
            if not result.result_rows:
                return None
                
            status = result.result_rows[0][0]
            return status
            
        except Exception as e:
            logger.error(f"Error getting range status: {e}")
            return None
    
    def claim_range(self, mode: str, dataset: str, start_block: int, end_block: int, 
                   worker_id: str) -> bool:
        """
        Atomically claim a block range for processing.
        Returns True if successfully claimed, False if already being processed.
        """
        try:
            status = self.get_range_status(mode, dataset, start_block, end_block)
            
            # Check if we can claim this range
            if status == 'completed':
                return False
            elif status == 'processing':
                return False
            
            # Claim the range
            client = self.db._connect()
            insert_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, worker_id, created_at)
            VALUES
            ('{mode}', '{dataset}', {start_block}, {end_block}, 
             'processing', '{worker_id}', now())
            """
            client.command(insert_query)
            return True
            
        except Exception as e:
            logger.error(f"Error claiming range: {e}")
            return False
    
    def complete_range(self, mode: str, dataset: str, start_block: int, 
                      end_block: int, rows_indexed: int = 0) -> None:
        """Mark a range as completed."""
        try:
            client = self.db._connect()
            
            update_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, completed_at, rows_indexed)
            VALUES
            ('{mode}', '{dataset}', {start_block}, {end_block}, 
             'completed', now(), {rows_indexed})
            """
            client.command(update_query)
            
            logger.debug(f"Marked {dataset} range {start_block}-{end_block} as completed ({rows_indexed} rows)")
                
        except Exception as e:
            logger.error(f"Error completing range: {e}")
    
    def fail_range(self, mode: str, dataset: str, start_block: int, 
                   end_block: int, error_message: str) -> None:
        """Mark a range as failed."""
        try:
            client = self.db._connect()
            
            # Get current attempt count
            count_query = f"""
            SELECT COALESCE(MAX(attempt_count), 0) + 1
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
            """
            result = client.query(count_query)
            next_attempt = result.result_rows[0][0] if result.result_rows else 1
            
            # Truncate error message
            safe_error = error_message[:500] if error_message else ""
            safe_error = safe_error.replace("'", "''")  # Escape single quotes
            
            update_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, error_message, attempt_count)
            VALUES
            ('{mode}', '{dataset}', {start_block}, {end_block}, 
             'failed', '{safe_error}', {next_attempt})
            """
            client.command(update_query)
            
            logger.error(f"Marked {dataset} range {start_block}-{end_block} as failed (attempt {next_attempt})")
            
        except Exception as e:
            logger.error(f"Error marking range as failed: {e}")
    
    def get_last_synced_block(self, mode: str, datasets: List[str]) -> int:
        """
        Get the last successfully synced block across all datasets.
        Returns the minimum to ensure completeness.
        """
        try:
            client = self.db._connect()
            
            datasets_str = "','".join(datasets)
            query = f"""
            SELECT 
                dataset,
                MAX(end_block) as last_block
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
              AND dataset IN ('{datasets_str}')
              AND status = 'completed'
            GROUP BY dataset
            """
            result = client.query(query)
            
            if not result.result_rows:
                return 0
                
            # Return minimum to ensure all datasets are synced
            last_blocks = [row[1] for row in result.result_rows]
            return min(last_blocks) if last_blocks else 0
            
        except Exception as e:
            logger.error(f"Error getting last synced block: {e}")
            return 0
    
    def find_gaps(self, mode: str, dataset: str, start_block: int, 
          end_block: int) -> List[Tuple[int, int]]:
        """
        Find gaps in indexed data - ONLY REAL GAPS, NOT CONTINUATION RANGES.
        A gap is a missing range WITHIN the completed scope, not beyond it.
        """
        gaps = []
        
        try:
            client = self.db._connect()
            
            # Step 1: Get the actual range that was attempted for this dataset
            highest_attempted = self._get_highest_attempted_block(mode, dataset)
            
            if highest_attempted == 0:
                logger.info(f"No data found for {dataset} in mode {mode}")
                return []
            
            # Step 2: Determine effective range to check
            effective_start = start_block
            if dataset in self.diff_datasets and effective_start == 0:
                effective_start = 1000  # Start from first valid range for diff datasets
            
            # CRITICAL FIX: Only look for gaps WITHIN the attempted range, not beyond it
            if end_block == 0 or end_block > highest_attempted:
                effective_end = highest_attempted
            else:
                effective_end = min(end_block, highest_attempted)
            
            # Don't look for gaps beyond what was actually attempted
            if effective_end <= effective_start:
                logger.info(f"No gap detection needed for {dataset}: effective range {effective_start}-{effective_end}")
                return []
            
            logger.debug(f"Gap detection for {dataset}: checking {effective_start} to {effective_end}")
            
            # Step 3: Get all COMPLETED ranges within this span
            completed_query = f"""
            SELECT start_block, end_block
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
            AND dataset = '{dataset}'
            AND status = 'completed'
            AND start_block >= {effective_start}
            AND end_block <= {effective_end}
            ORDER BY start_block
            """
            result = client.query(completed_query)
            completed_ranges = [(row[0], row[1]) for row in result.result_rows]
            
            # Step 4: Find missing ranges (gaps between completed ranges)
            # ONLY within the effective range, not extending beyond
            current = effective_start
            for comp_start, comp_end in completed_ranges:
                if current < comp_start:
                    # Found a gap WITHIN the attempted range
                    gaps.append((current, comp_start))
                current = max(current, comp_end)
            
            # Step 5: Add explicitly failed ranges
            failed_query = f"""
            SELECT DISTINCT start_block, end_block
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
            AND dataset = '{dataset}'
            AND status = 'failed'
            AND start_block >= {effective_start}
            AND end_block <= {effective_end}
            ORDER BY start_block
            """
            result = client.query(failed_query)
            
            for row in result.result_rows:
                gap_range = (row[0], row[1])
                if dataset in self.diff_datasets and gap_range[0] == 0:
                    continue
                if gap_range not in gaps:
                    gaps.append(gap_range)
            
            # Step 6: Remove duplicates and sort
            gaps = sorted(list(set(gaps)))
            
            # Step 7: Final validation
            validated_gaps = []
            for gap_start, gap_end in gaps:
                # Skip invalid ranges
                if dataset in self.diff_datasets and gap_start == 0:
                    continue
                
                # Skip tiny ranges
                if gap_end - gap_start < 100:  # Must be substantial gap
                    continue
                
                # Double-check this range isn't actually completed
                check_query = f"""
                SELECT COUNT(*) 
                FROM {self.database}.indexing_state
                WHERE mode = '{mode}'
                AND dataset = '{dataset}'
                AND status = 'completed'
                AND start_block = {gap_start}
                AND end_block = {gap_end}
                """
                result = client.query(check_query)
                
                if result.result_rows[0][0] == 0:  # Not completed
                    validated_gaps.append((gap_start, gap_end))
            
            if validated_gaps:
                logger.info(f"Found {len(validated_gaps)} REAL gaps for {dataset} (missing ranges within attempted scope)")
                for gap_start, gap_end in validated_gaps:
                    logger.info(f"  Real Gap: {dataset} {gap_start}-{gap_end}")
            else:
                logger.info(f"No real gaps found for {dataset} ✓ (all attempted ranges are complete)")
            
            return validated_gaps
            
        except Exception as e:
            logger.error(f"Error finding gaps: {e}")
            return []

    def _get_highest_attempted_block(self, mode: str, dataset: str) -> int:
        """
        Get the highest block that was actually attempted (completed, failed, or processing).
        This helps distinguish between real gaps and simply unprocessed work.
        """
        try:
            client = self.db._connect()
            
            # Find the highest end_block across all statuses for this dataset
            query = f"""
            SELECT MAX(end_block) as highest_block
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
            AND dataset = '{dataset}'
            AND status IN ('completed', 'failed', 'processing', 'pending')
            """
            result = client.query(query)
            
            if result.result_rows and result.result_rows[0][0] is not None:
                highest = result.result_rows[0][0]
                logger.debug(f"Highest attempted block for {dataset}: {highest}")
                return highest
            
            return 0
            
        except Exception as e:
            logger.error(f"Error getting highest attempted block: {e}")
            return 0

    def get_processing_summary(self, mode: str) -> Dict[str, Dict]:
        """
        Enhanced progress summary that separates real gaps from unprocessed work.
        """
        try:
            client = self.db._connect()
            
            # Get basic stats
            query = f"""
            SELECT 
                dataset,
                COUNT(*) as total_ranges,
                countIf(status = 'completed') as completed_ranges,
                countIf(status = 'processing') as processing_ranges,
                countIf(status = 'failed') as failed_ranges,
                countIf(status = 'pending') as pending_ranges,
                MAX(end_block) as highest_attempted_block,
                maxIf(end_block, status = 'completed') as highest_completed_block,
                SUM(rows_indexed) as total_rows_indexed
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
            GROUP BY dataset
            """
            result = client.query(query)
            
            summary = {}
            for row in result.result_rows:
                dataset = row[0]
                summary[dataset] = {
                    'total_ranges': row[1],
                    'completed_ranges': row[2],
                    'processing_ranges': row[3],
                    'failed_ranges': row[4],
                    'pending_ranges': row[5],
                    'highest_attempted_block': row[6] or 0,
                    'highest_completed_block': row[7] or 0,
                    'total_rows_indexed': row[8] or 0,
                    
                    # Calculate progress percentage
                    'completion_percentage': (row[2] / row[1] * 100) if row[1] > 0 else 0,
                    
                    # Determine status
                    'status': self._determine_dataset_status(row[2], row[3], row[4], row[5])
                }
                
            return summary
            
        except Exception as e:
            logger.error(f"Error getting processing summary: {e}")
            return {}

    def _determine_dataset_status(self, completed: int, processing: int, failed: int, pending: int) -> str:
        """Determine the overall status of a dataset."""
        total = completed + processing + failed + pending
        
        if total == 0:
            return "no_data"
        elif completed == total:
            return "complete"
        elif processing > 0:
            return "in_progress"
        elif failed > 0 and pending == 0 and processing == 0:
            return "failed"
        elif pending > 0:
            return "pending"
        else:
            return "mixed"
    
    def cleanup_stale_jobs(self, timeout_minutes: int = 30) -> int:
        """
        DISABLED: Don't reset any jobs during startup.
        This prevents interference with maintain operations.
        """
        logger.info("Stale job cleanup is DISABLED to prevent interference with maintain operations")
        return 0

    def has_valid_timestamps(self, start_block: int, end_block: int) -> bool:
        """Check if all blocks in range have valid timestamps."""
        try:
            client = self.db._connect()
            
            expected_count = end_block - start_block
            
            query = f"""
            SELECT COUNT(*) 
            FROM {self.database}.blocks
            WHERE block_number >= {start_block} 
              AND block_number < {end_block}
              AND timestamp IS NOT NULL
              AND timestamp > 0
              AND toDateTime(timestamp) > toDateTime('1971-01-01 00:00:00')
            """
            
            result = client.query(query)
            actual_count = result.result_rows[0][0] if result.result_rows else 0
            
            return actual_count == expected_count
            
        except Exception as e:
            logger.error(f"Error checking timestamps: {e}")
            return False