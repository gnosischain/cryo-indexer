"""
Database-based state management for the indexer.
Includes fixed-range gap detection for efficient processing.
"""
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from loguru import logger
import uuid
from ..config import settings


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
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    rows_indexed: Optional[int] = None
    error_message: Optional[str] = None
    batch_id: str = ""


class StateManager:
    """Manages indexing state in ClickHouse instead of files."""
    
    def __init__(self, clickhouse_manager):
        self.db = clickhouse_manager
        self.database = clickhouse_manager.database
        
        # Get range size from settings
        self.range_size = getattr(settings, 'indexing_range_size', 1000)
        
    def claim_range(self, mode: str, dataset: str, start_block: int, end_block: int, 
                   worker_id: str, batch_id: str = "", force: bool = False) -> bool:
        """
        Atomically claim a block range for processing.
        Returns True if successfully claimed, False if already claimed.
        """
        try:
            # If force mode, don't check existing data
            if not force:
                # Check if range is already being processed or completed
                client = self.db._connect()
                check_query = f"""
                SELECT COUNT(*) 
                FROM {self.database}.indexing_state
                WHERE mode = '{mode}'
                  AND dataset = '{dataset}'
                  AND start_block = {start_block}
                  AND end_block = {end_block}
                  AND status IN ('processing', 'completed')
                """
                result = client.query(check_query)
                
                if result.result_rows[0][0] > 0:
                    return False
            
            # Claim the range
            client = self.db._connect()
            insert_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, worker_id, started_at, batch_id)
            VALUES
            ('{mode}', '{dataset}', {start_block}, {end_block}, 
             'processing', '{worker_id}', now(), '{batch_id}')
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
            
            # Update sync position for continuous mode
            if mode == "continuous":
                self.update_sync_position(mode, dataset, end_block)
                
        except Exception as e:
            logger.error(f"Error completing range: {e}")
    
    def fail_range(self, mode: str, dataset: str, start_block: int, 
                   end_block: int, error_message: str) -> None:
        """Mark a range as failed."""
        try:
            client = self.db._connect()
            
            # Escape error message for SQL
            safe_error = error_message.replace("'", "''")[:500]  # Limit length
            
            # Get current attempt count first
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
            
            update_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, error_message, attempt_count)
            VALUES
            ('{mode}', '{dataset}', {start_block}, {end_block}, 
             'failed', '{safe_error}', {next_attempt})
            """
            client.command(update_query)
            
        except Exception as e:
            logger.error(f"Error marking range as failed: {e}")
    
    def get_last_synced_block(self, mode: str, datasets: List[str]) -> int:
        """
        Get the last successfully synced block for a mode.
        Returns the minimum across all datasets to ensure completeness.
        """
        try:
            client = self.db._connect()
            
            # For continuous mode, check sync position
            if mode == "continuous":
                datasets_str = "','".join(datasets)
                query = f"""
                SELECT MIN(last_synced_block) as min_block
                FROM {self.database}.sync_position
                WHERE mode = '{mode}'
                  AND dataset IN ('{datasets_str}')
                """
                result = client.query(query)
                if result.result_rows and result.result_rows[0][0] is not None:
                    return result.result_rows[0][0]
            
            # Otherwise, check completed ranges
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
    
    def update_sync_position(self, mode: str, dataset: str, block_number: int) -> None:
        """Update the sync position for continuous indexing."""
        try:
            client = self.db._connect()
            query = f"""
            INSERT INTO {self.database}.sync_position
            (mode, dataset, last_synced_block)
            VALUES ('{mode}', '{dataset}', {block_number})
            """
            client.command(query)
        except Exception as e:
            logger.error(f"Error updating sync position: {e}")
    
    def find_gaps(self, mode: str, dataset: str, start_block: int, 
                  end_block: int, min_gap_size: int = 1) -> List[Tuple[int, int]]:
        """
        Find gaps in indexed data for fixed-range approach.
        For blocks dataset: returns incomplete ranges
        For other datasets: returns missing ranges
        """
        gaps = []
        
        try:
            client = self.db._connect()
            
            # Align to range boundaries
            aligned_start = (start_block // self.range_size) * self.range_size
            aligned_end = ((end_block // self.range_size) + 1) * self.range_size
            
            # For blocks dataset, find incomplete ranges
            if dataset == 'blocks':
                incomplete_query = f"""
                SELECT start_block, end_block
                FROM {self.database}.indexing_state
                WHERE mode = '{mode}'
                  AND dataset = '{dataset}'
                  AND status = 'incomplete'
                  AND start_block >= {aligned_start}
                  AND end_block <= {aligned_end}
                ORDER BY start_block
                """
                result = client.query(incomplete_query)
                
                for row in result.result_rows:
                    gaps.append((row[0], row[1]))
                    logger.debug(f"Found incomplete range for {dataset}: {row[0]}-{row[1]}")
            
            # Find completely missing ranges for all datasets
            current = aligned_start
            while current < aligned_end:
                range_end = min(current + self.range_size, aligned_end)
                
                # Check if this range exists
                check_query = f"""
                SELECT COUNT(*)
                FROM {self.database}.indexing_state
                WHERE mode = '{mode}'
                  AND dataset = '{dataset}'
                  AND start_block = {current}
                  AND end_block = {range_end}
                """
                result = client.query(check_query)
                
                if result.result_rows[0][0] == 0:
                    # Range doesn't exist at all
                    gaps.append((current, range_end))
                    logger.debug(f"Found missing range for {dataset}: {current}-{range_end}")
                
                current = range_end
            
            logger.info(f"Found {len(gaps)} gaps for {dataset} in range {start_block}-{end_block}")
            return gaps
            
        except Exception as e:
            logger.error(f"Error finding gaps: {e}")
            return []
    
    def should_process_range(self, mode: str, dataset: str, 
                           start_block: int, end_block: int) -> bool:
        """
        Check if a range should be processed.
        For fixed ranges: only process if status is 'incomplete' or doesn't exist
        """
        try:
            client = self.db._connect()
            
            # Check if range exists and its status
            query = f"""
            SELECT status
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
                # Range doesn't exist, should process
                return True
                
            status = result.result_rows[0][0]
            
            # Only process if incomplete (for blocks) or doesn't exist
            return status == 'incomplete'
            
        except Exception as e:
            logger.error(f"Error checking if range should be processed: {e}")
            return True  # Process on error
    
    def get_stale_jobs(self, timeout_minutes: int = 30) -> List[IndexingRange]:
        """Find jobs that have been processing for too long."""
        try:
            client = self.db._connect()
            query = f"""
            SELECT mode, dataset, start_block, end_block, worker_id, started_at, attempt_count
            FROM {self.database}.indexing_state
            WHERE status = 'processing'
              AND started_at < now() - INTERVAL {timeout_minutes} MINUTE
            LIMIT 1000
            """
            result = client.query(query)
            
            stale_jobs = []
            for row in result.result_rows:
                stale_jobs.append(IndexingRange(
                    mode=row[0],
                    dataset=row[1],
                    start_block=row[2],
                    end_block=row[3],
                    worker_id=row[4],
                    started_at=row[5],
                    attempt_count=row[6] if len(row) > 6 else 0,
                    status='processing'
                ))
            
            return stale_jobs
            
        except Exception as e:
            logger.error(f"Error finding stale jobs: {e}")
            return []
    
    def reset_stale_jobs(self, timeout_minutes: int = 30) -> int:
        """Reset stale jobs back to pending status."""
        try:
            client = self.db._connect()
            
            # First get the stale jobs
            stale_jobs = self.get_stale_jobs(timeout_minutes)
            
            # Reset each one
            reset_count = 0
            for job in stale_jobs:
                next_attempt = job.attempt_count + 1
                
                # For fixed ranges, keep the status as incomplete if it was processing
                status = 'incomplete' if job.dataset == 'blocks' else 'pending'
                
                reset_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, attempt_count)
                VALUES
                ('{job.mode}', '{job.dataset}', {job.start_block}, {job.end_block}, 
                 '{status}', {next_attempt})
                """
                client.command(reset_query)
                reset_count += 1
                
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stale jobs")
                
            return reset_count
            
        except Exception as e:
            logger.error(f"Error resetting stale jobs: {e}")
            return 0
    
    def get_progress_summary(self, mode: str) -> Dict[str, Dict]:
        """Get indexing progress summary for fixed ranges."""
        try:
            client = self.db._connect()
            
            # Modified query to handle fixed ranges
            query = f"""
            SELECT 
                dataset,
                COUNT(*) as total_ranges,
                countIf(status = 'completed') as completed_ranges,
                countIf(status = 'incomplete') as incomplete_ranges,
                countIf(status = 'processing') as processing_ranges,
                countIf(status = 'failed') as failed_ranges,
                countIf(status = 'pending') as pending_ranges,
                MAX(end_block) as highest_block,
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
                    'incomplete_ranges': row[3],
                    'processing_ranges': row[4],
                    'failed_ranges': row[5],
                    'pending_ranges': row[6],
                    'highest_block': row[7],
                    'total_rows_indexed': row[8] or 0,
                    'range_size': self.range_size
                }
                
            return summary
            
        except Exception as e:
            logger.error(f"Error getting progress summary: {e}")
            return {}