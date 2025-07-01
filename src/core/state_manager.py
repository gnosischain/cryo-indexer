"""
Database-based state management for the indexer.
Simplified version that trusts indexing_state table completely.
"""
from typing import List, Dict, Optional, Tuple, Any
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
    """Manages indexing state in ClickHouse - simplified to trust state completely."""
    
    def __init__(self, clickhouse_manager):
        self.db = clickhouse_manager
        self.database = clickhouse_manager.database
        
        # Get range size from settings
        self.range_size = getattr(settings, 'indexing_range_size', 1000)
    
    def _prepare_value(self, value: Any, column_type: str = 'string') -> str:
        """
        Prepare a value for SQL insertion with consistent NULL handling.
        
        Args:
            value: The value to prepare
            column_type: Type of the column ('string', 'number', 'datetime')
            
        Returns:
            Properly formatted SQL value
        """
        if value is None or (isinstance(value, str) and value.strip() == ''):
            return 'NULL'
        elif column_type == 'string':
            # Escape single quotes and return quoted string
            safe_value = str(value).replace("'", "''")
            return f"'{safe_value}'"
        elif column_type == 'number':
            return str(value)
        elif column_type == 'datetime':
            return f"'{value}'" if value else 'NULL'
        else:
            return f"'{value}'"
    
    def get_range_status(self, mode: str, dataset: str, start_block: int, end_block: int) -> Optional[str]:
        """
        Get the current status of a range from indexing_state.
        Returns: 'completed', 'processing', 'failed', 'pending', or None if not found
        
        Also checks if 'processing' ranges are stale and should be considered available.
        """
        try:
            client = self.db._connect()
            
            # Get the latest status with timing info
            query = f"""
            SELECT status, started_at, worker_id
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
              AND dataset = {self._prepare_value(dataset)}
              AND start_block = {start_block}
              AND end_block = {end_block}
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = client.query(query)
            
            if not result.result_rows:
                return None
                
            status = result.result_rows[0][0]
            started_at = result.result_rows[0][1]
            worker_id = result.result_rows[0][2] if len(result.result_rows[0]) > 2 else None
            
            # If status is 'processing', check if it's stale
            if status == 'processing' and started_at:
                stale_timeout = getattr(settings, 'stale_job_timeout_minutes', 30)
                stale_check_query = f"""
                SELECT 
                    CASE 
                        WHEN started_at < now() - INTERVAL {stale_timeout} MINUTE 
                        THEN 1 
                        ELSE 0 
                    END as is_stale
                FROM {self.database}.indexing_state
                WHERE mode = {self._prepare_value(mode)}
                  AND dataset = {self._prepare_value(dataset)}
                  AND start_block = {start_block}
                  AND end_block = {end_block}
                  AND status = 'processing'
                  AND started_at = '{started_at}'
                """
                stale_result = client.query(stale_check_query)
                
                if stale_result.result_rows and stale_result.result_rows[0][0] == 1:
                    logger.warning(
                        f"Range {dataset} {start_block}-{end_block} has stale 'processing' status "
                        f"(worker: {worker_id}, started: {started_at})"
                    )
                    # Consider it as available for claiming
                    return 'stale'
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting range status: {e}")
            return None
    
    def claim_range(self, mode: str, dataset: str, start_block: int, end_block: int, 
                   worker_id: str, batch_id: str = "", force: bool = False) -> bool:
        """
        Atomically claim a block range for processing.
        Returns True if successfully claimed, False if already being processed.
        """
        try:
            # Get current status
            status = self.get_range_status(mode, dataset, start_block, end_block)
            
            # If force mode, always claim
            if force:
                logger.debug(f"Force claiming range {dataset} {start_block}-{end_block}")
            else:
                # Check if we can claim this range
                if status == 'completed':
                    logger.debug(f"Range {dataset} {start_block}-{end_block} already completed")
                    return False
                elif status == 'processing':
                    logger.debug(f"Range {dataset} {start_block}-{end_block} already being processed")
                    return False
            
            # Claim the range
            client = self.db._connect()
            insert_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, worker_id, started_at, batch_id)
            VALUES
            ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, {start_block}, {end_block}, 
             'processing', {self._prepare_value(worker_id)}, now(), {self._prepare_value(batch_id)})
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
            
            # First, get the started_at time from the existing processing entry
            get_started_query = f"""
            SELECT started_at 
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
            AND dataset = {self._prepare_value(dataset)}
            AND start_block = {start_block}
            AND end_block = {end_block}
            AND status = 'processing'
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = client.query(get_started_query)
            
            started_at = None
            if result.result_rows and result.result_rows[0][0]:
                started_at = result.result_rows[0][0]
            
            # Now insert the completed entry with the preserved started_at
            if started_at:
                update_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, started_at, completed_at, rows_indexed)
                VALUES
                ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, {start_block}, {end_block}, 
                'completed', '{started_at}', now(), {rows_indexed})
                """
            else:
                # Fallback if we couldn't find the started_at (shouldn't happen normally)
                update_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, completed_at, rows_indexed)
                VALUES
                ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, {start_block}, {end_block}, 
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
            
            # Get current attempt count first
            count_query = f"""
            SELECT COALESCE(MAX(attempt_count), 0) + 1
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
              AND dataset = {self._prepare_value(dataset)}
              AND start_block = {start_block}
              AND end_block = {end_block}
            """
            result = client.query(count_query)
            next_attempt = result.result_rows[0][0] if result.result_rows else 1
            
            # Prepare error message - truncate to 500 chars
            safe_error = error_message[:500] if error_message else None
            
            update_query = f"""
            INSERT INTO {self.database}.indexing_state
            (mode, dataset, start_block, end_block, status, error_message, attempt_count)
            VALUES
            ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, {start_block}, {end_block}, 
             'failed', {self._prepare_value(safe_error)}, {next_attempt})
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
                WHERE mode = {self._prepare_value(mode)}
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
            WHERE mode = {self._prepare_value(mode)}
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
            VALUES ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, {block_number})
            """
            client.command(query)
        except Exception as e:
            logger.error(f"Error updating sync position: {e}")
    
    def find_gaps(self, mode: str, dataset: str, start_block: int, 
                  end_block: int, min_gap_size: int = 1) -> List[Tuple[int, int]]:
        """
        Find gaps in indexed data based on indexing_state.
        A gap is any range that isn't marked as 'completed'.
        """
        gaps = []
        
        try:
            client = self.db._connect()
            
            # Align to range boundaries
            aligned_start = (start_block // self.range_size) * self.range_size
            aligned_end = ((end_block // self.range_size) + 1) * self.range_size
            
            # Find all completed ranges
            query = f"""
            SELECT start_block, end_block
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
              AND dataset = {self._prepare_value(dataset)}
              AND status = 'completed'
              AND start_block >= {aligned_start}
              AND end_block <= {aligned_end}
            ORDER BY start_block
            """
            result = client.query(query)
            
            completed_ranges = [(row[0], row[1]) for row in result.result_rows]
            
            # Find gaps
            current = aligned_start
            for comp_start, comp_end in completed_ranges:
                if current < comp_start:
                    gaps.append((current, comp_start))
                current = max(current, comp_end)
            
            # Check for gap at the end
            if current < aligned_end:
                gaps.append((current, aligned_end))
            
            # Also find non-completed ranges (failed, pending, etc)
            non_completed_query = f"""
            SELECT start_block, end_block
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
              AND dataset = {self._prepare_value(dataset)}
              AND status != 'completed'
              AND status != 'processing'
              AND start_block >= {aligned_start}
              AND end_block <= {aligned_end}
            ORDER BY start_block
            """
            result = client.query(non_completed_query)
            
            for row in result.result_rows:
                gaps.append((row[0], row[1]))
            
            # Remove duplicates and sort
            gaps = sorted(list(set(gaps)))
            
            logger.info(f"Found {len(gaps)} gaps for {dataset} in range {start_block}-{end_block}")
            return gaps
            
        except Exception as e:
            logger.error(f"Error finding gaps: {e}")
            return []
    
    def get_failed_ranges(self, mode: str, dataset: str, start_block: int, 
                         end_block: int, max_attempts: Optional[int] = None) -> List[Tuple[int, int, int, str]]:
        """
        Get failed ranges within the specified block range.
        Returns list of tuples: (start_block, end_block, attempt_count, error_message)
        """
        try:
            client = self.db._connect()
            
            # Build query
            query = f"""
            SELECT start_block, end_block, attempt_count, error_message
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
              AND dataset = {self._prepare_value(dataset)}
              AND status = 'failed'
              AND start_block >= {start_block}
              AND end_block <= {end_block}
            """
            
            # Add max attempts filter if specified
            if max_attempts is not None:
                query += f" AND attempt_count < {max_attempts}"
            
            query += " ORDER BY start_block"
            
            result = client.query(query)
            
            failed_ranges = []
            for row in result.result_rows:
                failed_ranges.append((row[0], row[1], row[2], row[3] or ""))
                logger.debug(f"Found failed range for {dataset}: {row[0]}-{row[1]} (attempts: {row[2]})")
            
            logger.info(f"Found {len(failed_ranges)} failed ranges for {dataset}")
            return failed_ranges
            
        except Exception as e:
            logger.error(f"Error getting failed ranges: {e}")
            return []
    
    def delete_range_entries(self, mode: str, dataset: str, ranges: List[Tuple[int, int]]) -> int:
        """
        Delete indexing_state entries for specified ranges.
        Returns number of ranges deleted.
        """
        try:
            client = self.db._connect()
            deleted_count = 0
            
            for start_block, end_block in ranges:
                delete_query = f"""
                ALTER TABLE {self.database}.indexing_state
                DELETE WHERE mode = {self._prepare_value(mode)}
                  AND dataset = {self._prepare_value(dataset)}
                  AND start_block = {start_block}
                  AND end_block = {end_block}
                """
                client.command(delete_query)
                deleted_count += 1
                logger.debug(f"Deleted entry for {dataset} range {start_block}-{end_block}")
            
            logger.info(f"Deleted {deleted_count} range entries for {dataset}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting range entries: {e}")
            return 0
    
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
                
                reset_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, attempt_count)
                VALUES
                ({self._prepare_value(job.mode)}, {self._prepare_value(job.dataset)}, 
                 {job.start_block}, {job.end_block}, 'pending', {next_attempt})
                """
                client.command(reset_query)
                reset_count += 1
                
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stale jobs")
                
            return reset_count
            
        except Exception as e:
            logger.error(f"Error resetting stale jobs: {e}")
            return 0
    
    def reset_all_processing_jobs(self) -> int:
        """
        Reset ALL 'processing' jobs to 'pending' status.
        Called on startup to clean up from previous crashes.
        """
        try:
            client = self.db._connect()
            
            # Get all processing jobs
            query = f"""
            SELECT mode, dataset, start_block, end_block, worker_id, attempt_count
            FROM {self.database}.indexing_state
            WHERE status = 'processing'
            """
            result = client.query(query)
            
            reset_count = 0
            for row in result.result_rows:
                mode, dataset, start_block, end_block, worker_id, attempt_count = row[:6]
                next_attempt = (attempt_count or 0) + 1
                
                # Insert a new 'pending' entry
                reset_query = f"""
                INSERT INTO {self.database}.indexing_state
                (mode, dataset, start_block, end_block, status, attempt_count, error_message)
                VALUES
                ({self._prepare_value(mode)}, {self._prepare_value(dataset)}, 
                 {start_block}, {end_block}, 'pending', {next_attempt}, 
                 {self._prepare_value('Reset from processing on startup')})
                """
                client.command(reset_query)
                reset_count += 1
                
                logger.info(
                    f"Reset {dataset} {start_block}-{end_block} from 'processing' to 'pending' "
                    f"(was worker: {worker_id})"
                )
            
            if reset_count > 0:
                logger.info(f"Reset {reset_count} processing jobs on startup")
            else:
                logger.info("No processing jobs found to reset")
                
            return reset_count
            
        except Exception as e:
            logger.error(f"Error resetting processing jobs: {e}")
            return 0
    
    def get_progress_summary(self, mode: str) -> Dict[str, Dict]:
        """Get indexing progress summary."""
        try:
            client = self.db._connect()
            
            query = f"""
            SELECT 
                dataset,
                COUNT(*) as total_ranges,
                countIf(status = 'completed') as completed_ranges,
                countIf(status = 'processing') as processing_ranges,
                countIf(status = 'failed') as failed_ranges,
                countIf(status = 'pending') as pending_ranges,
                MAX(end_block) as highest_block,
                SUM(rows_indexed) as total_rows_indexed
            FROM {self.database}.indexing_state
            WHERE mode = {self._prepare_value(mode)}
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
                    'highest_block': row[6],
                    'total_rows_indexed': row[7] or 0,
                    'range_size': self.range_size
                }
                
            return summary
            
        except Exception as e:
            logger.error(f"Error getting progress summary: {e}")
            return {}
        
    def get_max_completed_block(self, mode: str, dataset: str) -> int:
        """Get the maximum completed block for a dataset."""
        try:
            client = self.db._connect()
            result = client.query(f"""
                SELECT MAX(end_block) 
                FROM {self.database}.indexing_state
                WHERE mode = {self._prepare_value(mode)}
                AND dataset = {self._prepare_value(dataset)}
                AND status = 'completed'
            """)
            return result.result_rows[0][0] if result.result_rows and result.result_rows[0][0] else 0
        except Exception as e:
            logger.error(f"Error getting max completed block: {e}")
            return 0