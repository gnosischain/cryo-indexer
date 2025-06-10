"""
Database-based state management for the indexer.
Replaces file-based state with ClickHouse tables.
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
        Find gaps in indexed data for a specific dataset.
        Uses hybrid approach: indexing_state first, fallback to table data.
        """
        # First try the indexing_state approach
        gaps_from_state = self._find_gaps_from_state(mode, dataset, start_block, end_block, min_gap_size)
        
        # If we find gaps covering most of the range, check if we have actual table data
        total_range = end_block - start_block
        gaps_total = sum(gap[1] - gap[0] for gap in gaps_from_state)
        
        # Use configurable threshold from settings
        if gaps_total >= total_range * settings.gap_detection_threshold:
            logger.info(f"Large gap percentage detected for {dataset}, checking actual table data...")
            gaps_from_table = self._find_gaps_from_table(dataset, start_block, end_block, min_gap_size)
            
            # If table-based method finds significantly fewer gaps, use that
            table_gaps_total = sum(gap[1] - gap[0] for gap in gaps_from_table)
            if table_gaps_total < gaps_total * 0.5:  # Table method finds <50% of state gaps
                logger.info(f"Using table-based gap detection for {dataset}")
                return gaps_from_table
        
        return gaps_from_state
    
    def _find_gaps_from_state(self, mode: str, dataset: str, start_block: int, 
                             end_block: int, min_gap_size: int = 1) -> List[Tuple[int, int]]:
        """Original method: Find gaps using indexing_state table."""
        gaps = []
        try:
            client = self.db._connect()
            
            # Use configurable chunk size from settings
            chunk_size = settings.gap_detection_state_chunk_size
            current = start_block
            
            while current < end_block:
                chunk_end = min(current + chunk_size, end_block)
                
                # Get completed ranges in this chunk
                query = f"""
                SELECT start_block, end_block
                FROM {self.database}.indexing_state
                WHERE mode = '{mode}'
                  AND dataset = '{dataset}'
                  AND status = 'completed'
                  AND start_block >= {current}
                  AND end_block <= {chunk_end}
                ORDER BY start_block
                LIMIT 10000
                """
                result = client.query(query)
                
                # Find gaps in this chunk
                chunk_ranges = [(row[0], row[1]) for row in result.result_rows]
                chunk_gaps = self._find_gaps_in_ranges(chunk_ranges, current, chunk_end, min_gap_size)
                gaps.extend(chunk_gaps)
                
                current = chunk_end
                
        except Exception as e:
            logger.error(f"Error finding gaps from state: {e}")
            
        return gaps
    
    def _find_gaps_from_table(self, dataset: str, start_block: int, 
                             end_block: int, min_gap_size: int = 1) -> List[Tuple[int, int]]:
        """Alternative method: Find gaps by checking actual table data."""
        gaps = []
        
        # Table mappings
        table_mappings = {
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
            # Aliases
            'events': 'logs',
            'txs': 'transactions',
        }
        
        table_name = table_mappings.get(dataset, dataset)
        
        try:
            client = self.db._connect()
            
            # Check if table exists
            table_check = client.query(f"""
            SELECT count() FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            """)
            
            if table_check.result_rows[0][0] == 0:
                logger.warning(f"Table {table_name} does not exist")
                return [(start_block, end_block)]  # Entire range is a gap
            
            # Use configurable chunk size from settings
            chunk_size = settings.gap_detection_table_chunk_size
            current = start_block
            
            while current < end_block:
                chunk_end = min(current + chunk_size, end_block)
                
                # Check which blocks exist in this chunk
                existing_blocks_query = f"""
                SELECT DISTINCT block_number
                FROM {self.database}.{table_name}
                WHERE block_number >= {current} 
                  AND block_number < {chunk_end}
                  AND block_number IS NOT NULL
                ORDER BY block_number
                """
                
                result = client.query(existing_blocks_query)
                existing_blocks = [row[0] for row in result.result_rows]
                
                # Find gaps in this chunk
                chunk_gaps = self._find_gaps_in_block_list(
                    existing_blocks, current, chunk_end, min_gap_size
                )
                gaps.extend(chunk_gaps)
                
                current = chunk_end
                
            logger.info(f"Found {len(gaps)} gaps in {dataset} by checking table data")
            return gaps
            
        except Exception as e:
            logger.error(f"Error finding gaps from table data: {e}")
            # Fallback to assuming everything is a gap
            return [(start_block, end_block)]
    
    def _find_gaps_in_block_list(self, existing_blocks: List[int], 
                                start: int, end: int, min_gap_size: int) -> List[Tuple[int, int]]:
        """Find gaps in a list of existing block numbers."""
        if not existing_blocks:
            return [(start, end)] if end - start >= min_gap_size else []
        
        gaps = []
        current = start
        
        for block_num in sorted(existing_blocks):
            if block_num > current and block_num - current >= min_gap_size:
                gaps.append((current, block_num))
            current = max(current, block_num + 1)
        
        if current < end and end - current >= min_gap_size:
            gaps.append((current, end))
        
        return gaps
    
    def _find_gaps_in_ranges(self, ranges: List[Tuple[int, int]], 
                            start: int, end: int, min_gap_size: int) -> List[Tuple[int, int]]:
        """Find gaps in a list of ranges."""
        if not ranges:
            return [(start, end)] if end - start >= min_gap_size else []
            
        gaps = []
        current = start
        
        for range_start, range_end in sorted(ranges):
            if range_start > current and range_start - current >= min_gap_size:
                gaps.append((current, range_start))
            current = max(current, range_end)
            
        if current < end and end - current >= min_gap_size:
            gaps.append((current, end))
            
        return gaps
    
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
                ('{job.mode}', '{job.dataset}', {job.start_block}, {job.end_block}, 
                 'pending', {next_attempt})
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
        """Get indexing progress summary."""
        try:
            client = self.db._connect()
            query = f"""
            SELECT *
            FROM {self.database}.indexing_progress
            WHERE mode = '{mode}'
            """
            result = client.query(query)
            
            summary = {}
            columns = ['mode', 'dataset', 'completed_ranges', 'processing_ranges', 
                      'failed_ranges', 'pending_ranges', 'highest_completed_block', 
                      'total_rows_indexed']
            
            for row in result.result_rows:
                dataset = row[1]
                summary[dataset] = {
                    columns[i]: row[i] for i in range(2, len(columns))
                }
                
            return summary
            
        except Exception as e:
            logger.error(f"Error getting progress summary: {e}")
            return {}
    
    def should_process_range(self, mode: str, dataset: str, 
                           start_block: int, end_block: int) -> bool:
        """
        Check if a range should be processed.
        Returns True if the range hasn't been completed yet.
        """
        try:
            client = self.db._connect()
            query = f"""
            SELECT COUNT(*)
            FROM {self.database}.indexing_state
            WHERE mode = '{mode}'
              AND dataset = '{dataset}'
              AND start_block = {start_block}
              AND end_block = {end_block}
              AND status = 'completed'
            """
            result = client.query(query)
            
            return result.result_rows[0][0] == 0
            
        except Exception as e:
            logger.error(f"Error checking if range should be processed: {e}")
            return True  # Process on error