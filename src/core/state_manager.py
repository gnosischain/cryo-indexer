"""
Simplified state management for the indexer.
Single source of truth with clear status model.
Mode-independent: tracks state by (dataset, start_block, end_block) only.
"""
from typing import List, Dict, Optional, Tuple, Any, NamedTuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from loguru import logger
import time
import uuid


class TimestampCheck(NamedTuple):
    """
    Outcome of validating the timestamps of a freshly written block range.

    reason:
      'ok'          - every block in the range is present with a usable timestamp
      'invalid'     - every block is visible, but some have a missing/garbage timestamp
                      (the data is genuinely wrong; do NOT index dependent datasets)
      'not_visible' - blocks are still missing from the read replica after retries
                      (read-after-write lag, or a real hole - we cannot tell)
      'error'       - the validation query itself failed
    """
    ok: bool
    reason: str
    expected: int
    valid: int
    present: int


@dataclass
class IndexingRange:
    """Represents a range of blocks to index."""
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

        # Flipped off the first time the server rejects select_sequential_consistency
        self._sequential_consistency_supported = True

    def get_range_status(self, dataset: str, start_block: int, end_block: int) -> Optional[str]:
        """
        Get the current status of a range.
        Returns: 'completed', 'processing', 'failed', 'pending', or None if not found
        """
        try:
            client = self.db._connect()

            query = f"""
            SELECT status, created_at
            FROM {self.database}.indexing_state
            WHERE dataset = '{dataset}'
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

    def claim_range(self, dataset: str, start_block: int, end_block: int,
                   worker_id: str) -> bool:
        """
        Atomically claim a block range for processing.
        Returns True if successfully claimed, False if already being processed.
        """
        try:
            status = self.get_range_status(dataset, start_block, end_block)

            # Check if we can claim this range
            if status == 'completed':
                return False
            elif status == 'processing':
                return False

            # Claim the range
            client = self.db._connect()
            insert_query = f"""
            INSERT INTO {self.database}.indexing_state
            (dataset, start_block, end_block, status, worker_id, created_at)
            VALUES
            ('{dataset}', {start_block}, {end_block},
             'processing', '{worker_id}', now())
            """
            client.command(insert_query)
            return True

        except Exception as e:
            logger.error(f"Error claiming range: {e}")
            return False

    def complete_range(self, dataset: str, start_block: int,
                      end_block: int, rows_indexed: int = 0) -> None:
        """Mark a range as completed."""
        try:
            client = self.db._connect()

            update_query = f"""
            INSERT INTO {self.database}.indexing_state
            (dataset, start_block, end_block, status, completed_at, rows_indexed)
            VALUES
            ('{dataset}', {start_block}, {end_block},
             'completed', now(), {rows_indexed})
            """
            client.command(update_query)

            logger.debug(f"Marked {dataset} range {start_block}-{end_block} as completed ({rows_indexed} rows)")

        except Exception as e:
            logger.error(f"Error completing range: {e}")

    def fail_range(self, dataset: str, start_block: int,
                   end_block: int, error_message: str) -> None:
        """Mark a range as failed."""
        try:
            client = self.db._connect()

            # Get current attempt count
            count_query = f"""
            SELECT COALESCE(MAX(attempt_count), 0) + 1
            FROM {self.database}.indexing_state
            WHERE dataset = '{dataset}'
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
            (dataset, start_block, end_block, status, error_message, attempt_count)
            VALUES
            ('{dataset}', {start_block}, {end_block},
             'failed', '{safe_error}', {next_attempt})
            """
            client.command(update_query)

            logger.error(f"Marked {dataset} range {start_block}-{end_block} as failed (attempt {next_attempt})")

        except Exception as e:
            logger.error(f"Error marking range as failed: {e}")

    def get_last_synced_block(self, datasets: List[str]) -> int:
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
            WHERE dataset IN ('{datasets_str}')
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

    def find_gaps(self, dataset: str, start_block: int,
          end_block: int) -> List[Tuple[int, int]]:
        """
        Find gaps in indexed data - ONLY REAL GAPS, NOT CONTINUATION RANGES.
        A gap is a missing range WITHIN the completed scope, not beyond it.
        """
        gaps = []

        try:
            client = self.db._connect()

            # Step 1: Get the actual range that was attempted for this dataset
            highest_attempted = self._get_highest_attempted_block(dataset)

            if highest_attempted == 0:
                logger.info(f"No data found for {dataset}")
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
            WHERE dataset = '{dataset}'
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

            # Step 4b: A gap can also sit between the LAST completed range and the end of
            # the window. The loop above only emits gaps *between* two completed ranges,
            # so without this a scoped/chunked maintain silently leaves its final batch
            # unindexed and still reports success.
            if current < effective_end:
                trailing_end = effective_end

                # Never hand back a range a live indexer is part-way through. Callers
                # (maintain and auto-maintain) DELETE a range's rows before re-extracting,
                # and auto-maintain is designed to run alongside the continuous indexer.
                # effective_end is derived from the highest *attempted* block, which is
                # normally the end of an in-flight 'processing' range at the chain tip,
                # so clamp the trailing gap below anything still processing.
                # NOTE: uses ORDER BY/LIMIT rather than MIN() because a ClickHouse MIN()
                # over an empty set yields 0 for UInt32, not NULL.
                in_flight = client.query(f"""
                SELECT start_block
                FROM {self.database}.indexing_state
                WHERE dataset = '{dataset}'
                AND status = 'processing'
                AND end_block > {current}
                AND start_block < {effective_end}
                ORDER BY start_block
                LIMIT 1
                """)
                if in_flight.result_rows:
                    trailing_end = min(trailing_end, in_flight.result_rows[0][0])

                if current < trailing_end:
                    gaps.append((current, trailing_end))

            # Step 5: Add explicitly failed ranges
            failed_query = f"""
            SELECT DISTINCT start_block, end_block
            FROM {self.database}.indexing_state
            WHERE dataset = '{dataset}'
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
                WHERE dataset = '{dataset}'
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

    def _get_highest_attempted_block(self, dataset: str) -> int:
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
            WHERE dataset = '{dataset}'
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

    def get_processing_summary(self) -> Dict[str, Dict]:
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

    def recover_stuck_ranges(self, timeout_hours: int = 2) -> int:
        """
        Find ranges stuck in 'processing' for more than timeout_hours
        and mark them as 'failed' so maintain/auto-maintain can pick them up.
        """
        try:
            client = self.db._connect()

            query = f"""
            SELECT dataset, start_block, end_block, worker_id, created_at
            FROM {self.database}.indexing_state FINAL
            WHERE status = 'processing'
              AND created_at < now() - INTERVAL {timeout_hours} HOUR
            """
            result = client.query(query)

            recovered = 0
            for row in result.result_rows:
                dataset, start, end, worker_id, created = row
                logger.warning(
                    f"Recovering stuck range: {dataset} {start}-{end} "
                    f"(worker: {worker_id}, stuck since: {created})"
                )

                safe_msg = f"Auto-recovered: stuck processing since {created}"
                insert = f"""
                INSERT INTO {self.database}.indexing_state
                (dataset, start_block, end_block, status, error_message, attempt_count)
                VALUES
                ('{dataset}', {start}, {end}, 'failed', '{safe_msg}', 1)
                """
                client.command(insert)
                recovered += 1

            if recovered > 0:
                logger.info(f"Recovered {recovered} stuck processing ranges")

            return recovered

        except Exception as e:
            logger.error(f"Error recovering stuck ranges: {e}")
            return 0

    # Datasets where 0 rows is always suspicious (every range should have data)
    ZERO_ROW_CHECK_DATASETS = {'blocks', 'transactions', 'logs', 'traces'}

    def find_zero_row_ranges(self, dataset: str, start_block: int, end_block: int) -> List[Tuple[int, int]]:
        """
        Find completed ranges with 0 rows indexed (silent failures).
        Only checks datasets where 0 rows is always suspicious (blocks, transactions, logs, traces).
        Datasets like contracts, traces, native_transfers can legitimately have empty ranges.
        """
        # Skip datasets where empty ranges are normal
        if dataset not in self.ZERO_ROW_CHECK_DATASETS:
            return []

        try:
            client = self.db._connect()

            extra_filter = ""

            query = f"""
            SELECT start_block, end_block
            FROM {self.database}.indexing_state FINAL
            WHERE status = 'completed'
              AND dataset = '{dataset}'
              AND (rows_indexed IS NULL OR rows_indexed = 0)
              AND start_block >= {start_block}
              AND end_block <= {end_block}
              {extra_filter}
            ORDER BY start_block
            """
            result = client.query(query)

            ranges = [(row[0], row[1]) for row in result.result_rows]
            if ranges:
                logger.info(f"Found {len(ranges)} zero-row completed ranges for {dataset}")

            return ranges

        except Exception as e:
            logger.error(f"Error finding zero-row ranges: {e}")
            return []

    def mark_range_for_reprocess(self, dataset: str, start_block: int, end_block: int) -> None:
        """Mark a completed range as failed so it gets reprocessed."""
        try:
            client = self.db._connect()

            insert = f"""
            INSERT INTO {self.database}.indexing_state
            (dataset, start_block, end_block, status, error_message, attempt_count)
            VALUES
            ('{dataset}', {start_block}, {end_block}, 'failed',
             'Auto-maintain: zero rows detected, marked for reprocess', 1)
            """
            client.command(insert)

            logger.info(f"Marked {dataset} {start_block}-{end_block} for reprocessing (zero rows)")

        except Exception as e:
            logger.error(f"Error marking range for reprocess: {e}")

    # Read-after-write validation of a just-inserted block range.
    #
    # On ClickHouse Cloud (SharedMergeTree) a SELECT issued a second after the INSERT
    # regularly still misses the rows, so "found 0 of 100" is NOT evidence that the
    # blocks are wrong - only that this replica has not caught up yet. Retry with a
    # short backoff, and ask for sequential consistency so the read is guaranteed to
    # observe our own write.
    TIMESTAMP_VALIDATION_ATTEMPTS = 5
    TIMESTAMP_VALIDATION_BACKOFF_SECONDS = 0.5

    def validate_timestamps(self, start_block: int, end_block: int) -> TimestampCheck:
        """
        Check that every block in [start_block, end_block) is present with a usable
        timestamp, distinguishing "not visible yet" from "genuinely bad".

        The invariant callers rely on is unchanged: unless this returns ok=True, no
        dependent dataset (transactions, logs, ...) may be indexed against this range,
        because those derive their partition timestamps from the blocks table.
        """
        expected_count = end_block - start_block
        if expected_count <= 0:
            return TimestampCheck(True, 'ok', 0, 0, 0)

        last_error: Optional[Exception] = None
        valid_count = 0
        present_count = 0

        for attempt in range(1, self.TIMESTAMP_VALIDATION_ATTEMPTS + 1):
            try:
                last_error = None
                valid_count = self._count_blocks(start_block, end_block, valid_only=True)

                if valid_count >= expected_count:
                    if attempt > 1:
                        logger.info(
                            f"Timestamp validation for blocks {start_block}-{end_block} "
                            f"succeeded on attempt {attempt} (read-after-write lag)"
                        )
                    logger.debug(
                        f"Timestamp validation passed: {valid_count}/{expected_count} "
                        f"blocks have valid timestamps"
                    )
                    return TimestampCheck(True, 'ok', expected_count, valid_count, valid_count)

                # Are the missing blocks simply not visible yet, or are they visible
                # and carrying a bad timestamp? Same query minus the timestamp
                # predicates tells us which.
                present_count = self._count_blocks(start_block, end_block, valid_only=False)

                if present_count >= expected_count:
                    # Every block is readable, so this is a data problem, not a race.
                    # Retrying cannot change the answer - fail now.
                    self._log_timestamp_failure(
                        start_block, end_block, expected_count, valid_count, present_count,
                        "blocks are visible but carry missing/garbage timestamps"
                    )
                    return TimestampCheck(
                        False, 'invalid', expected_count, valid_count, present_count
                    )

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Error checking timestamps for blocks {start_block}-{end_block} "
                    f"(attempt {attempt}/{self.TIMESTAMP_VALIDATION_ATTEMPTS}): {e}"
                )

            if attempt < self.TIMESTAMP_VALIDATION_ATTEMPTS:
                delay = self.TIMESTAMP_VALIDATION_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.debug(
                    f"Blocks {start_block}-{end_block} not fully visible yet "
                    f"({valid_count}/{expected_count} valid, {present_count} present); "
                    f"retrying in {delay:.1f}s"
                )
                time.sleep(delay)

        if last_error is not None:
            logger.error(
                f"Timestamp validation for blocks {start_block}-{end_block} could not be "
                f"completed: {last_error}"
            )
            return TimestampCheck(False, 'error', expected_count, valid_count, present_count)

        self._log_timestamp_failure(
            start_block, end_block, expected_count, valid_count, present_count,
            f"blocks still not visible after {self.TIMESTAMP_VALIDATION_ATTEMPTS} attempts"
        )
        return TimestampCheck(False, 'not_visible', expected_count, valid_count, present_count)

    def has_valid_timestamps(self, start_block: int, end_block: int) -> bool:
        """Check if all blocks in range have valid timestamps."""
        return self.validate_timestamps(start_block, end_block).ok

    def _count_blocks(self, start_block: int, end_block: int, valid_only: bool) -> int:
        """
        Count distinct blocks in [start_block, end_block), optionally restricted to
        those with a usable timestamp. Uses FINAL for the deduplicated view.
        """
        timestamp_filter = ""
        if valid_only:
            timestamp_filter = """
            AND timestamp IS NOT NULL
            AND timestamp > 0
            AND toDateTime(timestamp) > toDateTime('1971-01-01 00:00:00')
            """

        query = f"""
        SELECT COUNT(DISTINCT block_number)
        FROM {self.database}.blocks FINAL
        WHERE block_number >= {start_block}
        AND block_number < {end_block}
        {timestamp_filter}
        """

        result = self._query_sequentially_consistent(query)
        return result.result_rows[0][0] if result.result_rows else 0

    def _query_sequentially_consistent(self, query: str):
        """
        Run a query that must observe writes this process just made.

        select_sequential_consistency makes the replica wait for the latest committed
        entries before answering, which is what closes the read-after-write hole on
        SharedMergeTree. Servers that reject the setting fall back to a plain query
        (the retry loop above is then the only protection), and we stop asking.
        """
        client = self.db._connect()

        if getattr(self, '_sequential_consistency_supported', True):
            try:
                return client.query(query, settings={'select_sequential_consistency': 1})
            except Exception as e:
                self._sequential_consistency_supported = False
                logger.warning(
                    f"select_sequential_consistency unavailable, falling back to plain "
                    f"reads for validation: {e}"
                )

        return client.query(query)

    def _log_timestamp_failure(self, start_block: int, end_block: int, expected_count: int,
                               valid_count: int, present_count: int, why: str) -> None:
        """Log a failed timestamp validation together with a sample of the offending rows."""
        logger.error(f"Timestamp validation failed for blocks {start_block}-{end_block}: {why}")
        logger.error(
            f"Expected: {expected_count}, With valid timestamp: {valid_count}, "
            f"Present at all: {present_count}"
        )

        try:
            debug_query = f"""
            SELECT
                block_number,
                timestamp,
                toDateTime(timestamp) as formatted_timestamp
            FROM {self.database}.blocks FINAL
            WHERE block_number >= {start_block}
            AND block_number < {end_block}
            ORDER BY block_number
            LIMIT 5
            """
            debug_result = self._query_sequentially_consistent(debug_query)

            if debug_result.result_rows:
                logger.error("Sample blocks (deduplicated view):")
                for row in debug_result.result_rows:
                    logger.error(f"  Block {row[0]}: timestamp={row[1]}, formatted={row[2]}")
            else:
                logger.error("No blocks found in deduplicated view")
        except Exception as e:
            logger.error(f"Could not sample blocks for debugging: {e}")
