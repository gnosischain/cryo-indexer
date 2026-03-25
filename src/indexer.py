import os
import time
import sys
import signal
import threading
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from .config import settings, OperationType
from .core.blockchain import BlockchainClient
from .core.state_manager import StateManager
from .core.utils import setup_logging
from .db.clickhouse_manager import ClickHouseManager
from .worker import IndexerWorker
from . import observability as obs
from .observability import start_metrics_server, update_health


class CryoIndexer:
    """Simplified stateless indexer with three core operations."""
    
    def __init__(self):
        # Validate settings
        settings.validate()
        
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
        logger.info(f"Starting Cryo Indexer")
        logger.info(f"Operation: {settings.operation.value}")
        logger.info(f"Mode: {settings.mode.value}")
        logger.info(f"Datasets: {settings.datasets}")
        logger.info(f"Workers: {settings.workers}")
        logger.info(f"Max retries per dataset: {settings.max_retries}")
        
        # Initialize components
        self.blockchain = BlockchainClient(settings.eth_rpc_url)
        self.clickhouse = ClickHouseManager(
            host=settings.clickhouse_host,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            port=settings.clickhouse_port,
            secure=settings.clickhouse_secure
        )
        self.state_manager = StateManager(self.clickhouse)
        
        # For maintain operations, skip stuck range recovery at startup
        # (maintain will call it explicitly as part of its flow)
        if settings.operation not in (OperationType.MAINTAIN, OperationType.AUTO_MAINTAIN, OperationType.VALIDATE):
            recovered = self.state_manager.recover_stuck_ranges(
                timeout_hours=settings.stuck_range_timeout_hours
            )
            if recovered > 0:
                logger.info(f"Recovered {recovered} stuck processing ranges at startup")
        
        # Start metrics/health HTTP server on port 9090
        try:
            start_metrics_server(port=9090)
            update_health(
                status='ok',
                clickhouse_connected=True,
                rpc_connected=True,
                operation=settings.operation.value,
                datasets=settings.datasets
            )
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")

        # Create directories
        os.makedirs(settings.data_dir, exist_ok=True)

        # Set up signal handling
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)
        
        self.running = True
        
        # Track start time for progress reporting
        self.start_time = time.time()
    
    def _handle_exit(self, sig, frame):
        """Handle shutdown gracefully."""
        logger.info("Shutdown signal received")
        self.running = False
        sys.exit(0)
    
    def run(self):
        """Run the indexer based on operation type."""
        if settings.operation == OperationType.CONTINUOUS:
            self._run_continuous()
        elif settings.operation == OperationType.HISTORICAL:
            self._run_historical()
        elif settings.operation == OperationType.MAINTAIN:
            self._run_maintain()
        elif settings.operation == OperationType.VALIDATE:
            self._run_validate()
        elif settings.operation == OperationType.AUTO_MAINTAIN:
            self._run_auto_maintain()
        else:
            logger.error(f"Unknown operation: {settings.operation}")
            sys.exit(1)
    
    def _run_continuous(self):
        """
        Run continuous indexing, following the chain tip with consistent 100-block ranges.
        
        Implements per-dataset retry tracking:
        - Each dataset is tracked independently for retry attempts
        - After max_retries failures, a dataset is skipped for the current range
        - Once all datasets are either completed or skipped, move to next range
        - Failed datasets can be fixed later with 'make maintain'
        """
        logger.info("Starting continuous indexing with fixed 100-block ranges")
        logger.info(f"Max retries per dataset: {settings.max_retries}")
        
        # Get starting point from database  
        last_block = self.state_manager.get_last_synced_block(
            settings.datasets
        )
        
        if settings.start_block > last_block:
            last_block = settings.start_block
        
        # Always align to batch_size boundaries for continuous mode
        # Round down to nearest batch_size boundary to ensure consistent ranges
        aligned_start = (last_block // settings.batch_size) * settings.batch_size
        if aligned_start < last_block:
            aligned_start += settings.batch_size
        
        logger.info(f"Starting from block {last_block}, aligned to range start {aligned_start}")
        logger.info(f"Using fixed batch size: {settings.batch_size} blocks")
        
        current_range_start = aligned_start
        
        # Per-dataset retry tracking: {(start_block, end_block, dataset): retry_count}
        dataset_retry_counts: Dict[Tuple[int, int, str], int] = {}
        
        # Create worker
        worker = IndexerWorker(
            worker_id="continuous",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url
        )
        
        while self.running:
            try:
                # Get latest block
                latest_block = self.blockchain.get_latest_block_number()
                safe_block = latest_block - settings.confirmation_blocks
                obs.chain_head_block.set(latest_block)
                obs.chain_lag_blocks.labels(dataset='all').set(latest_block - current_range_start)
                
                # Calculate next range
                range_end = current_range_start + settings.batch_size
                
                # Check if we can process this range
                if range_end <= safe_block:
                    # Filter out datasets that have exceeded max retries for this range
                    datasets_to_process = []
                    skipped_datasets = []
                    
                    for dataset in settings.datasets:
                        key = (current_range_start, range_end, dataset)
                        retry_count = dataset_retry_counts.get(key, 0)
                        
                        if retry_count >= settings.max_retries:
                            skipped_datasets.append(dataset)
                        else:
                            datasets_to_process.append(dataset)
                    
                    # Log skipped datasets
                    if skipped_datasets:
                        logger.warning(
                            f"Skipping datasets for range {current_range_start}-{range_end} "
                            f"(max retries {settings.max_retries} exceeded): {skipped_datasets}"
                        )
                    
                    # Check if all datasets are done (either completed or max retries exceeded)
                    if not datasets_to_process:
                        logger.warning(
                            f"All datasets exhausted for range {current_range_start}-{range_end}. "
                            f"Moving to next range. Run 'make maintain' to fix failed datasets."
                        )
                        # Clear retry counts for this range and move forward
                        dataset_retry_counts = {
                            k: v for k, v in dataset_retry_counts.items() 
                            if k[0] != current_range_start or k[1] != range_end
                        }
                        current_range_start = range_end
                        continue
                    
                    logger.info(
                        f"Processing blocks {current_range_start}-{range_end} "
                        f"(exactly {settings.batch_size} blocks, datasets: {datasets_to_process})"
                    )
                    
                    # Process the range with filtered datasets
                    success, failed_datasets = worker.process_range(
                        current_range_start, range_end, datasets_to_process
                    )
                    
                    if success:
                        # All datasets succeeded - move to next range
                        logger.info(f"Range {current_range_start}-{range_end} completed successfully")
                        
                        # Clear retry counts for this range
                        dataset_retry_counts = {
                            k: v for k, v in dataset_retry_counts.items() 
                            if k[0] != current_range_start or k[1] != range_end
                        }
                        current_range_start = range_end
                    else:
                        # Some datasets failed - increment their retry counts
                        for dataset in failed_datasets:
                            key = (current_range_start, range_end, dataset)
                            current_count = dataset_retry_counts.get(key, 0)
                            new_count = current_count + 1
                            dataset_retry_counts[key] = new_count
                            
                            if new_count >= settings.max_retries:
                                logger.error(
                                    f"Dataset '{dataset}' for range {current_range_start}-{range_end} "
                                    f"failed {new_count} times (max: {settings.max_retries}). "
                                    f"Will be skipped. Run 'make maintain' to fix."
                                )
                            else:
                                logger.warning(
                                    f"Dataset '{dataset}' for range {current_range_start}-{range_end} "
                                    f"failed, attempt {new_count}/{settings.max_retries}"
                                )
                        
                        # Sleep before retrying
                        time.sleep(settings.poll_interval)
                else:
                    logger.debug(f"Waiting for more blocks. Need {range_end}, safe block is {safe_block}")
                    time.sleep(settings.poll_interval)
                        
            except Exception as e:
                logger.error(f"Error in continuous loop: {e}", exc_info=True)
                time.sleep(settings.poll_interval)
    
    def _run_historical(self):
        """Run historical indexing for a specific range."""
        logger.info(f"Starting historical indexing from {settings.start_block} to {settings.end_block}")
        
        if settings.end_block <= settings.start_block:
            logger.error("END_BLOCK must be greater than START_BLOCK")
            sys.exit(1)
        
        if settings.workers == 1:
            self._run_historical_single()
        else:
            self._run_historical_parallel()
    
    def _run_historical_single(self):
        """Run historical indexing with a single worker."""
        worker = IndexerWorker(
            worker_id="historical",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url
        )
        
        current = settings.start_block
        total_blocks = settings.end_block - settings.start_block
        
        while current < settings.end_block and self.running:
            batch_end = min(current + settings.batch_size, settings.end_block)
            
            logger.info(f"Processing blocks {current}-{batch_end}")
            success, failed_datasets = worker.process_range(current, batch_end, settings.datasets)
            
            if success:
                current = batch_end
                self._log_progress(current - settings.start_block, total_blocks)
            else:
                logger.error(f"Failed to process blocks {current}-{batch_end}, failed datasets: {failed_datasets}")
                time.sleep(5)  # Brief pause before retry
        
        logger.info("Historical indexing complete")
    
    def _run_historical_parallel(self):
        """Run historical indexing with multiple workers."""
        logger.info(f"Starting parallel historical indexing with {settings.workers} workers")
        
        # Create work ranges
        ranges = []
        current = settings.start_block
        while current < settings.end_block:
            batch_end = min(current + settings.batch_size, settings.end_block)
            ranges.append((current, batch_end))
            current = batch_end
        
        total_ranges = len(ranges)
        completed = 0
        failed = 0
        
        logger.info(f"Created {total_ranges} work items")
        
        def process_range(range_info):
            start, end = range_info
            worker = IndexerWorker(
                worker_id=f"parallel_{threading.current_thread().ident}",
                blockchain=self.blockchain,
                clickhouse=self.clickhouse,
                state_manager=self.state_manager,
                data_dir=settings.data_dir,
                network_name=settings.network_name,
                rpc_url=settings.eth_rpc_url
            )
            
            success, failed_datasets = worker.process_range(start, end, settings.datasets)
            return (start, end, success, failed_datasets)
        
        # Process ranges in parallel
        with ThreadPoolExecutor(max_workers=settings.workers) as executor:
            future_to_range = {executor.submit(process_range, r): r for r in ranges}
            
            for future in as_completed(future_to_range):
                start, end, success, failed_datasets = future.result()
                
                if success:
                    completed += 1
                    logger.info(f"✓ Completed {start}-{end}")
                else:
                    failed += 1
                    logger.error(f"✗ Failed {start}-{end}, failed datasets: {failed_datasets}")
                
                # Progress report
                if (completed + failed) % 10 == 0:
                    progress = (completed + failed) / total_ranges * 100
                    logger.info(f"Progress: {completed + failed}/{total_ranges} ({progress:.1f}%) - ✓{completed} ✗{failed}")
        
        logger.info(f"Historical indexing complete. ✓ Completed: {completed}, ✗ Failed: {failed}")
        
        if failed > 0:
            logger.error("Some ranges failed. Run 'maintain' operation to fix.")
            sys.exit(1)
    
    def _run_maintain(self):
        """
        Process all non-completed ranges using the same flow as historical/continuous.
        Simple: find non-completed ranges, then process them exactly like other modes.
        """
        logger.info("Starting maintain operation")
        
        # Determine range filter
        start_filter = settings.start_block if settings.start_block > 0 else 0
        end_filter = settings.end_block if settings.end_block > 0 else 0
        
        if start_filter > 0 or end_filter > 0:
            logger.info(f"Range filter: START_BLOCK={start_filter} END_BLOCK={end_filter}")
        else:
            logger.info("No range filter - processing ALL non-completed ranges")
        
        try:
            # Get all non-completed ranges that need processing
            failed_ranges = self._get_non_completed_ranges(start_filter, end_filter)

            # Also find gap ranges (never-indexed ranges within completed scope)
            # These are ranges with NO entry in indexing_state at all
            existing_set = set(failed_ranges)
            for dataset in settings.datasets:
                gaps = self.state_manager.find_gaps(
                    dataset, start_filter, end_filter
                )
                for gap_start, gap_end in gaps:
                    # Split large gaps into batch_size-sized ranges
                    current = gap_start
                    while current < gap_end:
                        range_end = min(current + settings.batch_size, gap_end)
                        range_tuple = (current, range_end, dataset)
                        if range_tuple not in existing_set:
                            failed_ranges.append(range_tuple)
                            existing_set.add(range_tuple)
                        current = range_end

            if not failed_ranges:
                logger.info("✅ No non-completed ranges or gaps found - nothing to maintain!")
                return

            logger.info(f"Found {len(failed_ranges)} ranges to process (including gaps)")
            
            # Process them using the same logic as historical mode
            if settings.workers == 1:
                self._process_maintain_ranges_single(failed_ranges)
            else:
                self._process_maintain_ranges_parallel(failed_ranges)
                
            logger.info("✅ MAINTAIN OPERATION COMPLETE")
                    
        except Exception as e:
            logger.error(f"Error in maintain operation: {e}")
            sys.exit(1)

    def _run_auto_maintain(self):
        """
        Periodic self-healing operation. Safe to run alongside continuous indexer.
        Scans recent data for gaps, failed ranges, stuck processing, and zero-row completions.
        Uses claim_range() for concurrency safety (not maintenance mode).
        """
        logger.info("Starting auto-maintain operation")
        logger.info(f"Lookback: {settings.auto_maintain_lookback_hours} hours")
        logger.info(f"Stuck range timeout: {settings.stuck_range_timeout_hours} hours")

        try:
            # Step 1: Recover stuck processing ranges
            recovered = self.state_manager.recover_stuck_ranges(
                timeout_hours=settings.stuck_range_timeout_hours
            )
            if recovered > 0:
                logger.info(f"Recovered {recovered} stuck processing ranges")

            # Step 2: Calculate lookback window
            # Gnosis chain: ~5 second block time = ~720 blocks/hour
            blocks_per_hour = 720
            lookback_blocks = settings.auto_maintain_lookback_hours * blocks_per_hour

            ranges_to_fix = []
            existing_set = set()

            for dataset in settings.datasets:
                highest = self.state_manager._get_highest_attempted_block(dataset)
                if highest == 0:
                    continue

                lookback_start = max(0, highest - lookback_blocks)
                # Align to batch_size
                lookback_start = (lookback_start // settings.batch_size) * settings.batch_size

                logger.info(f"Auto-maintain scanning {dataset}: blocks {lookback_start}-{highest}")

                # Step 3: Find gaps in recent window
                gaps = self.state_manager.find_gaps(dataset, lookback_start, highest)
                for gap_start, gap_end in gaps:
                    current = gap_start
                    while current < gap_end:
                        range_end = min(current + settings.batch_size, gap_end)
                        range_tuple = (current, range_end, dataset)
                        if range_tuple not in existing_set:
                            ranges_to_fix.append(range_tuple)
                            existing_set.add(range_tuple)
                        current = range_end

                # Step 4: Find failed ranges in recent window
                try:
                    client = self.state_manager.db._connect()
                    failed_query = f"""
                    SELECT DISTINCT start_block, end_block
                    FROM {self.state_manager.database}.indexing_state FINAL
                    WHERE dataset = '{dataset}'
                    AND status = 'failed'
                    AND start_block >= {lookback_start}
                    AND end_block <= {highest}
                    ORDER BY start_block
                    """
                    result = client.query(failed_query)
                    for row in result.result_rows:
                        range_tuple = (row[0], row[1], dataset)
                        if range_tuple not in existing_set:
                            ranges_to_fix.append(range_tuple)
                            existing_set.add(range_tuple)
                except Exception as e:
                    logger.error(f"Error finding failed ranges for {dataset}: {e}")

                # Step 5: Find zero-row completed ranges
                zero_ranges = self.state_manager.find_zero_row_ranges(
                    dataset, lookback_start, highest
                )
                for start, end in zero_ranges:
                    self.state_manager.mark_range_for_reprocess(dataset, start, end)
                    range_tuple = (start, end, dataset)
                    if range_tuple not in existing_set:
                        ranges_to_fix.append(range_tuple)
                        existing_set.add(range_tuple)

            if not ranges_to_fix:
                logger.info("✅ Auto-maintain: no issues found in recent data")
                return

            logger.info(f"Auto-maintain found {len(ranges_to_fix)} ranges to fix")

            # Step 6: Process ranges using normal worker flow (NOT maintenance mode)
            # Use claim_range for concurrency safety with continuous indexer
            if settings.workers == 1:
                self._process_auto_maintain_single(ranges_to_fix)
            else:
                self._process_auto_maintain_parallel(ranges_to_fix)

            logger.info("✅ AUTO-MAINTAIN OPERATION COMPLETE")

        except Exception as e:
            logger.error(f"Error in auto-maintain operation: {e}")
            sys.exit(1)

    def _process_auto_maintain_single(self, ranges):
        worker = IndexerWorker(
            worker_id="auto_maintain_single",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url
        )

        fixed = 0
        skipped = 0
        failed = 0

        for i, (start, end, dataset) in enumerate(ranges):
            logger.info(f"Auto-maintain {i+1}/{len(ranges)}: {dataset} {start}-{end}")

            # Delete existing data (including withdrawals for blocks)
            table_name = worker.table_mappings.get(dataset, dataset)
            self.clickhouse.delete_range_with_related(table_name, start, end)

            # Process using normal flow (claim_range will skip if continuous has it)
            success = worker._process_single_dataset(dataset, start, end)
            if success:
                fixed += 1
                logger.info(f"✅ Auto-fixed {dataset} {start}-{end}")
            else:
                # Check if it was skipped due to claim conflict
                status = self.state_manager.get_range_status(dataset, start, end)
                if status == 'processing':
                    skipped += 1
                    logger.info(f"⏭️ Skipped {dataset} {start}-{end} (being processed by another worker)")
                else:
                    failed += 1
                    logger.error(f"❌ Failed to fix {dataset} {start}-{end}")

        logger.info(f"Auto-maintain complete. Fixed: {fixed}, Skipped: {skipped}, Failed: {failed}")

    def _process_auto_maintain_parallel(self, ranges):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        fixed = 0
        skipped = 0
        failed = 0

        def process_range(start, end, dataset):
            worker = IndexerWorker(
                worker_id=f"auto_maintain_parallel_{id(start)}",
                blockchain=self.blockchain,
                clickhouse=self.clickhouse,
                state_manager=self.state_manager,
                data_dir=settings.data_dir,
                network_name=settings.network_name,
                rpc_url=settings.eth_rpc_url
            )

            table_name = worker.table_mappings.get(dataset, dataset)
            self.clickhouse.delete_range_with_related(table_name, start, end)

            return worker._process_single_dataset(dataset, start, end)

        with ThreadPoolExecutor(max_workers=settings.workers) as executor:
            futures = {}
            for start, end, dataset in ranges:
                future = executor.submit(process_range, start, end, dataset)
                futures[future] = (start, end, dataset)

            for future in as_completed(futures):
                start, end, dataset = futures[future]
                try:
                    if future.result():
                        fixed += 1
                        logger.info(f"✅ Auto-fixed {dataset} {start}-{end}")
                    else:
                        status = self.state_manager.get_range_status(dataset, start, end)
                        if status == 'processing':
                            skipped += 1
                        else:
                            failed += 1
                            logger.error(f"❌ Failed {dataset} {start}-{end}")
                except Exception as e:
                    failed += 1
                    logger.error(f"❌ Error fixing {dataset} {start}-{end}: {e}")

        logger.info(f"Auto-maintain parallel complete. Fixed: {fixed}, Skipped: {skipped}, Failed: {failed}")

    def _get_non_completed_ranges(self, start_filter: int, end_filter: int) -> List[Tuple[int, int, str]]:
        """
        Get all ranges that are not completed (failed, processing, pending).
        ASSUMES ALL SCRAPERS ARE STOPPED - Claims all non-completed ranges.
        Returns list of (start_block, end_block, dataset) tuples.
        """
        try:
            client = self.state_manager.db._connect()
            
            # Build WHERE clause
            datasets_str = "','".join(settings.datasets)
            where_clause = f"dataset IN ('{datasets_str}')"
            
            if start_filter > 0:
                where_clause += f" AND start_block >= {start_filter}"
            if end_filter > 0:
                where_clause += f" AND end_block <= {end_filter}"
            
            # RELIABLE APPROACH: Use MAX(created_at) to get the truly latest record
            # Since maintain assumes scrapers are stopped, we claim ALL non-completed ranges
            query = f"""
            WITH latest_per_range AS (
                SELECT 
                    start_block,
                    end_block,
                    dataset,
                    MAX(created_at) as max_created_at
                FROM {self.state_manager.database}.indexing_state
                WHERE {where_clause}
                GROUP BY start_block, end_block, dataset
            ),
            latest_status AS (
                SELECT 
                    l.start_block,
                    l.end_block,
                    l.dataset,
                    s.status
                FROM latest_per_range l
                JOIN {self.state_manager.database}.indexing_state s
                ON l.start_block = s.start_block 
                AND l.end_block = s.end_block 
                AND l.dataset = s.dataset
                AND l.max_created_at = s.created_at
            )
            SELECT
                start_block,
                end_block,
                dataset
            FROM latest_status
            WHERE status != 'completed'
            AND NOT (dataset IN ('balance_diffs', 'code_diffs', 'nonce_diffs', 'storage_diffs') AND start_block = 0)
            ORDER BY dataset, start_block
            """
            
            result = client.query(query)
            ranges = [(row[0], row[1], row[2]) for row in result.result_rows]
            
            # Debug: Show what we found
            if ranges:
                logger.info(f"Found {len(ranges)} non-completed ranges:")
                for start, end, dataset in ranges[:10]:  # Show first 10
                    # Get the actual latest status for debugging
                    debug_query = f"""
                    SELECT status, created_at, worker_id
                    FROM {self.state_manager.database}.indexing_state
                    WHERE dataset = '{dataset}'
                    AND start_block = {start}
                    AND end_block = {end}
                    ORDER BY created_at DESC
                    LIMIT 5
                    """
                    debug_result = client.query(debug_query)
                    statuses = [(row[0], str(row[1]), row[2]) for row in debug_result.result_rows]
                    logger.info(f"  DEBUG {dataset} {start}-{end}: ALL recent statuses = {statuses}")
                    
                    # Also check what our query logic found as latest
                    latest_query = f"""
                    WITH latest_per_range AS (
                        SELECT 
                            start_block,
                            end_block,
                            dataset,
                            MAX(created_at) as max_created_at
                        FROM {self.state_manager.database}.indexing_state
                        WHERE dataset = '{dataset}'
                        AND start_block = {start}
                        AND end_block = {end}
                        GROUP BY start_block, end_block, dataset
                    ),
                    latest_status AS (
                        SELECT 
                            l.start_block,
                            l.end_block,
                            l.dataset,
                            s.status,
                            s.created_at
                        FROM latest_per_range l
                        JOIN {self.state_manager.database}.indexing_state s
                        ON l.start_block = s.start_block 
                        AND l.end_block = s.end_block 
                        AND l.dataset = s.dataset
                        AND l.max_created_at = s.created_at
                    )
                    SELECT status, created_at FROM latest_status
                    """
                    latest_result = client.query(latest_query)
                    latest_status = latest_result.result_rows[0] if latest_result.result_rows else None
                    logger.info(f"  DEBUG QUERY LOGIC FOUND: {latest_status}")
                
                if len(ranges) > 10:
                    logger.info(f"  ... and {len(ranges) - 10} more ranges")
            
            logger.info(f"Non-completed ranges breakdown:")
            for dataset in settings.datasets:
                dataset_ranges = [r for r in ranges if r[2] == dataset]
                if dataset_ranges:
                    logger.info(f"  {dataset}: {len(dataset_ranges)} ranges")
            
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting non-completed ranges: {e}")
            return []

    def _process_maintain_ranges_single(self, ranges: List[Tuple[int, int, str]]):
        """Process maintain ranges one by one using normal worker flow."""
        worker = IndexerWorker(
            worker_id="maintain_single",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url
        )
        
        fixed = 0
        failed = 0
        
        for i, (start, end, dataset) in enumerate(ranges):
            logger.info(f"Processing range {i+1}/{len(ranges)}: {dataset} {start}-{end}")
            
            # Use the same flow as historical/continuous:
            # 1. Delete existing data (for maintain)
            # 2. Process the range normally (claim -> process -> complete/fail)
            
            if self._process_maintain_range(worker, start, end, dataset):
                fixed += 1
                logger.info(f"✅ Fixed {dataset} {start}-{end}")
            else:
                failed += 1
                logger.error(f"❌ Failed {dataset} {start}-{end}")
        
        logger.info(f"Single-threaded processing complete. Fixed: {fixed}, Failed: {failed}")

    def _process_maintain_ranges_parallel(self, ranges: List[Tuple[int, int, str]]):
        """Process maintain ranges in parallel using normal worker flow."""
        def process_maintain_range(range_info):
            start, end, dataset = range_info
            worker_id = f"maintain_parallel_{threading.current_thread().ident}"
            
            worker = IndexerWorker(
                worker_id=worker_id,
                blockchain=self.blockchain,
                clickhouse=self.clickhouse,
                state_manager=self.state_manager,
                data_dir=settings.data_dir,
                network_name=settings.network_name,
                rpc_url=settings.eth_rpc_url
            )
            
            success = self._process_maintain_range(worker, start, end, dataset)
            return (start, end, dataset, success)
        
        fixed = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=settings.workers) as executor:
            futures = {executor.submit(process_maintain_range, r): r for r in ranges}
            
            for future in as_completed(futures):
                start, end, dataset, success = future.result()
                if success:
                    fixed += 1
                    logger.info(f"✅ Fixed {dataset} {start}-{end}")
                else:
                    failed += 1
                    logger.error(f"❌ Failed {dataset} {start}-{end}")
        
        logger.info(f"Parallel processing complete. Fixed: {fixed}, Failed: {failed}")

    def _process_maintain_range(self, worker: IndexerWorker, start: int, end: int, dataset: str) -> bool:
        """
        Process a single maintain range using the SAME flow as historical/continuous.
        The only difference is we delete existing data first.
        """
        try:
            # Step 1: Delete existing data (this is maintain-specific)
            table_name = worker.table_mappings.get(dataset, dataset)
            deleted_rows = self.clickhouse.delete_range_with_related(table_name, start, end)
            if deleted_rows > 0:
                logger.info(f"Deleted {deleted_rows} existing rows for {dataset} {start}-{end}")
            
            # Step 2: Process using the SAME flow as historical/continuous
            # This will: claim_range -> extract_and_load -> complete_range (OR fail_range)
            success = worker._process_single_dataset(dataset, start, end)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing maintain range {dataset} {start}-{end}: {e}")
            return False
    
    def _run_validate(self):
        """Run validation operation to check data integrity (read-only)."""
        logger.info("Starting validation operation")
        
        # Get progress summary
        summary = self.state_manager.get_processing_summary()
        
        print("\n=== INDEXING PROGRESS ===\n")
        for dataset, stats in summary.items():
            print(f"{dataset}:")
            print(f"  Completed ranges: {stats.get('completed_ranges', 0)}")
            print(f"  Processing ranges: {stats.get('processing_ranges', 0)}")
            print(f"  Failed ranges: {stats.get('failed_ranges', 0)}")
            print(f"  Pending ranges: {stats.get('pending_ranges', 0)}")
            print(f"  Highest attempted: {stats.get('highest_attempted_block', 0)}")
            print(f"  Highest completed: {stats.get('highest_completed_block', 0)}")
            print(f"  Total rows: {stats.get('total_rows_indexed', 0):,}")
            print(f"  Status: {stats.get('status', 'unknown')}")
            print(f"  Progress: {stats.get('completion_percentage', 0):.1f}%")
            print()
        
        # Find gaps if range is specified
        if settings.start_block or settings.end_block:
            start = settings.start_block or 0
            end = settings.end_block or self.blockchain.get_latest_block_number()
            
            print(f"\n=== CHECKING FOR ACTIONABLE GAPS ({start} to {end}) ===\n")
            
            total_gaps = 0
            for dataset in settings.datasets:
                gaps = self.state_manager.find_gaps(
                    dataset, start, end
                )
                
                if gaps:
                    total_gaps += len(gaps)
                    print(f"{dataset}: {len(gaps)} actionable gaps found")
                    for i, (gap_start, gap_end) in enumerate(gaps[:5]):
                        print(f"  Gap {i+1}: blocks {gap_start}-{gap_end} ({gap_end - gap_start} blocks)")
                    if len(gaps) > 5:
                        print(f"  ... and {len(gaps) - 5} more")
                else:
                    print(f"{dataset}: No actionable gaps found ✓")
            
            if total_gaps > 0:
                print(f"\n💡 Run 'make maintain START_BLOCK={start} END_BLOCK={end}' to fix these issues")
                sys.exit(1)
            else:
                print("\n✅ Validation passed!")
        else:
            # If no range specified, check for failed/pending work
            has_issues = False
            for dataset, stats in summary.items():
                if stats.get('failed_ranges', 0) > 0 or stats.get('processing_ranges', 0) > 0:
                    has_issues = True
                    break
            
            if has_issues:
                print("\n💡 Run 'make maintain' to fix failed/stale ranges")
                sys.exit(1)
            else:
                print("\n✅ All datasets look healthy!")
        
        logger.info("Validation completed")
    
    def _log_progress(self, completed_blocks: int, total_blocks: int):
        """Log progress for single-threaded operations."""
        progress_pct = (completed_blocks / total_blocks * 100) if total_blocks > 0 else 0
        elapsed = time.time() - self.start_time
        
        if elapsed > 0 and completed_blocks > 0:
            rate = completed_blocks / elapsed
            eta = (total_blocks - completed_blocks) / rate if rate > 0 else 0
        else:
            rate = 0
            eta = 0
        
        logger.info(
            f"Progress: {completed_blocks}/{total_blocks} blocks ({progress_pct:.1f}%) | "
            f"Rate: {rate:.1f} blocks/sec | ETA: {eta/60:.1f} min"
        )


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()