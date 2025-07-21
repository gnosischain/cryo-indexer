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
        
        # Clean up stale jobs on startup
        self.state_manager.cleanup_stale_jobs()
        
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
        else:
            logger.error(f"Unknown operation: {settings.operation}")
            sys.exit(1)
    
    def _run_continuous(self):
        """Run continuous indexing, following the chain tip."""
        logger.info("Starting continuous indexing")
        
        # Get starting point from database
        last_block = self.state_manager.get_last_synced_block(
            settings.mode.value, settings.datasets
        )
        
        if settings.start_block > last_block:
            last_block = settings.start_block
            
        logger.info(f"Starting from block {last_block}")
        
        # Create worker
        worker = IndexerWorker(
            worker_id="continuous",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url,
            mode=settings.mode.value
        )
        
        while self.running:
            try:
                # Get latest block
                latest_block = self.blockchain.get_latest_block_number()
                safe_block = latest_block - settings.confirmation_blocks
                
                # Check if there's work to do
                if safe_block > last_block:
                    # Process in small batches for reliability
                    batch_end = min(last_block + settings.batch_size, safe_block)
                    
                    logger.info(f"Processing blocks {last_block}-{batch_end}")
                    success = worker.process_range(last_block, batch_end, settings.datasets)
                    
                    if success:
                        last_block = batch_end
                    else:
                        logger.error(f"Failed to process blocks {last_block}-{batch_end}")
                        time.sleep(settings.poll_interval)
                else:
                    logger.debug(f"No new blocks. Latest: {latest_block}, Last processed: {last_block}")
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
            rpc_url=settings.eth_rpc_url,
            mode=settings.mode.value
        )
        
        current = settings.start_block
        total_blocks = settings.end_block - settings.start_block
        
        while current < settings.end_block and self.running:
            batch_end = min(current + settings.batch_size, settings.end_block)
            
            logger.info(f"Processing blocks {current}-{batch_end}")
            success = worker.process_range(current, batch_end, settings.datasets)
            
            if success:
                current = batch_end
                self._log_progress(current - settings.start_block, total_blocks)
            else:
                logger.error(f"Failed to process blocks {current}-{batch_end}")
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
                rpc_url=settings.eth_rpc_url,
                mode=settings.mode.value
            )
            
            success = worker.process_range(start, end, settings.datasets)
            return (start, end, success)
        
        # Process ranges in parallel
        with ThreadPoolExecutor(max_workers=settings.workers) as executor:
            future_to_range = {executor.submit(process_range, r): r for r in ranges}
            
            for future in as_completed(future_to_range):
                start, end, success = future.result()
                
                if success:
                    completed += 1
                    logger.info(f"✓ Completed {start}-{end}")
                else:
                    failed += 1
                    logger.error(f"✗ Failed {start}-{end}")
                
                # Progress report
                if (completed + failed) % 10 == 0:
                    progress = (completed + failed) / total_ranges * 100
                    logger.info(f"Progress: {completed + failed}/{total_ranges} ({progress:.1f}%) - ✓{completed} ✗{failed}")
        
        logger.info(f"Historical indexing complete. ✓ Completed: {completed}, ✗ Failed: {failed}")
        
        if failed > 0:
            logger.error("Some ranges failed. Run 'maintain' operation to fix.")
            sys.exit(1)
    
    def _run_maintain(self):
        """Run maintenance operation - process failed/pending ranges, optionally filtered by block range."""
        logger.info("Starting maintenance operation")
        
        # Determine range filter
        start_filter = settings.start_block if settings.start_block > 0 else 0
        end_filter = settings.end_block if settings.end_block > 0 else 0
        
        if start_filter > 0 or end_filter > 0:
            logger.info(f"Filtering maintenance to block range: {start_filter or 'start'} to {end_filter or 'end'}")
            logger.info("Scanning indexing_state table for failed/pending ranges in specified range...")
        else:
            logger.info("Processing ALL failed/pending ranges in indexing_state table...")
        
        # Get issues, optionally filtered by range
        all_issues = self.state_manager.get_failed_and_pending_ranges(
            settings.mode.value, 
            settings.datasets,
            start_filter,
            end_filter
        )
        
        if not all_issues:
            if start_filter > 0 or end_filter > 0:
                logger.info("🎉 No maintenance issues found in the specified range!")
            else:
                logger.info("🎉 No maintenance issues found!")
            logger.info("All ranges in scope are either completed or currently processing.")
            return
        
        # Group issues by type for reporting
        failed_issues = [issue for issue in all_issues if issue[3] == 'failed']
        pending_issues = [issue for issue in all_issues if issue[3] == 'pending']
        
        if start_filter > 0 or end_filter > 0:
            logger.info(f"Found {len(all_issues)} ranges needing maintenance in range {start_filter or 'start'}-{end_filter or 'end'}:")
        else:
            logger.info(f"Found {len(all_issues)} ranges needing maintenance:")
        
        logger.info(f"  - {len(failed_issues)} failed ranges")
        logger.info(f"  - {len(pending_issues)} pending ranges")
        
        # Log what we're going to fix (first 10)
        for start, end, dataset, status in all_issues[:10]:
            logger.info(f"Will fix: {dataset} {start}-{end} (status: {status})")
        if len(all_issues) > 10:
            logger.info(f"... and {len(all_issues) - 10} more ranges")
        
        # Process issues
        if settings.workers == 1:
            self._maintain_single_state_issues(all_issues)
        else:
            self._maintain_parallel_state_issues(all_issues)
        
        logger.info("Maintenance operation completed")

    def _maintain_single_state_issues(self, all_issues: List[Tuple[int, int, str, str]]):
        """Single-threaded maintenance - process each state issue individually."""
        worker = IndexerWorker(
            worker_id="maintain",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url,
            mode=settings.mode.value
        )
        
        fixed = 0
        failed = 0
        
        for i, (start, end, dataset, status) in enumerate(all_issues):
            logger.info(f"Processing issue {i+1}/{len(all_issues)}: {dataset} {start}-{end} (was: {status})")
            
            # Process this specific dataset and range
            success = worker.process_range(start, end, [dataset])
            
            if success:
                fixed += 1
                logger.info(f"✓ Fixed {dataset} {start}-{end}")
            else:
                failed += 1
                logger.error(f"✗ Failed {dataset} {start}-{end}")
        
        logger.info(f"Maintenance complete. Fixed: {fixed}, Failed: {failed}")

    def _maintain_parallel_state_issues(self, all_issues: List[Tuple[int, int, str, str]]):
        """Parallel maintenance - process each state issue individually."""
        def fix_issue(issue_info):
            start, end, dataset, status = issue_info
            worker = IndexerWorker(
                worker_id=f"maintain_{threading.current_thread().ident}",
                blockchain=self.blockchain,
                clickhouse=self.clickhouse,
                state_manager=self.state_manager,
                data_dir=settings.data_dir,
                network_name=settings.network_name,
                rpc_url=settings.eth_rpc_url,
                mode=settings.mode.value
            )
            
            success = worker.process_range(start, end, [dataset])
            return (start, end, dataset, success)
        
        fixed = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=settings.workers) as executor:
            future_to_issue = {executor.submit(fix_issue, issue): issue for issue in all_issues}
            
            for future in as_completed(future_to_issue):
                start, end, dataset, success = future.result()
                
                if success:
                    fixed += 1
                    logger.info(f"✓ Fixed {dataset} {start}-{end}")
                else:
                    failed += 1
                    logger.error(f"✗ Failed {dataset} {start}-{end}")
        
        logger.info(f"Maintenance complete. Fixed: {fixed}, Failed: {failed}")
    
    def _run_validate(self):
        """Run validation operation to check data integrity (read-only)."""
        logger.info("Starting validation operation")
        
        # Get progress summary - FIXED: Use correct method name
        summary = self.state_manager.get_processing_summary(settings.mode.value)
        
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
                    settings.mode.value, dataset, start, end
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