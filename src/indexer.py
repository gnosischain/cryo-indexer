import os
import time
import sys
import signal
import uuid
import threading
from typing import List, Tuple, Optional, Dict
from loguru import logger

from .config import settings, OperationType, IndexMode
from .core.blockchain import BlockchainClient
from .core.state_manager import StateManager
from .core.utils import setup_logging
from .core.worker_pool import WorkerPool, WorkResult
from .db.clickhouse_manager import ClickHouseManager
from .worker import IndexerWorker
from .backfill_worker import BackfillWorker
from .timestamp_fixer import TimestampFixer


class CryoIndexer:
    """Main stateless indexer that uses database for all state management."""
    
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
        
        # Initialize components (blockchain not needed for backfill)
        if settings.operation != OperationType.BACKFILL:
            self.blockchain = BlockchainClient(settings.eth_rpc_url)
        else:
            self.blockchain = None
            
        self.clickhouse = ClickHouseManager(
            host=settings.clickhouse_host,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            port=settings.clickhouse_port,
            secure=settings.clickhouse_secure
        )
        self.state_manager = StateManager(self.clickhouse)
        
        # Clean up stale processing jobs on startup
        self._cleanup_on_startup()
        
        # Create directories
        os.makedirs(settings.data_dir, exist_ok=True)
        
        # Generate batch ID for this run
        self.batch_id = str(uuid.uuid4())[:8]
        
        # Track start time for progress reporting
        self.start_time = time.time()
        
        # Set up signal handling
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)
        
        self.running = True
        self.worker_pool = None
        
        # Progress tracking
        self.completed_ranges = []
        self.failed_ranges = []
        self.progress_lock = threading.Lock()
        self.last_progress_report = time.time()
        
        # Enhanced gap filling settings
        self.handle_failed_ranges = os.environ.get("HANDLE_FAILED_RANGES", "false").lower() == "true"
        self.delete_failed_before_retry = os.environ.get("DELETE_FAILED_BEFORE_RETRY", "false").lower() == "true"
        self.max_retries_override = int(os.environ.get("MAX_RETRIES_OVERRIDE", "0")) or settings.max_retries
    
    def _cleanup_on_startup(self):
        """Clean up any stale processing jobs from previous runs."""
        logger.info("Checking for stale processing jobs from previous runs...")
        
        # Reset all processing jobs to pending
        reset_count = self.state_manager.reset_all_processing_jobs()
        
        if reset_count > 0:
            logger.warning(
                f"Found and reset {reset_count} 'processing' jobs from previous run. "
                f"These will be retried."
            )
        else:
            logger.info("No stale processing jobs found.")
    
    def _handle_exit(self, sig, frame):
        """Handle shutdown gracefully."""
        logger.info("Shutdown signal received")
        self.running = False
        
        if self.worker_pool:
            self.worker_pool.stop()
            
        sys.exit(0)
    
    def run(self):
        """Run the indexer based on operation type."""
        if settings.operation == OperationType.CONTINUOUS:
            self._run_continuous()
        elif settings.operation == OperationType.HISTORICAL:
            self._run_historical()
        elif settings.operation == OperationType.FILL_GAPS:
            self._run_fill_gaps()
        elif settings.operation == OperationType.VALIDATE:
            self._run_validate()
        elif settings.operation == OperationType.BACKFILL:
            self._run_backfill()
        elif settings.operation == OperationType.FIX_TIMESTAMPS: 
            self._run_fix_timestamps()
        elif settings.operation == OperationType.CONSOLIDATE:
            self._run_consolidate()
        elif settings.operation == OperationType.PROCESS_FAILED:
            self._run_process_failed()
        else:
            logger.error(f"Unknown operation: {settings.operation}")
            sys.exit(1)
    
    def _run_continuous(self):
        """Run continuous indexing, following the chain tip."""
        logger.info("Starting continuous indexing")
        
        # Reset any stale jobs periodically
        last_stale_check = time.time()
        stale_check_interval = 300  # Check every 5 minutes
        
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
            mode=settings.mode.value,
            batch_id=self.batch_id
        )
        
        while self.running:
            try:
                # Periodically check for stale jobs
                if time.time() - last_stale_check > stale_check_interval:
                    self.state_manager.reset_stale_jobs(timeout_minutes=settings.stale_job_timeout_minutes)
                    last_stale_check = time.time()
                
                # Get latest block
                latest_block = self.blockchain.get_latest_block_number()
                safe_block = latest_block - settings.confirmation_blocks
                
                # Check if there's work to do
                if safe_block > last_block:
                    # Process in batches
                    batch_end = min(last_block + settings.batch_size, safe_block)
                    
                    logger.info(f"Processing blocks {last_block}-{batch_end}")
                    success = worker.process_range(
                        last_block, batch_end, settings.datasets
                    )
                    
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
        
        # Reset any stale jobs first
        self.state_manager.reset_stale_jobs(timeout_minutes=settings.stale_job_timeout_minutes)
        
        if settings.workers == 1:
            # Single worker mode
            self._run_historical_single()
        else:
            # Multi-worker mode with thread pool
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
            mode=settings.mode.value,
            batch_id=self.batch_id
        )
        
        current = settings.start_block
        while current < settings.end_block and self.running:
            batch_end = min(current + settings.batch_size, settings.end_block)
            
            logger.info(f"Processing blocks {current}-{batch_end}")
            success = worker.process_range(current, batch_end, settings.datasets, force=True)
            
            if success:
                current = batch_end
                self._update_progress(current - settings.start_block, settings.end_block - settings.start_block)
            else:
                logger.error(f"Failed to process blocks {current}-{batch_end}")
                # Check if we should retry
                stale_jobs = self.state_manager.get_stale_jobs()
                failed_job = next((j for j in stale_jobs if j.start_block == current and j.end_block == batch_end), None)
                
                if failed_job and failed_job.attempt_count >= settings.max_retries:
                    logger.error(f"Max retries reached for {current}-{batch_end}, skipping")
                    current = batch_end
                else:
                    time.sleep(10)
        
        logger.info("Historical indexing complete")
    
    def _run_historical_parallel(self):
        """Run historical indexing with multiple workers using thread pool."""
        logger.info(f"Starting parallel historical indexing with {settings.workers} workers")
        
        # Create work ranges
        ranges = []
        current = settings.start_block
        while current < settings.end_block:
            batch_end = min(current + settings.batch_size, settings.end_block)
            ranges.append((current, batch_end))
            current = batch_end
        
        total_blocks = settings.end_block - settings.start_block
        logger.info(f"Created {len(ranges)} work items for {total_blocks} blocks")
        
        # Create worker pool configuration
        pool_config = {
            'eth_rpc_url': settings.eth_rpc_url,
            'network_name': settings.network_name,
            'data_dir': settings.data_dir,
            'clickhouse_host': settings.clickhouse_host,
            'clickhouse_user': settings.clickhouse_user,
            'clickhouse_password': settings.clickhouse_password,
            'clickhouse_database': settings.clickhouse_database,
            'clickhouse_port': settings.clickhouse_port,
            'clickhouse_secure': settings.clickhouse_secure,
            'mode': settings.mode.value,
            'batch_id': self.batch_id
        }
        
        # Create and start worker pool
        self.worker_pool = WorkerPool(settings.workers, pool_config)
        self.worker_pool.start(result_callback=self._handle_work_result)
        
        # Progress monitoring thread
        progress_thread = threading.Thread(target=self._monitor_progress, args=(len(ranges),))
        progress_thread.daemon = True
        progress_thread.start()
        
        # Submit work to pool
        submitted = 0
        for start, end in ranges:
            while not self.worker_pool.submit(start, end, settings.datasets, block=False):
                if not self.running:
                    break
                time.sleep(0.1)  # Queue is full, wait a bit
                
            submitted += 1
            
            if not self.running:
                break
        
        logger.info(f"Submitted {submitted}/{len(ranges)} work items")
        
        # Wait for completion
        self.worker_pool.wait_for_completion()
        
        # Stop the pool
        self.worker_pool.stop()
        
        # Final report
        with self.progress_lock:
            completed = len(self.completed_ranges)
            failed = len(self.failed_ranges)
        
        logger.info(f"Historical indexing complete. ✓ Completed: {completed}, ✗ Failed: {failed}")
        
        if failed > 0:
            logger.error(f"Failed ranges: {self.failed_ranges[:10]}...")  # Show first 10
            sys.exit(1)
    
    def _handle_work_result(self, result: WorkResult):
        """Handle result from a worker."""
        with self.progress_lock:
            if result.success:
                self.completed_ranges.append((result.start_block, result.end_block))
            else:
                self.failed_ranges.append((result.start_block, result.end_block))
                if result.error:
                    logger.error(f"Range {result.start_block}-{result.end_block} failed: {result.error}")
    
    def _monitor_progress(self, total_ranges: int):
        """Monitor and report progress."""
        while self.running and self.worker_pool:
            time.sleep(5)  # Report every 5 seconds
            
            with self.progress_lock:
                completed = len(self.completed_ranges)
                failed = len(self.failed_ranges)
            
            stats = self.worker_pool.get_stats()
            
            # Calculate progress
            progress_pct = (completed / total_ranges * 100) if total_ranges > 0 else 0
            
            # Calculate rate
            elapsed = time.time() - self.start_time
            if elapsed > 0 and completed > 0:
                rate = completed / elapsed * 60  # ranges per minute
                eta = (total_ranges - completed) / rate if rate > 0 else 0
            else:
                rate = 0
                eta = 0
            
            # Log progress
            logger.info(
                f"Progress: {completed}/{total_ranges} ({progress_pct:.1f}%) | "
                f"Active: {stats['active']} | Pending: {stats['pending']} | "
                f"Failed: {failed} | Rate: {rate:.1f} ranges/min | "
                f"ETA: {eta:.1f} min"
            )
            
            # Log detailed stats every 30 seconds
            if time.time() - self.last_progress_report > 30:
                logger.info(f"Pool stats: {stats}")
                self.last_progress_report = time.time()
    
    def _update_progress(self, completed_blocks: int, total_blocks: int):
        """Update progress for single-threaded mode."""
        progress_pct = (completed_blocks / total_blocks * 100) if total_blocks > 0 else 0
        elapsed = time.time() - self.start_time
        
        if elapsed > 0 and completed_blocks > 0:
            rate = completed_blocks / elapsed  # blocks per second
            eta = (total_blocks - completed_blocks) / rate if rate > 0 else 0
        else:
            rate = 0
            eta = 0
        
        logger.info(
            f"Progress: {completed_blocks}/{total_blocks} blocks ({progress_pct:.1f}%) | "
            f"Rate: {rate:.1f} blocks/sec | ETA: {eta/60:.1f} min"
        )
    
    def _run_fill_gaps(self):
        """Find and fill gaps in the indexed data, including failed ranges."""
        logger.info("Starting gap fill operation")
        
        # Reset any stale jobs first
        self.state_manager.reset_stale_jobs(timeout_minutes=settings.stale_job_timeout_minutes)
        
        # Determine range
        start = settings.start_block or 0
        end = settings.end_block or self.blockchain.get_latest_block_number()
        
        logger.info(f"Searching for gaps between blocks {start} and {end}")
        
        # Find all types of gaps for each dataset
        all_gaps = []
        
        for dataset in settings.datasets:
            # 1. Find missing ranges (standard gaps)
            gaps = self.state_manager.find_gaps(
                settings.mode.value, dataset, start, end, min_gap_size=1
            )
            logger.info(f"Found {len(gaps)} missing ranges in {dataset}")
            
            # 2. Find failed ranges if enabled
            failed_ranges = []
            if self.handle_failed_ranges:
                failed_ranges = self.state_manager.get_failed_ranges(
                    settings.mode.value, dataset, start, end, max_attempts=self.max_retries_override
                )
                logger.info(f"Found {len(failed_ranges)} failed ranges in {dataset}")
                
                # Delete failed entries if requested
                if self.delete_failed_before_retry and failed_ranges:
                    ranges_to_delete = [(r[0], r[1]) for r in failed_ranges]
                    deleted = self.state_manager.delete_range_entries(
                        settings.mode.value, dataset, ranges_to_delete
                    )
                    logger.info(f"Deleted {deleted} failed entries for {dataset}")
                
                # Add failed ranges to gaps (just start and end blocks)
                for start_block, end_block, attempts, error in failed_ranges:
                    gaps.append((start_block, end_block))
            
            # Add all gaps for this dataset
            all_gaps.extend([(gap[0], gap[1], dataset) for gap in gaps])
        
        if not all_gaps:
            logger.info("No gaps found!")
            return
        
        # Deduplicate gaps (same range for multiple datasets)
        unique_ranges = {}
        for start, end, dataset in all_gaps:
            key = (start, end)
            if key not in unique_ranges:
                unique_ranges[key] = []
            unique_ranges[key].append(dataset)
        
        logger.info(f"Found {len(unique_ranges)} unique gap ranges to fill")
        
        # Process gaps using the same logic as historical
        if settings.workers == 1:
            self._fill_gaps_single(unique_ranges)
        else:
            self._fill_gaps_parallel(unique_ranges)
    
    def _fill_gaps_single(self, gaps: Dict[Tuple[int, int], List[str]]):
        """Fill gaps with a single worker."""
        worker = IndexerWorker(
            worker_id="gap_fill",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url,
            mode=settings.mode.value,
            batch_id=self.batch_id
        )
        
        filled = 0
        failed = 0
        
        for (start, end), datasets in gaps.items():
            logger.info(f"Filling gap {start}-{end} for datasets: {datasets}")
            success = worker.process_range(start, end, datasets)
            
            if success:
                filled += 1
            else:
                failed += 1
        
        logger.info(f"Gap fill complete. Filled: {filled}, Failed: {failed}")
    
    def _fill_gaps_parallel(self, gaps: Dict[Tuple[int, int], List[str]]):
        """Fill gaps with multiple workers using thread pool."""
        logger.info(f"Starting parallel gap fill with {settings.workers} workers")
        
        # Create worker pool configuration
        pool_config = {
            'eth_rpc_url': settings.eth_rpc_url,
            'network_name': settings.network_name,
            'data_dir': settings.data_dir,
            'clickhouse_host': settings.clickhouse_host,
            'clickhouse_user': settings.clickhouse_user,
            'clickhouse_password': settings.clickhouse_password,
            'clickhouse_database': settings.clickhouse_database,
            'clickhouse_port': settings.clickhouse_port,
            'clickhouse_secure': settings.clickhouse_secure,
            'mode': settings.mode.value,
            'batch_id': self.batch_id
        }
        
        # Create and start worker pool
        self.worker_pool = WorkerPool(settings.workers, pool_config)
        self.worker_pool.start(result_callback=self._handle_work_result)
        
        # Submit gaps to pool
        for (start, end), datasets in gaps.items():
            self.worker_pool.submit(start, end, datasets)
        
        # Wait for completion
        self.worker_pool.wait_for_completion()
        
        # Stop the pool
        self.worker_pool.stop()
        
        # Report results
        with self.progress_lock:
            filled = len(self.completed_ranges)
            failed = len(self.failed_ranges)
        
        logger.info(f"Gap fill complete. Filled: {filled}, Failed: {failed}")
    
    def _run_backfill(self):
        """Run backfill operation to populate indexing_state from existing data."""
        logger.info("Starting backfill operation")
        
        # Create backfill worker
        backfill_worker = BackfillWorker(
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            mode=settings.mode.value
        )
        
        # Determine block range
        start_block = settings.start_block if settings.start_block > 0 else None
        end_block = settings.end_block if settings.end_block > 0 else None
        
        if start_block is not None and end_block is not None:
            logger.info(f"Backfilling block range {start_block} to {end_block}")
        else:
            logger.info("Backfilling entire dataset")
        
        # Check if parallel mode is requested
        if settings.workers > 1 and len(settings.datasets) > 1:
            logger.info(f"Running parallel backfill with {settings.workers} workers for {len(settings.datasets)} datasets")
            results = backfill_worker.backfill_datasets_parallel(
                datasets=settings.datasets,
                start_block=start_block,
                end_block=end_block,
                force=settings.backfill_force,
                max_workers=settings.workers
            )
        else:
            # Single-threaded mode
            logger.info("Running single-threaded backfill")
            results = backfill_worker.backfill_datasets(
                datasets=settings.datasets,
                start_block=start_block,
                end_block=end_block,
                force=settings.backfill_force
            )
        
        # Validate results
        success = backfill_worker.validate_backfill(settings.datasets)
        
        if success:
            logger.info("✓ Backfill completed successfully")
            sys.exit(0)
        else:
            logger.error("✗ Backfill validation failed")
            sys.exit(1)
    
    def _run_validate(self):
        """Validate the indexed data."""
        logger.info("Starting validation")
        
        # Get progress summary
        summary = self.state_manager.get_progress_summary(settings.mode.value)
        
        print("\n=== INDEXING PROGRESS ===\n")
        for dataset, stats in summary.items():
            print(f"{dataset}:")
            print(f"  Completed ranges: {stats.get('completed_ranges', 0)}")
            print(f"  Processing ranges: {stats.get('processing_ranges', 0)}")
            print(f"  Failed ranges: {stats.get('failed_ranges', 0)}")
            print(f"  Pending ranges: {stats.get('pending_ranges', 0)}")
            print(f"  Highest block: {stats.get('highest_block', 0)}")
            print(f"  Total rows: {stats.get('total_rows_indexed', 0):,}")
            print()
        
        # Find gaps
        start = settings.start_block or 0
        end = settings.end_block or self.blockchain.get_latest_block_number()
        
        print(f"\n=== CHECKING FOR GAPS ({start} to {end}) ===\n")
        
        has_gaps = False
        has_failed = False
        for dataset in settings.datasets:
            # Check for missing ranges
            gaps = self.state_manager.find_gaps(
                settings.mode.value, dataset, start, end
            )
            
            # Check for failed ranges
            failed_ranges = self.state_manager.get_failed_ranges(
                settings.mode.value, dataset, start, end
            )
            
            if gaps or failed_ranges:
                has_gaps = True if gaps else has_gaps
                has_failed = True if failed_ranges else has_failed
                
                print(f"{dataset}:")
                
                if gaps:
                    print(f"  {len(gaps)} missing ranges")
                    for i, (gap_start, gap_end) in enumerate(gaps[:5]):
                        print(f"    Missing {i+1}: blocks {gap_start}-{gap_end} ({gap_end - gap_start} blocks)")
                    if len(gaps) > 5:
                        print(f"    ... and {len(gaps) - 5} more")
                
                if failed_ranges:
                    print(f"  {len(failed_ranges)} failed ranges")
                    for i, (start_block, end_block, attempts, error) in enumerate(failed_ranges[:5]):
                        print(f"    Failed {i+1}: blocks {start_block}-{end_block} (attempts: {attempts})")
                        if error:
                            print(f"      Error: {error[:100]}...")
                    if len(failed_ranges) > 5:
                        print(f"    ... and {len(failed_ranges) - 5} more")
            else:
                print(f"{dataset}: No gaps found ✓")
        
        # Check for stale jobs
        stale_jobs = self.state_manager.get_stale_jobs()
        if stale_jobs:
            print(f"\n=== STALE JOBS ===\n")
            print(f"Found {len(stale_jobs)} stale jobs:")
            for job in stale_jobs[:10]:
                print(f"  {job.dataset}: {job.start_block}-{job.end_block} (worker: {job.worker_id})")
        
        # Exit code based on validation results
        if has_gaps or has_failed or stale_jobs:
            sys.exit(1)
        else:
            print("\n✓ Validation passed!")
            sys.exit(0)

    def _run_fix_timestamps(self):
        """Fix incorrect timestamps in blockchain data tables."""
        logger.info("Starting timestamp fix operation")
        
        # Create timestamp fixer
        fixer = TimestampFixer(
            clickhouse=self.clickhouse,
            state_manager=self.state_manager
        )
        
        # Find affected datasets
        affected = fixer.find_affected_datasets()
        
        if not affected:
            logger.info("No datasets with timestamp issues found")
            print("\n✓ All timestamps are correct!")
            sys.exit(0)
        
        # Print affected datasets
        print("\n=== AFFECTED DATASETS ===\n")
        total_affected = 0
        for dataset, count in affected.items():
            print(f"{dataset}: {count:,} rows with incorrect timestamps")
            total_affected += count
        print(f"\nTotal affected rows: {total_affected:,}")
        
        # Fix timestamps
        print("\n=== STARTING FIX ===\n")
        results = fixer.fix_timestamps(list(affected.keys()))
        
        # Print summary
        fixer.print_fix_summary(results)
        
        # Verify fixes
        print("\n=== VERIFICATION ===\n")
        verification = fixer.verify_fixes(list(affected.keys()))
        
        all_clean = True
        for dataset, status in verification.items():
            if status.get('remaining_bad', 0) > 0:
                print(f"✗ {dataset}: Still has {status['remaining_bad']} bad timestamps")
                all_clean = False
            elif 'error' in status:
                print(f"✗ {dataset}: Error during verification: {status['error']}")
                all_clean = False
            else:
                print(f"✓ {dataset}: Clean")
        
        if all_clean:
            print("\n✓ All timestamps successfully fixed!")
            sys.exit(0)
        else:
            print("\n✗ Some timestamps could not be fixed")
            sys.exit(1)

    def _run_consolidate(self):
        """Consolidate fragmented ranges in indexing_state table."""
        logger.info("Starting range consolidation operation")
        
        # Import here to avoid circular imports
        from .consolidate_ranges import RangeConsolidator
        
        # Create consolidator
        consolidator = RangeConsolidator(
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            mode=settings.mode.value
        )
        
        # Run consolidation
        results = consolidator.consolidate_all_datasets(settings.datasets)
        
        # Check if any consolidation happened
        total_merged = sum(r.ranges_merged for r in results.values())
        
        if total_merged > 0:
            logger.info(f"✓ Consolidation completed successfully. Merged {total_merged} ranges.")
            sys.exit(0)
        else:
            logger.info("✓ No adjacent ranges found to consolidate.")
            sys.exit(0)

    def _run_process_failed(self):
        """Process ONLY failed ranges within safe boundaries, no gap detection."""
        logger.info("Starting failed range processing (no gap detection)")
        
        # Reset any stale jobs first
        self.state_manager.reset_stale_jobs(timeout_minutes=settings.stale_job_timeout_minutes)
        
        # Determine safe range based on what's been indexed
        start = settings.start_block or 0
        
        # If no end block specified, find the safe maximum
        if not settings.end_block:
            # Get the minimum of maximum completed blocks across all datasets
            safe_max_blocks = []
            for dataset in settings.datasets:
                max_block = self.state_manager.get_max_completed_block(
                    settings.mode.value, dataset
                )
                if max_block > 0:
                    safe_max_blocks.append(max_block)
            
            if safe_max_blocks:
                end = min(safe_max_blocks)
                logger.info(f"Using safe max block: {end}")
            else:
                logger.warning("No completed blocks found, nothing to process")
                return
        else:
            end = settings.end_block
        
        logger.info(f"Processing failed ranges between blocks {start} and {end}")
        
        # Collect all failed ranges
        all_failed_ranges = []
        
        for dataset in settings.datasets:
            # Get failed ranges ONLY (not gaps)
            failed_ranges = self.state_manager.get_failed_ranges(
                settings.mode.value, 
                dataset, 
                start, 
                end,
                max_attempts=settings.max_retries
            )
            
            if failed_ranges:
                logger.info(f"Found {len(failed_ranges)} failed ranges in {dataset}")
                for start_block, end_block, attempts, error in failed_ranges:
                    all_failed_ranges.append((start_block, end_block, dataset, attempts))
            else:
                logger.info(f"No failed ranges found in {dataset}")
        
        if not all_failed_ranges:
            logger.info("No failed ranges to process!")
            return
        
        # Sort by block range for better locality
        all_failed_ranges.sort(key=lambda x: (x[0], x[1]))
        
        logger.info(f"Total failed ranges to process: {len(all_failed_ranges)}")
        
        # Process failed ranges
        if settings.workers == 1:
            self._process_failed_single(all_failed_ranges)
        else:
            self._process_failed_parallel(all_failed_ranges)

    def _process_failed_single(self, failed_ranges: List[Tuple[int, int, str, int]]):
        """Process failed ranges with a single worker."""
        worker = IndexerWorker(
            worker_id="failed_processor",
            blockchain=self.blockchain,
            clickhouse=self.clickhouse,
            state_manager=self.state_manager,
            data_dir=settings.data_dir,
            network_name=settings.network_name,
            rpc_url=settings.eth_rpc_url,
            mode=settings.mode.value,
            batch_id=self.batch_id
        )
        
        processed = 0
        success_count = 0
        
        for start_block, end_block, dataset, attempts in failed_ranges:
            logger.info(
                f"Processing failed range {dataset} {start_block}-{end_block} "
                f"(attempt {attempts + 1})"
            )
            
            # Process with force=True to override status checks
            success = worker.process_range(
                start_block, 
                end_block, 
                [dataset], 
                force=True
            )
            
            if success:
                success_count += 1
                logger.info(f"✓ Successfully processed {dataset} {start_block}-{end_block}")
            else:
                logger.error(f"✗ Failed to process {dataset} {start_block}-{end_block}")
            
            processed += 1
            
            # Progress report
            if processed % 10 == 0:
                logger.info(
                    f"Progress: {processed}/{len(failed_ranges)} "
                    f"({success_count} successful)"
                )
        
        logger.info(
            f"Failed range processing complete. "
            f"Processed: {processed}, Successful: {success_count}"
        )

    def _process_failed_parallel(self, failed_ranges: List[Tuple[int, int, str, int]]):
        """Process failed ranges with multiple workers."""
        # Group by (start_block, end_block) to process all datasets for a range together
        range_map = {}
        for start_block, end_block, dataset, attempts in failed_ranges:
            key = (start_block, end_block)
            if key not in range_map:
                range_map[key] = []
            range_map[key].append(dataset)
        
        logger.info(f"Processing {len(range_map)} unique ranges with {settings.workers} workers")
        
        # Create worker pool configuration
        pool_config = {
            'eth_rpc_url': settings.eth_rpc_url,
            'network_name': settings.network_name,
            'data_dir': settings.data_dir,
            'clickhouse_host': settings.clickhouse_host,
            'clickhouse_user': settings.clickhouse_user,
            'clickhouse_password': settings.clickhouse_password,
            'clickhouse_database': settings.clickhouse_database,
            'clickhouse_port': settings.clickhouse_port,
            'clickhouse_secure': settings.clickhouse_secure,
            'mode': settings.mode.value,
            'batch_id': self.batch_id
        }
        
        # Create and start worker pool
        self.worker_pool = WorkerPool(settings.workers, pool_config)
        self.worker_pool.start(result_callback=self._handle_work_result)
        
        # Progress monitoring thread
        progress_thread = threading.Thread(
            target=self._monitor_progress, 
            args=(len(range_map),)
        )
        progress_thread.daemon = True
        progress_thread.start()
        
        # Submit work
        for (start_block, end_block), datasets in range_map.items():
            self.worker_pool.submit(start_block, end_block, datasets)
        
        # Wait for completion
        self.worker_pool.wait_for_completion()
        
        # Stop the pool
        self.worker_pool.stop()
        
        # Report results
        with self.progress_lock:
            logger.info(
                f"Failed range processing complete. "
                f"✓ Successful: {len(self.completed_ranges)}, "
                f"✗ Failed: {len(self.failed_ranges)}"
            )


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()