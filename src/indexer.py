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
        else:
            logger.error(f"Unknown operation: {settings.operation}")
            sys.exit(1)
    
    def _run_continuous(self):
        """Run continuous indexing, following the chain tip."""
        logger.info("Starting continuous indexing")
        
        # Reset any stale jobs
        self.state_manager.reset_stale_jobs(timeout_minutes=30)
        
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
        
        # Reset any stale jobs
        self.state_manager.reset_stale_jobs(timeout_minutes=30)
        
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
        """Find and fill gaps in the indexed data."""
        logger.info("Starting gap fill operation")
        
        # Determine range
        start = settings.start_block or 0
        end = settings.end_block or self.blockchain.get_latest_block_number()
        
        logger.info(f"Searching for gaps between blocks {start} and {end}")
        
        # Find gaps for each dataset
        all_gaps = []
        for dataset in settings.datasets:
            gaps = self.state_manager.find_gaps(
                settings.mode.value, dataset, start, end, min_gap_size=1
            )
            logger.info(f"Found {len(gaps)} gaps in {dataset}")
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
            print(f"  Highest block: {stats.get('highest_completed_block', 0)}")
            print(f"  Total rows: {stats.get('total_rows_indexed', 0):,}")
            print()
        
        # Find gaps
        start = settings.start_block or 0
        end = settings.end_block or self.blockchain.get_latest_block_number()
        
        print(f"\n=== CHECKING FOR GAPS ({start} to {end}) ===\n")
        
        has_gaps = False
        for dataset in settings.datasets:
            gaps = self.state_manager.find_gaps(
                settings.mode.value, dataset, start, end
            )
            
            if gaps:
                has_gaps = True
                print(f"{dataset}: {len(gaps)} gaps found")
                for i, (gap_start, gap_end) in enumerate(gaps[:10]):
                    print(f"  Gap {i+1}: blocks {gap_start}-{gap_end} ({gap_end - gap_start} blocks)")
                if len(gaps) > 10:
                    print(f"  ... and {len(gaps) - 10} more gaps")
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
        if has_gaps or stale_jobs:
            sys.exit(1)
        else:
            print("\n✓ Validation passed!")
            sys.exit(0)


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()