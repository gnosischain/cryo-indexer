import os
import time
import multiprocessing
from multiprocessing import Queue, Process
import queue
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
import signal
import sys
import random  # For RPC endpoint selection

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .worker import IndexerWorker
from .utils import setup_logging, load_state, save_state


# Message types for worker-coordinator communication
WORKER_READY = "READY"
WORKER_COMPLETED = "COMPLETED"
WORKER_FAILED = "FAILED"


# Multiple RPC endpoints for better distribution
RPC_ENDPOINTS = [
    settings.eth_rpc_url,  # Primary endpoint from settings
]

# Add backup endpoints if configured
BACKUP_RPC_ENDPOINTS = os.environ.get("BACKUP_RPC_ENDPOINTS", "").split(",")
for endpoint in BACKUP_RPC_ENDPOINTS:
    if endpoint.strip():
        RPC_ENDPOINTS.append(endpoint.strip())


def get_rpc_endpoint(worker_id):
    """Get an RPC endpoint, distributing load across available endpoints."""
    if len(RPC_ENDPOINTS) == 1:
        return RPC_ENDPOINTS[0]
    
    # Distribute workers across endpoints
    index = worker_id % len(RPC_ENDPOINTS)
    return RPC_ENDPOINTS[index]


class MultiprocessIndexer:
    """
    Multiprocess indexer that uses separate Python processes for true parallelism.
    Uses a simple queue-based approach for coordination rather than file-based state.
    """
    
    def __init__(self):
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
        # Get the current indexer mode
        current_mode = settings.get_current_mode()
        logger.info(f"Starting multiprocess indexer in mode: {current_mode.name} with {settings.parallel_workers} processes")
        logger.info(f"Datasets to index: {current_mode.datasets}")
        logger.info(f"Using {len(RPC_ENDPOINTS)} RPC endpoints for load distribution")
        
        # Performance tuning
        self.worker_batch_size = int(os.environ.get("WORKER_BATCH_SIZE", str(settings.worker_batch_size)))
        self.max_queue_size = max(settings.parallel_workers * 3, 20)  # Buffer tasks for workers
        self.prefetch_blocks = int(os.environ.get("PREFETCH_BLOCKS", "5000"))  # How far ahead to queue work
        
        logger.info(f"Performance settings: batch_size={self.worker_batch_size}, "
                    f"prefetch={self.prefetch_blocks}, queue_size={self.max_queue_size}")
        
        # Initialize blockchain client (for main process only)
        self.blockchain = BlockchainClient(settings.eth_rpc_url)
        
        # Create ClickHouse manager for main process
        self.clickhouse = ClickHouseManager(
            host=settings.clickhouse_host,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            port=settings.clickhouse_port,
            secure=settings.clickhouse_secure
        )
        
        # Create necessary directories
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(settings.state_dir, exist_ok=True)
        
        # Get the highest block from the database to resume from
        highest_block = self.clickhouse.get_latest_processed_block()
        logger.info(f"Highest block in database: {highest_block}")
        
        # Use local state file only for main process restart capability
        self.state_file = os.path.join(settings.state_dir, f"indexer_state_{current_mode.name}.json")
        self.state = load_state(settings.state_dir, state_file=os.path.basename(self.state_file))
        
        # Update last_block to be the max of state file and database
        self.state['last_block'] = max(self.state.get('last_block', 0), highest_block)
        save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
        
        logger.info(f"Resuming from block {self.state['last_block']}")
        
        # Set up multiprocessing communication with larger queue sizes
        self.task_queue = Queue(self.max_queue_size)  # Tasks for workers
        self.result_queue = Queue(self.max_queue_size)  # Results from workers
        
        # Set up signal handling
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)
        
        # Process management
        self.workers = []
        self.next_worker_id = 0
        self.available_workers = 0  # Track number of idle workers
        
        # In-memory state tracking
        self.active_ranges = []
        self.completed_ranges = []
        self.failed_ranges = []
        self.next_block = self.state.get('last_block', 0)
        
        # Stats tracking
        self.start_time = time.time()
        self.total_blocks_processed = 0
        self.blocks_since_last_report = 0
        self.last_report_time = time.time()
        self.report_interval = 10  # Report throughput every 10 seconds
    
    def _handle_exit(self, sig, frame):
        """Handle exit signals gracefully."""
        logger.info("Shutdown signal received, terminating worker processes...")
        
        # Send termination message to all workers
        for _ in range(len(self.workers)):
            self.task_queue.put(None)  # None is the termination signal
        
        # Wait for workers to exit
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=2)
                if worker.is_alive():
                    worker.terminate()
        
        # Save final state
        self.state['last_block'] = self.next_block
        save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
        
        # Log information about failed block ranges
        if self.failed_ranges:
            logger.error(f"There are {len(self.failed_ranges)} permanently failed block ranges")
            logger.error(f"Permanent failures: {self.failed_ranges}")
        
        # Report performance stats
        self._report_throughput(final=True)
        
        logger.info("Shutdown complete")
        sys.exit(0)
    
    def _worker_process(self, worker_id, task_queue, result_queue):
        """Worker process that processes blocks from the task queue."""
        try:
            # Configure process-specific logger
            logger.remove()  # Remove existing handlers
            logger.add(
                lambda msg: print(f"[Worker {worker_id}] {msg}", end="\n"),
                level=settings.log_level
            )
            logger.add(
                os.path.join(settings.log_dir, f"worker_{worker_id}.log"),
                rotation="10 MB",
                level=settings.log_level
            )
            
            logger.info(f"Worker {worker_id} started")
            
            # Get RPC endpoint for this worker for better load distribution
            rpc_url = get_rpc_endpoint(worker_id)
            logger.info(f"Worker {worker_id} using RPC endpoint: {rpc_url}")
            
            # Create data directory for this worker
            worker_data_dir = os.path.join(settings.data_dir, f"worker_{worker_id}")
            os.makedirs(worker_data_dir, exist_ok=True)
            
            # Create own blockchain client and database connection
            blockchain = BlockchainClient(rpc_url)
            clickhouse = ClickHouseManager(
                host=settings.clickhouse_host,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
                port=settings.clickhouse_port,
                secure=settings.clickhouse_secure
            )
            
            # Create worker instance
            worker = IndexerWorker(
                worker_id=str(worker_id),
                blockchain=blockchain,
                clickhouse=clickhouse,
                data_dir=worker_data_dir,
                network_name=settings.network_name,
                rpc_url=rpc_url
            )
            
            # Signal that we're ready for work
            logger.info(f"Worker {worker_id} is ready for tasks")
            result_queue.put((WORKER_READY, worker_id, None))
            
            task_count = 0
            
            # Process tasks until told to exit
            while True:
                try:
                    # Block with timeout to avoid CPU spinning
                    task = task_queue.get(timeout=60)
                    
                    # None is the signal to exit
                    if task is None:
                        logger.info(f"Worker {worker_id} received exit signal")
                        break
                    
                    start_block, end_block, datasets, retry_count = task
                    task_count += 1
                    
                    if retry_count > 0:
                        logger.info(f"Worker {worker_id} retrying blocks {start_block}-{end_block} (attempt {retry_count+1})")
                        # Adjust rate limits for retries
                        os.environ["WORKER_CRYO_REQUESTS_PER_SECOND"] = str(max(50, 500 // (retry_count + 1)))
                        os.environ["WORKER_CRYO_MAX_CONCURRENT"] = str(max(1, 5 // (retry_count + 1)))
                        os.environ["WORKER_CRYO_TIMEOUT"] = str(300 + (retry_count * 60))
                    else:
                        logger.info(f"Worker {worker_id} processing blocks {start_block}-{end_block} (task #{task_count})")
                    
                    # Process the block range
                    success = worker.process_block_range(start_block, end_block, datasets)
                    
                    if success:
                        logger.info(f"Worker {worker_id} completed blocks {start_block}-{end_block}")
                        result_queue.put((WORKER_COMPLETED, worker_id, (start_block, end_block)))
                    else:
                        logger.error(f"Worker {worker_id} failed to process blocks {start_block}-{end_block}")
                        result_queue.put((WORKER_FAILED, worker_id, (start_block, end_block, retry_count)))
                    
                    # Signal that we're ready for more work
                    logger.info(f"Worker {worker_id} is ready for more tasks")
                    result_queue.put((WORKER_READY, worker_id, None))
                    
                except queue.Empty:
                    # Timeout waiting for a task, just continue
                    continue
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}", exc_info=True)
                    # Try to recover by signaling ready again
                    try:
                        result_queue.put((WORKER_READY, worker_id, None))
                    except:
                        pass
                    time.sleep(5)  # Brief pause before continuing
        
        except Exception as e:
            logger.error(f"Worker {worker_id} process crashed: {e}", exc_info=True)
    
    def _start_workers(self):
        """Start the worker processes."""
        for _ in range(settings.parallel_workers):
            worker_id = self.next_worker_id
            self.next_worker_id += 1
            
            p = Process(
                target=self._worker_process,
                args=(worker_id, self.task_queue, self.result_queue)
            )
            p.daemon = True
            p.start()
            self.workers.append(p)
            logger.info(f"Started worker {worker_id}")
    
    def _check_worker_results(self):
        """Check for any results from workers."""
        # Process all available results without waiting
        results_processed = 0
        blocks_completed = 0
        
        try:
            while not self.result_queue.empty():
                result = self.result_queue.get(block=False)
                results_processed += 1
                
                if not isinstance(result, tuple) or len(result) != 3:
                    logger.error(f"Malformed worker result: {result}")
                    continue
                    
                message_type, worker_id, data = result
                
                if message_type == WORKER_READY:
                    # Worker is ready for a new task
                    self.available_workers += 1
                    logger.debug(f"Worker {worker_id} is available. Total available: {self.available_workers}")
                
                elif message_type == WORKER_COMPLETED:
                    start_block, end_block = data
                    
                    # Calculate number of blocks completed
                    blocks_in_range = end_block - start_block
                    blocks_completed += blocks_in_range
                    self.total_blocks_processed += blocks_in_range
                    self.blocks_since_last_report += blocks_in_range
                    
                    # Remove from active ranges
                    if [start_block, end_block] in self.active_ranges:
                        self.active_ranges.remove([start_block, end_block])
                    
                    # Add to completed ranges
                    self.completed_ranges.append([start_block, end_block])
                    logger.info(f"Block range {start_block}-{end_block} marked as completed ({blocks_in_range} blocks)")
                    
                    # Update last_block if this range extends it
                    if start_block <= self.next_block and end_block > self.next_block:
                        self.next_block = end_block
                        self.state['last_block'] = self.next_block
                        save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                        logger.info(f"Updated next_block to {self.next_block}")
                
                elif message_type == WORKER_FAILED:
                    start_block, end_block, retry_count = data
                    
                    # Remove from active ranges
                    if [start_block, end_block] in self.active_ranges:
                        self.active_ranges.remove([start_block, end_block])
                    
                    # Check if we've exceeded retry limit
                    if retry_count >= 2:  # Max 3 attempts (0, 1, 2)
                        logger.error(f"Block range {start_block}-{end_block} failed permanently after 3 attempts")
                        self.failed_ranges.append([start_block, end_block])
                    else:
                        # Queue for retry with incremented retry count
                        logger.warning(f"Scheduling retry for block range {start_block}-{end_block} (attempt {retry_count+2}/3)")
                        self.task_queue.put((start_block, end_block, settings.get_current_mode().datasets, retry_count + 1))
                else:
                    logger.warning(f"Unknown message type: {message_type}")
        
        except Exception as e:
            logger.error(f"Error processing worker results: {str(e)}", exc_info=True)
        
        if results_processed > 0:
            logger.debug(f"Processed {results_processed} worker messages, {blocks_completed} blocks completed")
            
        # Report throughput periodically
        self._report_throughput()
    
    def _report_throughput(self, final=False):
        """Report throughput statistics."""
        current_time = time.time()
        elapsed = current_time - self.last_report_time
        
        if elapsed >= self.report_interval or final:
            # Calculate blocks per second
            if elapsed > 0:
                blocks_per_second = self.blocks_since_last_report / elapsed
            else:
                blocks_per_second = 0
                
            # Calculate overall average
            total_elapsed = current_time - self.start_time
            if total_elapsed > 0:
                overall_blocks_per_second = self.total_blocks_processed / total_elapsed
            else:
                overall_blocks_per_second = 0
            
            logger.info(f"Performance: {blocks_per_second:.2f} blocks/sec current, "
                        f"{overall_blocks_per_second:.2f} blocks/sec average, "
                        f"{self.total_blocks_processed} blocks total")
            
            # Reset counters
            self.last_report_time = current_time
            self.blocks_since_last_report = 0
    
    def _assign_work(self, safe_block):
        """Assign work to idle workers if available."""
        # Skip if no workers are available
        if self.available_workers <= 0:
            return
        
        # Skip if task queue is nearly full
        if self.task_queue.qsize() > self.max_queue_size * 0.8:
            logger.debug(f"Task queue nearly full ({self.task_queue.qsize()}/{self.max_queue_size}), waiting for workers to catch up")
            return
        
        tasks_assigned = 0
        prefetch_limit = self.next_block + self.prefetch_blocks
        
        # Cap to safe block
        prefetch_limit = min(prefetch_limit, safe_block)
        
        # Pre-fill task queue with work up to prefetch limit
        while self.next_block < prefetch_limit and tasks_assigned < self.max_queue_size:
            # Calculate end block (limit batch size for better distribution)
            end_block = min(self.next_block + self.worker_batch_size, prefetch_limit)
            
            # Skip ranges that overlap with active or permanently failed ranges
            skip = False
            
            # Check active ranges
            for start, end in self.active_ranges:
                if (start <= self.next_block < end) or (start < end_block <= end) or (self.next_block <= start < end_block):
                    logger.debug(f"Skipping overlapping active range: {start}-{end}")
                    self.next_block = max(end, self.next_block)
                    skip = True
                    break
            
            if skip:
                continue
            
            # Check permanently failed ranges
            for start, end in self.failed_ranges:
                if (start <= self.next_block < end) or (start < end_block <= end) or (self.next_block <= start < end_block):
                    logger.warning(f"Skipping permanently failed range: {start}-{end}")
                    self.next_block = max(end, self.next_block)
                    skip = True
                    break
            
            if skip:
                continue
            
            # Assign the range
            logger.info(f"Queuing block range {self.next_block}-{end_block} for processing ({end_block - self.next_block} blocks)")
            self.active_ranges.append([self.next_block, end_block])
            self.task_queue.put((self.next_block, end_block, settings.get_current_mode().datasets, 0))
            tasks_assigned += 1
            
            # Update next_block
            self.next_block = end_block
            
            # Decrement available workers (but don't go below zero)
            self.available_workers = max(0, self.available_workers - 1)
        
        if tasks_assigned > 0:
            logger.info(f"Assigned {tasks_assigned} tasks to queue, {self.task_queue.qsize()}/{self.max_queue_size} queued")
    
    def _log_status(self):
        """Log current status of the indexer."""
        queue_size = self.task_queue.qsize()
        logger.info(f"Status: Current block: {self.next_block}, "
                   f"Active ranges: {len(self.active_ranges)}, "
                   f"Available workers: {self.available_workers}, "
                   f"Task queue: {queue_size}/{self.max_queue_size}, "
                   f"Completed ranges: {len(self.completed_ranges)}, "
                   f"Failed ranges: {len(self.failed_ranges)}")
        
        # Report throughput
        self._report_throughput()
    
    def _check_worker_health(self):
        """Check if all worker processes are still alive."""
        for i, worker in enumerate(self.workers):
            if not worker.is_alive():
                logger.error(f"Worker process {i} has died. Restarting...")
                
                # Create a new worker
                worker_id = self.next_worker_id
                self.next_worker_id += 1
                
                p = Process(
                    target=self._worker_process,
                    args=(worker_id, self.task_queue, self.result_queue)
                )
                p.daemon = True
                p.start()
                
                # Replace the dead worker
                self.workers[i] = p
    
    def run(self):
        """Start the multiprocess indexer and manage worker processes."""
        logger.info("Starting multiprocess indexer...")
        
        current_mode = settings.get_current_mode()
        historical_mode = settings.end_block > 0
        
        # Start worker processes
        self._start_workers()
        
        # Give workers time to initialize
        logger.info("Waiting for workers to initialize...")
        time.sleep(2)
        
        # Status logging timer
        last_status_time = time.time()
        status_interval = 30  # Log status every 30 seconds
        
        # Set start time for throughput calculations
        self.start_time = time.time()
        self.last_report_time = self.start_time
        
        try:
            while True:
                # Check worker health
                self._check_worker_health()
                
                # Get latest block from blockchain
                latest_block = self.blockchain.get_latest_block_number()
                safe_block = latest_block - settings.confirmation_blocks
                
                if historical_mode:
                    safe_block = min(safe_block, settings.end_block)
                
                # Process results from workers
                self._check_worker_results()
                
                # Pre-fill task queue with work
                self._assign_work(safe_block)
                
                # Periodically log status
                current_time = time.time()
                if current_time - last_status_time > status_interval:
                    self._log_status()
                    last_status_time = current_time
                
                # Check if we're done in historical mode
                if historical_mode and self.next_block >= settings.end_block and len(self.active_ranges) == 0 and self.task_queue.empty():
                    logger.info(f"Reached end block {settings.end_block}. Indexing complete.")
                    
                    # Send termination signal to all workers
                    for _ in range(len(self.workers)):
                        self.task_queue.put(None)
                    
                    # Wait for workers to exit
                    for worker in self.workers:
                        worker.join(timeout=5)
                    
                    self._log_status()
                    # Report final performance statistics
                    self._report_throughput(final=True)
                    return
                
                # Avoid CPU spinning - very short sleep
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            self._handle_exit(None, None)
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            self._handle_exit(None, None)
            raise