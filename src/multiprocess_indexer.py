import os
import time
import json
import multiprocessing
from contextlib import contextmanager
import fcntl
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
from datetime import datetime
import signal
import sys
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .worker import IndexerWorker
from .utils import setup_logging, load_state, save_state


@contextmanager
def file_lock(file_path):
    """Context manager for file locking to prevent race conditions."""
    lock_file = f"{file_path}.lock"
    lock_fd = open(lock_file, 'w+')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
        if os.path.exists(lock_file):
            try:
                os.remove(lock_file)
            except:
                pass


def read_shared_state(file_path):
    """Read the shared state file with locking to prevent race conditions."""
    with file_lock(file_path):
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            return {
                "active_ranges": [],
                "completed_ranges": [],
                "last_block": 0,
                "next_block": 0
            }


def write_shared_state(file_path, state):
    """Write the shared state file with locking to prevent race conditions."""
    with file_lock(file_path):
        with open(file_path, 'w') as f:
            json.dump(state, f, indent=2)


class ProcessingError(Exception):
    """Exception raised when block processing fails."""
    pass


class MultiprocessIndexer:
    """
    Multiprocess indexer that uses separate Python processes for true parallelism.
    Much simpler than thread-based solutions and guarantees parallel RPC calls.
    """
    
    def __init__(self):
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
        # Get the current indexer mode
        current_mode = settings.get_current_mode()
        logger.info(f"Starting multiprocess indexer in mode: {current_mode.name} with {settings.parallel_workers} processes")
        logger.info(f"Datasets to index: {current_mode.datasets}")
        
        # Initialize blockchain client (for main process only)
        self.blockchain = BlockchainClient(settings.eth_rpc_url)
        
        # Create necessary directories
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(settings.state_dir, exist_ok=True)
        
        # Use a state file specific to this mode
        self.state_file = os.path.join(settings.state_dir, f"indexer_state_{current_mode.name}.json")
        logger.info(f"Using state file: {self.state_file}")
        
        # Load initial state
        self.state = load_state(settings.state_dir, state_file=os.path.basename(self.state_file))
        
        # Add mode info to state if not present
        if "mode" not in self.state:
            self.state["mode"] = current_mode.name
            
        # Initialize or reset multiprocessing info in state
        self.state["multiprocessing"] = {
            "last_processed_block": self.state.get('last_block', 0),
            "active_ranges": [],
            "next_block": self.state.get('last_block', 0)
        }
        save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
        
        # Create a shared state file for workers
        self.shared_state_file = os.path.join(settings.state_dir, f"shared_state_{current_mode.name}.json")
        self._init_shared_state()
        
        # Set up signal handling
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)
        
        # Process management
        self.processes = []
        self.failed_ranges = []  # Track ranges that failed completely after max retries
    
    def _init_shared_state(self):
        """Initialize the shared state file for worker processes."""
        if not os.path.exists(self.shared_state_file):
            shared_state = {
                "active_ranges": [],
                "completed_ranges": [],
                "last_block": self.state.get('last_block', 0),
                "next_block": self.state.get('last_block', 0)
            }
            write_shared_state(self.shared_state_file, shared_state)
    
    def _handle_exit(self, sig, frame):
        """Handle exit signals gracefully."""
        logger.info("Shutdown signal received, terminating worker processes...")
        for p in self.processes:
            if p.is_alive():
                p.terminate()
        
        logger.info("Saving final state...")
        self._update_main_state()
        
        # Log information about failed block ranges
        if self.failed_ranges:
            logger.error(f"There are {len(self.failed_ranges)} permanently failed block ranges")
            logger.error(f"Permanent failures: {self.failed_ranges}")
        
        logger.info("Shutdown complete")
        sys.exit(0)
    
    def _update_main_state(self):
        """Update the main state from the shared state file."""
        try:
            shared_state = read_shared_state(self.shared_state_file)
            
            # Update the main state with the highest block from shared state
            if 'last_block' in shared_state and shared_state['last_block'] > self.state.get('last_block', 0):
                self.state['last_block'] = shared_state['last_block']
                self.state['last_indexed_timestamp'] = int(time.time())
                save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                logger.info(f"Updated main state last_block to {self.state['last_block']}")
        except Exception as e:
            logger.error(f"Error updating main state: {e}")
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(ProcessingError),
        before_sleep=before_sleep_log(logger, "INFO"),  
        reraise=True
    )
    def _process_block_range(self, worker_id, start_block, end_block, datasets):
        """Process a block range with automatic retries."""
        retry_count = 0
        for attempt in range(3):  # tenacity doesn't easily expose attempt number, so we track it manually
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
                
                if attempt > 0:
                    logger.info(f"Worker {worker_id} RETRY #{attempt} for blocks {start_block}-{end_block}")
                    # Adjust rate limits for retries
                    os.environ["WORKER_CRYO_REQUESTS_PER_SECOND"] = str(max(50, 500 // (attempt + 1)))
                    os.environ["WORKER_CRYO_MAX_CONCURRENT"] = str(max(1, 5 // (attempt + 1)))
                    os.environ["WORKER_CRYO_TIMEOUT"] = str(300 + (attempt * 60))  # Increase timeout for retries
                else:
                    logger.info(f"Worker {worker_id} processing blocks {start_block}-{end_block}")
                
                # Each worker gets its own blockchain client and ClickHouse manager
                blockchain = BlockchainClient(settings.eth_rpc_url)
                clickhouse = ClickHouseManager(
                    host=settings.clickhouse_host,
                    user=settings.clickhouse_user,
                    password=settings.clickhouse_password,
                    database=settings.clickhouse_database,
                    port=settings.clickhouse_port,
                    secure=settings.clickhouse_secure
                )
                
                # Create worker using the unified IndexerWorker
                worker = IndexerWorker(
                    worker_id=str(worker_id),
                    blockchain=blockchain,
                    clickhouse=clickhouse,
                    data_dir=settings.data_dir,
                    network_name=settings.network_name,
                    rpc_url=settings.eth_rpc_url
                )
                
                # Process the block range
                success = worker.process_block_range(start_block, end_block, datasets)
                
                if success:
                    logger.info(f"Worker {worker_id} completed blocks {start_block}-{end_block}")
                    self._mark_range_completed(start_block, end_block)
                    return True
                else:
                    logger.error(f"Worker {worker_id} failed to process blocks {start_block}-{end_block}")
                    # If worker.process_block_range returns False, raise exception to trigger retry
                    raise ProcessingError(f"Failed to process blocks {start_block}-{end_block}")
                
            except ProcessingError:
                # Let tenacity handle the retry
                raise
            except Exception as e:
                logger.error(f"Worker {worker_id} encountered an error: {e}", exc_info=True)
                raise ProcessingError(f"Exception while processing blocks {start_block}-{end_block}: {str(e)}")
    
    def _worker_process(self, worker_id, start_block, end_block, datasets):
        """Worker process function that runs independently."""
        try:
            # Process with retries
            self._process_block_range(worker_id, start_block, end_block, datasets)
        except Exception as e:
            # If all retries failed, mark as permanently failed
            logger.error(f"Worker {worker_id} failed to process blocks {start_block}-{end_block} after all retries: {e}")
            self._mark_range_failed_permanent(start_block, end_block)
    
    def _get_active_workers(self):
        """Get the number of currently active worker processes."""
        return sum(1 for p in self.processes if p.is_alive())
    
    def _mark_range_completed(self, start_block, end_block):
        """Mark a block range as completed in the shared state."""
        try:
            # Read current shared state with locking
            shared_state = read_shared_state(self.shared_state_file)
            
            # Remove from active ranges
            active_ranges = shared_state.get('active_ranges', [])
            if [start_block, end_block] in active_ranges:
                active_ranges.remove([start_block, end_block])
            
            # Add to completed ranges
            completed_ranges = shared_state.get('completed_ranges', [])
            completed_ranges.append([start_block, end_block])
            
            # Sort and merge completed ranges
            completed_ranges.sort()
            merged_ranges = []
            
            for r in completed_ranges:
                if not merged_ranges or r[0] > merged_ranges[-1][1] + 1:
                    merged_ranges.append(r)
                else:
                    merged_ranges[-1][1] = max(merged_ranges[-1][1], r[1])
            
            # Update the last block if applicable
            if merged_ranges and merged_ranges[0][0] <= shared_state.get('last_block', 0) + 1:
                shared_state['last_block'] = max(shared_state.get('last_block', 0), merged_ranges[0][1])
            
            # Update shared state
            shared_state['active_ranges'] = active_ranges
            shared_state['completed_ranges'] = merged_ranges
            
            # Write updated shared state with locking
            write_shared_state(self.shared_state_file, shared_state)
            
        except Exception as e:
            logger.error(f"Error marking range {start_block}-{end_block} as completed: {e}")
    
    def _mark_range_failed_permanent(self, start_block, end_block):
        """Mark a block range as permanently failed after all retries."""
        try:
            # Read current shared state with locking
            shared_state = read_shared_state(self.shared_state_file)
            
            # Remove from active ranges
            active_ranges = shared_state.get('active_ranges', [])
            if [start_block, end_block] in active_ranges:
                active_ranges.remove([start_block, end_block])
            
            # Add to in-memory failed ranges list
            self.failed_ranges.append([start_block, end_block])
            
            # Log the permanent failure
            logger.error(f"Block range {start_block}-{end_block} has failed permanently after multiple retries")
            
            # Update shared state
            shared_state['active_ranges'] = active_ranges
            
            # Write updated shared state with locking
            write_shared_state(self.shared_state_file, shared_state)
            
        except Exception as e:
            logger.error(f"Error marking range {start_block}-{end_block} as permanently failed: {e}")
    
    def _clean_finished_processes(self):
        """Remove completed processes from the list."""
        self.processes = [p for p in self.processes if p.is_alive()]
    
    def _get_next_block_range(self, safe_block):
        """Get the next block range to process."""
        try:
            # Read current shared state with locking
            shared_state = read_shared_state(self.shared_state_file)
            
            # Get the next block to process
            next_block = shared_state.get('next_block', self.state.get('last_block', 0))
            
            # If we've reached the safe block, return None
            if next_block >= safe_block:
                return None
            
            # Calculate end block
            end_block = min(next_block + settings.worker_batch_size, safe_block)
            
            # Check if this range is already being processed
            active_ranges = shared_state.get('active_ranges', [])
            for start, end in active_ranges:
                if (start <= next_block < end) or (start < end_block <= end):
                    # Range overlap, try finding the next free range
                    next_block = max(end, next_block)
                    if next_block >= safe_block:
                        return None
                    end_block = min(next_block + settings.worker_batch_size, safe_block)
            
            # Check if this range is in permanently failed ranges
            for start, end in self.failed_ranges:
                if (start <= next_block < end) or (start < end_block <= end) or (next_block <= start < end_block):
                    # Skip this range or part of it
                    logger.warning(f"Skipping permanently failed range: {start}-{end}")
                    next_block = max(end, next_block)
                    if next_block >= safe_block:
                        return None
                    end_block = min(next_block + settings.worker_batch_size, safe_block)
            
            # Update next_block for next call
            shared_state['next_block'] = end_block
            
            # Add to active ranges
            active_ranges.append([next_block, end_block])
            shared_state['active_ranges'] = active_ranges
            
            # Write updated shared state with locking
            write_shared_state(self.shared_state_file, shared_state)
            
            return (next_block, end_block)
            
        except Exception as e:
            logger.error(f"Error getting next block range: {e}")
            return None
    
    def _log_status(self):
        """Log current status of the indexer."""
        try:
            shared_state = read_shared_state(self.shared_state_file)
            
            active_count = len(shared_state.get('active_ranges', []))
            completed_count = len(shared_state.get('completed_ranges', []))
            permanent_failures = len(self.failed_ranges)
            current_block = shared_state.get('next_block', 0)
            
            logger.info(f"Status: Current block: {current_block}, "
                       f"Active ranges: {active_count}, "
                       f"Completed ranges: {completed_count}, "
                       f"Permanent failures: {permanent_failures}")
        except Exception as e:
            logger.error(f"Error logging status: {e}")
    
    def run(self):
        """Start the multiprocess indexer and manage worker processes."""
        logger.info("Starting multiprocess indexer...")
        
        current_mode = settings.get_current_mode()
        historical_mode = settings.end_block > 0
        
        # Status logging timer
        last_status_time = time.time()
        status_interval = 60  # Log status every 60 seconds
        
        try:
            while True:
                # Clean up finished processes
                self._clean_finished_processes()
                
                # Get latest block from blockchain
                latest_block = self.blockchain.get_latest_block_number()
                safe_block = latest_block - settings.confirmation_blocks
                
                if historical_mode:
                    safe_block = min(safe_block, settings.end_block)
                
                # Check if we need to start more workers
                active_workers = self._get_active_workers()
                available_slots = settings.parallel_workers - active_workers
                
                if available_slots > 0:
                    for _ in range(available_slots):
                        next_range = self._get_next_block_range(safe_block)
                        
                        if next_range:
                            start_block, end_block = next_range
                            worker_id = len(self.processes)
                            
                            logger.info(f"Starting worker {worker_id} for blocks {start_block}-{end_block}")
                            
                            # Create and start new worker process
                            p = multiprocessing.Process(
                                target=self._worker_process,
                                args=(worker_id, start_block, end_block, current_mode.datasets)
                            )
                            p.start()
                            self.processes.append(p)
                            
                            # Give process a moment to initialize
                            time.sleep(0.5)
                        else:
                            break
                
                # Periodically update the main state from shared state
                self._update_main_state()
                
                # Periodically log status
                current_time = time.time()
                if current_time - last_status_time > status_interval:
                    self._log_status()
                    last_status_time = current_time
                
                # Check if we're done in historical mode
                if historical_mode and self.state.get('last_block', 0) >= settings.end_block:
                    # Wait for remaining processes to finish
                    for p in self.processes:
                        if p.is_alive():
                            p.join(timeout=10)
                    
                    logger.info(f"Reached end block {settings.end_block}. Indexing complete.")
                    self._log_status()
                    return
                
                # Small delay
                time.sleep(2)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            # Clean up processes on exit
            for p in self.processes:
                if p.is_alive():
                    p.terminate()
            
            self._update_main_state()
            self._log_status()
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            # Clean up processes on error
            for p in self.processes:
                if p.is_alive():
                    p.terminate()
            
            self._update_main_state()
            self._log_status()
            
            raise