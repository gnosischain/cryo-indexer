"""
Thread-based worker pool for parallel blockchain indexing.
"""
import threading
import queue
import time
from typing import List, Tuple, Dict, Optional, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from ..worker import IndexerWorker
from ..core.blockchain import BlockchainClient
from ..db.clickhouse_manager import ClickHouseManager
from ..core.state_manager import StateManager


@dataclass
class WorkItem:
    """Represents a unit of work for indexing."""
    start_block: int
    end_block: int
    datasets: List[str]
    priority: int = 0
    retry_count: int = 0
    

@dataclass
class WorkResult:
    """Result from processing a work item."""
    start_block: int
    end_block: int
    success: bool
    rows_processed: int = 0
    error: Optional[str] = None
    worker_id: str = ""
    duration: float = 0.0


class WorkerThread(threading.Thread):
    """Worker thread that processes indexing tasks."""
    
    def __init__(
        self,
        worker_id: int,
        work_queue: queue.Queue,
        result_queue: queue.Queue,
        config: Dict,
        stop_event: threading.Event
    ):
        super().__init__(name=f"Worker-{worker_id}")
        self.worker_id = worker_id
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.config = config
        self.stop_event = stop_event
        self.daemon = True
        
        # Each worker thread gets its own connections
        self.blockchain = None
        self.clickhouse = None
        self.state_manager = None
        self.worker = None
        
    def run(self):
        """Main worker loop."""
        try:
            # Initialize connections
            self._initialize_connections()
            
            logger.info(f"Worker {self.worker_id} started")
            
            while not self.stop_event.is_set():
                try:
                    # Get work with timeout to check stop event periodically
                    work_item = self.work_queue.get(timeout=1.0)
                    
                    if work_item is None:  # Poison pill
                        break
                        
                    # Process the work item
                    self._process_work_item(work_item)
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Worker {self.worker_id} error: {e}", exc_info=True)
                    time.sleep(1)  # Brief pause before continuing
                    
        except Exception as e:
            logger.error(f"Worker {self.worker_id} crashed: {e}", exc_info=True)
        finally:
            self._cleanup_connections()
            logger.info(f"Worker {self.worker_id} stopped")
    
    def _initialize_connections(self):
        """Initialize blockchain and database connections."""
        try:
            # Create blockchain client
            self.blockchain = BlockchainClient(self.config['eth_rpc_url'])
            
            # Create ClickHouse connection
            self.clickhouse = ClickHouseManager(
                host=self.config['clickhouse_host'],
                user=self.config['clickhouse_user'],
                password=self.config['clickhouse_password'],
                database=self.config['clickhouse_database'],
                port=self.config['clickhouse_port'],
                secure=self.config['clickhouse_secure']
            )
            
            # Create state manager
            self.state_manager = StateManager(self.clickhouse)
            
            # Create worker instance
            self.worker = IndexerWorker(
                worker_id=f"thread_{self.worker_id}",
                blockchain=self.blockchain,
                clickhouse=self.clickhouse,
                state_manager=self.state_manager,
                data_dir=self.config['data_dir'],
                network_name=self.config['network_name'],
                rpc_url=self.config['eth_rpc_url'],
                mode=self.config['mode'],
                batch_id=self.config['batch_id']
            )
            
            logger.info(f"Worker {self.worker_id} initialized connections")
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id} failed to initialize: {e}")
            raise
    
    def _cleanup_connections(self):
        """Clean up connections before stopping."""
        try:
            if hasattr(self.clickhouse, 'close'):
                self.clickhouse.close()
        except:
            pass
    
    def _process_work_item(self, work_item: WorkItem):
        """Process a single work item."""
        start_time = time.time()
        
        try:
            logger.info(f"Worker {self.worker_id} processing blocks {work_item.start_block}-{work_item.end_block}")
            
            # Process the range
            success = self.worker.process_range(
                work_item.start_block,
                work_item.end_block,
                work_item.datasets,
                force=True  # Force processing in historical mode
            )
            
            duration = time.time() - start_time
            
            # Send result
            result = WorkResult(
                start_block=work_item.start_block,
                end_block=work_item.end_block,
                success=success,
                worker_id=f"thread_{self.worker_id}",
                duration=duration
            )
            
            self.result_queue.put(result)
            
            if success:
                logger.info(f"Worker {self.worker_id} completed {work_item.start_block}-{work_item.end_block} in {duration:.2f}s")
            else:
                logger.error(f"Worker {self.worker_id} failed {work_item.start_block}-{work_item.end_block}")
                
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Worker {self.worker_id} exception processing {work_item.start_block}-{work_item.end_block}: {e}")
            
            # Send failure result
            result = WorkResult(
                start_block=work_item.start_block,
                end_block=work_item.end_block,
                success=False,
                error=str(e),
                worker_id=f"thread_{self.worker_id}",
                duration=duration
            )
            self.result_queue.put(result)


class WorkerPool:
    """Manages a pool of worker threads for parallel indexing."""
    
    def __init__(self, num_workers: int, config: Dict):
        self.num_workers = num_workers
        self.config = config
        
        # Queues - smaller queue size to reduce flooding warnings
        self.work_queue = queue.Queue(maxsize=num_workers * 2)  # Reduced from 4 to 2
        self.result_queue = queue.Queue()
        
        # Thread management
        self.workers = []
        self.stop_event = threading.Event()
        
        # Statistics
        self.stats = {
            'submitted': 0,
            'completed': 0,
            'failed': 0,
            'total_duration': 0.0,
            'total_rows': 0
        }
        
        # Result processor thread
        self.result_processor = None
        self.result_callback = None
        
        # Work submission tracking
        self._all_work_items = []
        self._submission_index = 0
        self._submission_lock = threading.Lock()
        
    def start(self, result_callback: Optional[Callable[[WorkResult], None]] = None):
        """Start the worker pool."""
        self.result_callback = result_callback
        
        # Start worker threads
        for i in range(self.num_workers):
            worker = WorkerThread(
                worker_id=i,
                work_queue=self.work_queue,
                result_queue=self.result_queue,
                config=self.config,
                stop_event=self.stop_event
            )
            worker.start()
            self.workers.append(worker)
        
        # Start result processor
        self.result_processor = threading.Thread(
            target=self._process_results,
            name="ResultProcessor"
        )
        self.result_processor.daemon = True
        self.result_processor.start()
        
        # Start work submission thread
        self.submission_thread = threading.Thread(
            target=self._submit_work_items,
            name="WorkSubmitter"
        )
        self.submission_thread.daemon = True
        self.submission_thread.start()
        
        logger.info(f"Started worker pool with {self.num_workers} workers")
    
    def submit(self, start_block: int, end_block: int, datasets: List[str], 
               priority: int = 0, block: bool = True) -> bool:
        """Submit work to the pool."""
        work_item = WorkItem(
            start_block=start_block,
            end_block=end_block,
            datasets=datasets,
            priority=priority
        )
        
        # Add to work items list for async submission
        with self._submission_lock:
            self._all_work_items.append(work_item)
            
        return True
    
    def _submit_work_items(self):
        """Background thread to submit work items to the queue."""
        while not self.stop_event.is_set():
            try:
                with self._submission_lock:
                    if self._submission_index < len(self._all_work_items):
                        work_item = self._all_work_items[self._submission_index]
                        self._submission_index += 1
                    else:
                        work_item = None
                
                if work_item:
                    # Try to submit with timeout
                    try:
                        self.work_queue.put(work_item, timeout=1.0)
                        self.stats['submitted'] += 1
                    except queue.Full:
                        # Put the item back for retry
                        with self._submission_lock:
                            self._submission_index -= 1
                        time.sleep(0.1)  # Brief pause before retry
                else:
                    # No more work to submit, sleep a bit
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error in work submission thread: {e}")
                time.sleep(1)
    
    def stop(self, timeout: int = 30):
        """Stop the worker pool gracefully."""
        logger.info("Stopping worker pool...")
        
        # Signal stop
        self.stop_event.set()
        
        # Send poison pills
        for _ in self.workers:
            try:
                self.work_queue.put(None, block=False)
            except queue.Full:
                pass
        
        # Wait for workers to finish
        deadline = time.time() + timeout
        for worker in self.workers:
            remaining = deadline - time.time()
            if remaining > 0:
                worker.join(timeout=remaining)
                if worker.is_alive():
                    logger.warning(f"Worker {worker.name} did not stop gracefully")
        
        # Wait for result processor
        if self.result_processor and self.result_processor.is_alive():
            self.result_processor.join(timeout=5)
            
        # Wait for submission thread
        if hasattr(self, 'submission_thread') and self.submission_thread.is_alive():
            self.submission_thread.join(timeout=5)
        
        logger.info("Worker pool stopped")
    
    def _process_results(self):
        """Process results from workers."""
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                result = self.result_queue.get(timeout=0.5)
                
                # Update statistics
                if result.success:
                    self.stats['completed'] += 1
                else:
                    self.stats['failed'] += 1
                
                self.stats['total_duration'] += result.duration
                
                # Call callback if provided
                if self.result_callback:
                    try:
                        self.result_callback(result)
                    except Exception as e:
                        logger.error(f"Error in result callback: {e}")
                        
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing result: {e}")
    
    def get_stats(self) -> Dict:
        """Get current statistics."""
        stats = self.stats.copy()
        stats['pending'] = self.work_queue.qsize()
        
        with self._submission_lock:
            stats['total_work_items'] = len(self._all_work_items)
            stats['submitted_work_items'] = self._submission_index
            stats['remaining_to_submit'] = len(self._all_work_items) - self._submission_index
        
        stats['active'] = self.stats['submitted'] - self.stats['completed'] - self.stats['failed'] - stats['pending']
        
        if self.stats['completed'] > 0:
            stats['avg_duration'] = self.stats['total_duration'] / self.stats['completed']
        else:
            stats['avg_duration'] = 0
            
        return stats
    
    def wait_for_completion(self, check_interval: float = 1.0) -> bool:
        """Wait for all submitted work to complete."""
        while not self.stop_event.is_set():
            stats = self.get_stats()
            
            # Check if all work is done
            if (stats['submitted_work_items'] >= stats['total_work_items'] and 
                stats['pending'] == 0 and stats['active'] == 0):
                return True
                
            time.sleep(check_interval)
            
        return False