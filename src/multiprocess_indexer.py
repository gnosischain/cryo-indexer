import os
import time
import json
import multiprocessing
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
from datetime import datetime
import signal
import sys

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .worker import IndexerWorker  # Import the new unified worker
from .utils import setup_logging, load_state, save_state


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
        self.process_data = {}  # Store info about each process
        
    def _init_shared_state(self):
        """Initialize the shared state file for worker processes."""
        if not os.path.exists(self.shared_state_file):
            shared_state = {
                "active_ranges": [],
                "completed_ranges": [],
                "last_block": self.state.get('last_block', 0),
                "next_block": self.state.get('last_block', 0)
            }
            with open(self.shared_state_file, 'w') as f:
                json.dump(shared_state, f, indent=2)
    
    def _handle_exit(self, sig, frame):
        """Handle exit signals gracefully."""
        logger.info("Shutdown signal received, terminating worker processes...")
        for p in self.processes:
            if p.is_alive():
                p.terminate()
        
        logger.info("Saving final state...")
        self._update_main_state()
        
        logger.info("Shutdown complete")
        sys.exit(0)
    
    def _update_main_state(self):
        """Update the main state from the shared state file."""
        try:
            if os.path.exists(self.shared_state_file):
                with open(self.shared_state_file, 'r') as f:
                    shared_state = json.load(f)
                
                # Update the main state with the highest block from shared state
                if 'last_block' in shared_state and shared_state['last_block'] > self.state.get('last_block', 0):
                    self.state['last_block'] = shared_state['last_block']
                    self.state['last_indexed_timestamp'] = int(time.time())
                    save_state(self.state, settings.state_dir, state_file=os.path.basename(self.state_file))
                    logger.info(f"Updated main state last_block to {self.state['last_block']}")
        except Exception as e:
            logger.error(f"Error updating main state: {e}")
    
    def _worker_process(self, worker_id, start_block, end_block, datasets):
        """Worker process function that runs independently."""
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
        
        logger.info(f"Worker {worker_id} started, processing blocks {start_block}-{end_block}")
        
        try:
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
            
            # Create worker using the new unified IndexerWorker
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
            else:
                logger.error(f"Worker {worker_id} failed to process blocks {start_block}-{end_block}")
        
        except Exception as e:
            logger.error(f"Worker {worker_id} encountered an error: {e}", exc_info=True)
    
    def _get_active_workers(self):
        """Get the number of currently active worker processes."""
        return sum(1 for p in self.processes if p.is_alive())
    
    def _mark_range_completed(self, start_block, end_block):
        """Mark a block range as completed in the shared state."""
        try:
            # Read current shared state
            with open(self.shared_state_file, 'r') as f:
                shared_state = json.load(f)
            
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
            
            # Write updated shared state
            with open(self.shared_state_file, 'w') as f:
                json.dump(shared_state, f, indent=2)
            
        except Exception as e:
            logger.error(f"Error marking range {start_block}-{end_block} as completed: {e}")
    
    def _clean_finished_processes(self):
        """Remove completed processes from the list."""
        self.processes = [p for p in self.processes if p.is_alive()]
    
    def _get_next_block_range(self, safe_block):
        """Get the next block range to process."""
        try:
            # Read current shared state
            with open(self.shared_state_file, 'r') as f:
                shared_state = json.load(f)
            
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
            
            # Update next_block for next call
            shared_state['next_block'] = end_block
            
            # Add to active ranges
            active_ranges.append([next_block, end_block])
            shared_state['active_ranges'] = active_ranges
            
            # Write updated shared state
            with open(self.shared_state_file, 'w') as f:
                json.dump(shared_state, f, indent=2)
            
            return (next_block, end_block)
            
        except Exception as e:
            logger.error(f"Error getting next block range: {e}")
            return None
    
    def run(self):
        """Start the multiprocess indexer and manage worker processes."""
        logger.info("Starting multiprocess indexer...")
        
        current_mode = settings.get_current_mode()
        historical_mode = settings.end_block > 0
        
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
                
                # Check if we're done in historical mode
                if historical_mode and self.state.get('last_block', 0) >= settings.end_block:
                    # Wait for remaining processes to finish
                    for p in self.processes:
                        if p.is_alive():
                            p.join(timeout=10)
                    
                    logger.info(f"Reached end block {settings.end_block}. Indexing complete.")
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
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            # Clean up processes on error
            for p in self.processes:
                if p.is_alive():
                    p.terminate()
            
            self._update_main_state()
            
            raise