import os
import time
import glob
import shutil
import subprocess
import json
from typing import Dict, Any, List, Tuple, Optional
from loguru import logger
from datetime import datetime

from .config import settings
from .blockchain import BlockchainClient
from .clickhouse_manager import ClickHouseManager
from .utils import setup_logging, load_state, save_state, find_parquet_files, format_block_range


class CryoIndexer:
    """Main indexer class that orchestrates the indexing process."""
    
    def __init__(self):
        # Set up logging
        setup_logging(settings.log_level, settings.log_dir)
        
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
        
        # Create necessary directories
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(settings.state_dir, exist_ok=True)
        
        # Load state
        self.state = load_state(settings.state_dir)
        
        # Initialize last block if it's 0
        if self.state['last_block'] == 0:
            current_last_block = self.clickhouse.get_latest_processed_block()
            start_override = settings.start_block
            self.state['last_block'] = max(current_last_block, start_override)
            save_state(self.state, settings.state_dir)
        
        logger.info(f"Indexer initialized with state: {self.state}")
        logger.info(f"Using data directory: {settings.data_dir}")
        logger.info(f"Using datasets: {settings.datasets}")
        
        # Verify Cryo installation
        self._verify_cryo()
        
        # Set up tables in ClickHouse
        self.clickhouse.setup_tables()
    
    def _verify_cryo(self):
        """Verify that Cryo is installed and working correctly."""
        try:
            result = subprocess.run(
                ["cryo", "--version"],
                check=False,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                logger.error(f"Cryo verification failed: {result.stderr}")
                raise Exception("Cryo is not properly installed or accessible")
            
            logger.info(f"Cryo version: {result.stdout.strip()}")
        except Exception as e:
            logger.error(f"Error verifying Cryo: {e}")
            raise
    
    def run(self) -> None:
        """Main loop that continuously indexes new blocks."""
        logger.info("Starting indexer...")
        
        while True:
            try:
                # Get the latest block number
                latest_block = self.blockchain.get_latest_block_number()
                
                # Calculate the safe block to process (allowing for potential reorgs)
                safe_block = latest_block - settings.confirmation_blocks
                
                logger.debug(f"Latest block: {latest_block}, Safe block: {safe_block}, Last processed: {self.state['last_block']}")
                
                # Check if there are new blocks to process
                if safe_block > self.state['last_block']:
                    # Check for reorgs before continuing
                    self._handle_potential_reorg()
                    
                    # Process new blocks
                    next_block = self.state['last_block']
                    end_block = min(safe_block, next_block + settings.max_blocks_per_batch)
                    
                    logger.info(f"Processing blocks {next_block} to {end_block}")
                    self._process_blocks(next_block, end_block)
                    
                    # Update state
                    self.state['last_block'] = end_block + 1
                    self.state['last_indexed_timestamp'] = int(time.time())
                    save_state(self.state, settings.state_dir)
                else:
                    logger.info(f"No new blocks to process. Waiting {settings.poll_interval} seconds...")
                
                # Sleep until next check
                time.sleep(settings.poll_interval)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                time.sleep(settings.poll_interval)
    
    def _handle_potential_reorg(self) -> None:
        """Check for and handle any blockchain reorganizations."""
        try:
            # Only check for reorgs if we've processed some blocks already
            if self.state['last_block'] <= 0:
                return
                
            # Get a range of blocks from our database to check against
            check_start = max(0, self.state['last_block'] - 10)
            db_blocks = self.clickhouse.get_blocks_in_range(check_start, self.state['last_block'])
            
            if not db_blocks:
                logger.warning("No blocks found in database for reorg check")
                return
                
            # Check if there's a reorg
            reorg_detected, common_ancestor = self.blockchain.detect_reorg(
                self.state['last_block'], 
                db_blocks
            )
            
            if reorg_detected:
                logger.warning(f"Reorg detected! Rolling back from block {self.state['last_block']} to {common_ancestor}")
                
                # Roll back the database
                success = self.clickhouse.rollback_to_block(common_ancestor)
                
                if success:
                    # Update our state
                    self.state['last_block'] = common_ancestor
                    save_state(self.state, settings.state_dir)
                    logger.info(f"Successfully rolled back to block {common_ancestor}")
                else:
                    logger.error("Failed to roll back database during reorg handling")
        
        except Exception as e:
            logger.error(f"Error handling potential reorg: {e}", exc_info=True)
    
    def _process_blocks(self, start_block: int, end_block: int) -> None:
        """Process a range of blocks using Cryo and load the data to ClickHouse."""
        try:
            # Clean up data directory before starting
            self._clean_data_directory()
            
            # Run Cryo to extract the data
            self._run_cryo(start_block, end_block)
            
            # Load the extracted data to ClickHouse
            self._load_to_clickhouse()
            
        except Exception as e:
            logger.error(f"Error processing blocks {start_block}-{end_block}: {e}", exc_info=True)
            raise
    
    def _clean_data_directory(self) -> None:
        """Clean up the data directory before a new extraction."""
        try:
            # Remove all files in the data directory
            files = glob.glob(os.path.join(settings.data_dir, "*"))
            for f in files:
                if os.path.isfile(f):
                    os.remove(f)
                elif os.path.isdir(f) and not f.endswith(".cryo"):
                    shutil.rmtree(f)
            
            # Make sure .cryo directory exists for reports
            os.makedirs(os.path.join(settings.data_dir, ".cryo", "reports"), exist_ok=True)
            
            logger.debug(f"Cleaned data directory: {settings.data_dir}")
        except Exception as e:
            logger.warning(f"Error cleaning data directory: {e}", exc_info=True)
    
    def _run_cryo(self, start_block: int, end_block: int) -> None:
        """Run Cryo to extract blockchain data."""
        try:
            # Process smaller batch sizes for more reliability
            batch_size = min(100, end_block - start_block + 1)
            
            # Process in smaller batches
            current_start = start_block
            while current_start <= end_block:
                current_end = min(current_start + batch_size - 1, end_block)
                logger.info(f"Running Cryo for blocks {current_start}-{current_end}")
                
                # Build the command
                cmd = [
                    "cryo",
                    *settings.datasets,
                    "--blocks", format_block_range(current_start, current_end),
                    "--output-dir", settings.data_dir,
                    "--rpc", settings.eth_rpc_url,
                    "--overwrite",
                    "--verbose",
                    "--requests-per-second", "1000",  # Rate limiting to avoid RPC issues
                    "--max-concurrent-requests", "5"  # Limit concurrent requests
                ]
                
                # If chain ID is specified, include it
                if settings.chain_id != 1:
                    cmd.extend(["--network-name", str(settings.chain_id)])
                
                logger.debug(f"Running Cryo command: {' '.join(cmd)}")
                
                # Run the command with environment variables and output capture
                env = os.environ.copy()
                env["ETH_RPC_URL"] = settings.eth_rpc_url
                
                # Try running with a smaller timeout first
                try:
                    process = subprocess.run(
                        cmd,
                        check=False,
                        capture_output=True,
                        text=True,
                        env=env,
                        timeout=300  # 5 minute timeout
                    )
                except subprocess.TimeoutExpired:
                    logger.warning(f"Cryo command timed out for blocks {current_start}-{current_end}, retrying with --requests-per-second 5")
                    # Try again with lower rate limit
                    cmd = [c if c != "10" else "5" for c in cmd]
                    process = subprocess.run(
                        cmd,
                        check=False,
                        capture_output=True,
                        text=True,
                        env=env
                    )
                
                logger.debug(f"Cryo stdout: {process.stdout}")
                
                if process.returncode != 0:
                    logger.error(f"Cryo error: {process.stderr}")
                    # Check if files were created despite the error
                    created_files = find_parquet_files(settings.data_dir, settings.datasets[0])
                    if created_files:
                        logger.warning("Cryo reported an error but did produce some output files. Continuing.")
                    else:
                        raise Exception(f"Cryo process failed with return code {process.returncode}: {process.stderr}")
                        
                logger.info(f"Successfully extracted data for blocks {current_start}-{current_end}")
                current_start = current_end + 1
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Cryo process error: {e.stderr}")
            raise Exception(f"Cryo process failed: {e}")
        except Exception as e:
            logger.error(f"Error running Cryo: {e}", exc_info=True)
            raise
    
    def _load_to_clickhouse(self) -> None:
        """Load extracted data files to ClickHouse."""
        total_rows = 0
        files_processed = 0
        files_failed = 0
        
        # Process each dataset
        for dataset in settings.datasets:
            files = find_parquet_files(settings.data_dir, dataset)
            
            if not files:
                logger.warning(f"No parquet files found for dataset {dataset}")
                continue
                
            logger.info(f"Found {len(files)} parquet files for dataset {dataset}")
            
            # Process each file
            for file_path in files:
                try:
                    rows = self.clickhouse.insert_parquet_file(file_path)
                    total_rows += rows
                    files_processed += 1
                    logger.info(f"Inserted {rows} rows from {os.path.basename(file_path)}")
                except Exception as e:
                    files_failed += 1
                    logger.error(f"Error loading file {file_path}: {e}", exc_info=True)
        
        logger.info(f"Loaded {total_rows} total rows from {files_processed} files to ClickHouse ({files_failed} files failed)")


if __name__ == "__main__":
    indexer = CryoIndexer()
    indexer.run()