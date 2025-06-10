import os
import json
import glob
import logging
import pandas as pd
import pyarrow.parquet as pq
from typing import Dict, Any, List, Optional, Tuple
from loguru import logger


def setup_logging(log_level: str, log_dir: str) -> None:
    """Set up logging configuration."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "indexer.log")
    
    # Remove default logger
    logger.remove()
    
    # Add console and file loggers
    logger.add(
        lambda msg: print(msg, end=""),
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(
        log_file,
        rotation="10 MB",
        retention="7 days",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"
    )
    
    logger.info(f"Logging initialized at level {log_level}")


def load_state(state_dir: str, state_file: Optional[str] = None) -> Dict[str, Any]:
    """Load indexer state from file."""
    if state_file:
        file_path = os.path.join(state_dir, state_file)
    else:
        file_path = os.path.join(state_dir, "indexer_state.json")
    
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return json.load(f)
        else:
            logger.warning(f"State file {file_path} not found, initializing with default state")
            return {
                'last_block': 0,
                'last_indexed_timestamp': 0,
                'network_name': os.environ.get('NETWORK_NAME', ''),
                'mode': 'default'
            }
    except Exception as e:
        logger.error(f"Error loading state from {file_path}: {e}")
        return {
            'last_block': 0,
            'last_indexed_timestamp': 0,
            'network_name': os.environ.get('NETWORK_NAME', ''),
            'mode': 'default'
        }


def save_state(state: Dict[str, Any], state_dir: str, state_file: Optional[str] = None) -> None:
    """Save indexer state to file."""
    if state_file:
        file_path = os.path.join(state_dir, state_file)
    else:
        file_path = os.path.join(state_dir, "indexer_state.json")
    
    try:
        os.makedirs(state_dir, exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(state, f, indent=2)
        logger.debug(f"State saved to {file_path}: last_block={state.get('last_block', 0)}, mode={state.get('mode', 'default')}")
    except Exception as e:
        logger.error(f"Error saving state to {file_path}: {e}")


def find_parquet_files(data_dir: str, dataset: str) -> List[str]:
    """Find all parquet files for a dataset in the data directory."""
    # Handle multiple datasets
    if isinstance(dataset, list):
        all_files = []
        for ds in dataset:
            all_files.extend(find_parquet_files(data_dir, ds))
        return all_files
    
    # Look for network-specific patterns too
    patterns = [
        # Standard cryo pattern: {network}__{dataset}__{range}.parquet
        os.path.join(data_dir, f"**/*__{dataset}__*.parquet"),
        # Alternative pattern: {network}_{dataset}_{range}.parquet
        os.path.join(data_dir, f"**/*_{dataset}_*.parquet"),
        # Alternative pattern: {dataset}__{range}.parquet
        os.path.join(data_dir, f"**/{dataset}__*.parquet"),
        # Alternative pattern: {dataset}_{range}.parquet
        os.path.join(data_dir, f"**/{dataset}_*.parquet"),
        # Most general pattern (use as last resort)
        os.path.join(data_dir, f"**/*{dataset}*.parquet"),
    ]
    
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern, recursive=True))
    
    return sorted(list(set(files)))  # Remove duplicates and sort


def read_parquet_to_pandas(file_path: str) -> pd.DataFrame:
    """Read a parquet file into a pandas DataFrame with better error handling."""
    try:
        # First check if the file exists and is not empty
        if not os.path.exists(file_path):
            logger.error(f"Parquet file does not exist: {file_path}")
            return pd.DataFrame()
            
        if os.path.getsize(file_path) == 0:
            logger.error(f"Parquet file is empty: {file_path}")
            return pd.DataFrame()
        
        # Try reading with pyarrow first for better error messages
        try:
            table = pq.read_table(file_path)
            return table.to_pandas()
        except Exception as e:
            logger.warning(f"PyArrow failed to read {file_path}: {e}, trying pandas directly")
            return pd.read_parquet(file_path)
    except Exception as e:
        logger.error(f"Error reading parquet file {file_path}: {e}")
        return pd.DataFrame()


def format_block_range(start_block: int, end_block: int) -> str:
    """Format a block range for use with Cryo CLI."""
    return f"{start_block}:{end_block}"


def parse_dataset_name_from_file(file_path: str) -> str:
    """Extract the dataset name from a parquet file path with better pattern matching."""
    filename = os.path.basename(file_path)
    
    # Try different parsing patterns
    # Pattern 1: network__dataset__range.parquet
    parts = filename.split("__")
    if len(parts) > 1:
        return parts[1]
    
    # Pattern 2: dataset__range.parquet
    if "__" in filename:
        return filename.split("__")[0]
    
    # Pattern 3: dataset_range.parquet
    if "_" in filename:
        return filename.split("_")[0]
    
    # Fallback to known datasets
    known_datasets = ["blocks", "transactions", "txs", "logs", "events", "contracts", 
                      "traces", "native_transfers", "balance_diffs", "code_diffs", 
                      "nonce_diffs", "storage_diffs"]
    
    for dataset in known_datasets:
        if dataset in filename:
            return dataset
    
    return "unknown"


def list_modes() -> None:
    """Utility function to list all available indexer modes."""
    try:
        from .config import settings
        
        print("Available indexer modes:")
        print("=" * 80)
        
        for name, mode in settings.get_available_modes().items():
            print(f"Mode: {name}")
            print(f"  Description: {mode.description}")
            print(f"  Datasets: {', '.join(mode.datasets)}")
            print(f"  Start Block: {mode.start_block}")
            print("-" * 80)
    except ImportError:
        print("Could not import settings. Make sure you're running this from the correct directory.")


def parse_block_ranges(ranges_str: str) -> List[Tuple[int, int]]:
    """Parse comma-separated block ranges like '100-200,500-600'."""
    ranges = []
    for range_str in ranges_str.split(','):
        range_str = range_str.strip()
        if '-' in range_str:
            try:
                start, end = map(int, range_str.split('-'))
                ranges.append((start, end))
            except ValueError:
                logger.error(f"Invalid range format: {range_str}")
    return ranges


def find_continuous_ranges(blocks: List[int]) -> List[Tuple[int, int]]:
    """Find continuous ranges in a list of block numbers."""
    if not blocks:
        return []
    
    blocks = sorted(blocks)
    ranges = []
    start = blocks[0]
    end = blocks[0]
    
    for block in blocks[1:]:
        if block == end + 1:
            end = block
        else:
            ranges.append((start, end + 1))
            start = block
            end = block
    
    ranges.append((start, end + 1))
    return ranges