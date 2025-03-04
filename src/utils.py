import os
import json
import glob
import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from loguru import logger

def setup_logging(log_level: str, log_dir: str) -> None:
    """Set up logging configuration."""
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

def load_state(state_dir: str) -> Dict[str, Any]:
    """Load indexer state from file."""
    state_file = os.path.join(state_dir, "indexer_state.json")
    try:
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                return json.load(f)
        else:
            logger.warning(f"State file {state_file} not found, initializing with default state")
            return {
                'last_block': 0,
                'last_indexed_timestamp': 0,
                'chain_id': 1
            }
    except Exception as e:
        logger.error(f"Error loading state: {e}")
        return {
            'last_block': 0,
            'last_indexed_timestamp': 0,
            'chain_id': 1
        }

def save_state(state: Dict[str, Any], state_dir: str) -> None:
    """Save indexer state to file."""
    state_file = os.path.join(state_dir, "indexer_state.json")
    try:
        os.makedirs(state_dir, exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
        logger.debug(f"State saved: last_block={state.get('last_block', 0)}")
    except Exception as e:
        logger.error(f"Error saving state: {e}")

def find_parquet_files(data_dir: str, dataset: str) -> List[str]:
    """Find all parquet files for a dataset in the data directory."""
    pattern = os.path.join(data_dir, f"**/*__{dataset}__*.parquet")
    return glob.glob(pattern, recursive=True)

def read_parquet_to_pandas(file_path: str) -> pd.DataFrame:
    """Read a parquet file into a pandas DataFrame."""
    try:
        return pd.read_parquet(file_path)
    except Exception as e:
        logger.error(f"Error reading parquet file {file_path}: {e}")
        return pd.DataFrame()

def format_block_range(start_block: int, end_block: int) -> str:
    """Format a block range for use with Cryo CLI."""
    return f"{start_block}:{end_block}"

def parse_dataset_name_from_file(file_path: str) -> str:
    """Extract the dataset name from a parquet file path."""
    filename = os.path.basename(file_path)
    parts = filename.split("__")
    if len(parts) > 1:
        return parts[1]
    return "unknown"