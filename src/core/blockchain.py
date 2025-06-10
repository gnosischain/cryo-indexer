import time
import requests
import json
from loguru import logger
from typing import Dict, Any, Optional, List, Tuple
from web3 import Web3
from tenacity import retry, stop_after_attempt, wait_exponential

class BlockchainClient:
    """Client for interacting with blockchain nodes via JSON-RPC."""
    
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        logger.info(f"BlockchainClient initialization: { self.rpc_url}")
        self.web3 = Web3(Web3.HTTPProvider(rpc_url))
        self.session = requests.Session()
        logger.info(f"BlockchainClient initialized. Connected: {self.web3.is_connected()}")
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_latest_block_number(self) -> int:
        """Get the latest block number from the blockchain."""
        try:
            return self.web3.eth.block_number
        except Exception as e:
            logger.error(f"Error getting latest block number: {e}")
            raise
    
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
    def get_block_hash(self, block_number: int) -> str:
        """Get the block hash for a given block number."""
        try:
            block = self.web3.eth.get_block(block_number)
            return block.hash.hex()
        except Exception as e:
            logger.error(f"Error getting block hash for block {block_number}: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def get_block(self, block_number: int) -> Dict[str, Any]:
        """Get the full block data for a given block number."""
        try:
            block = self.web3.eth.get_block(block_number, full_transactions=True)
            return {
                'number': block.number,
                'hash': block.hash.hex(),
                'parent_hash': block.parentHash.hex(),
                'timestamp': block.timestamp
            }
        except Exception as e:
            logger.error(f"Error getting block {block_number}: {e}")
            raise
    
    def find_common_ancestor(self, suspect_block: int, chain_db_blocks: List[Dict[str, Any]]) -> int:
        """
        Find the common ancestor between the current chain and our indexed chain.
        
        Args:
            suspect_block: The block number where we suspect a reorg might have happened
            chain_db_blocks: List of blocks from our database, containing block numbers and hashes
            
        Returns:
            Block number of the common ancestor
        """
        # Convert to a dict for faster lookups
        db_blocks = {b['block_number']: b['block_hash'] for b in chain_db_blocks}
        
        # Start from the suspect block and move backwards
        check_block = suspect_block
        
        while check_block > 0:
            # Skip if we don't have this block in our database
            if check_block not in db_blocks:
                check_block -= 1
                continue
                
            # Get the current hash for this block number
            try:
                chain_hash = self.get_block_hash(check_block)
                db_hash = db_blocks[check_block]
                
                if chain_hash == db_hash:
                    logger.info(f"Found common ancestor at block {check_block}")
                    return check_block
            except Exception as e:
                logger.warning(f"Error comparing block {check_block}: {e}")
                
            check_block -= 1
            
        # If we reached block 0, return 0 as the common ancestor
        return 0
    
    def detect_reorg(self, last_processed_block: int, chain_db_blocks: List[Dict[str, Any]]) -> Tuple[bool, int]:
        """
        Detect if a reorganization has occurred.
        
        Returns:
            Tuple of (reorg_detected, common_ancestor)
        """
        if not chain_db_blocks:
            return False, last_processed_block
            
        # Get the hash of the most recent processed block
        try:
            chain_hash = self.get_block_hash(last_processed_block)
            db_hash = next((b['block_hash'] for b in chain_db_blocks if b['block_number'] == last_processed_block), None)
            
            if db_hash is None:
                logger.warning(f"Block {last_processed_block} not found in database")
                return False, last_processed_block
                
            if chain_hash != db_hash:
                logger.warning(f"Reorg detected at block {last_processed_block}")
                logger.warning(f"Chain hash: {chain_hash}, DB hash: {db_hash}")
                
                # Find the common ancestor
                common_ancestor = self.find_common_ancestor(last_processed_block, chain_db_blocks)
                return True, common_ancestor
            
            return False, last_processed_block
            
        except Exception as e:
            logger.error(f"Error detecting reorg: {e}")
            return False, last_processed_block