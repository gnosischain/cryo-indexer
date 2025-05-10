import threading
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from typing import Dict, Any, Optional
from loguru import logger

class ClickHouseConnectionPool:
    """
    A simple connection pool for ClickHouse clients.
    Creates a separate client for each thread to avoid concurrent query issues.
    """
    
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 8443,
        secure: bool = True
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.secure = secure
        
        # Store clients per thread
        self._clients = {}
        self._lock = threading.Lock()
    
    def get_client(self) -> Client:
        """
        Get a ClickHouse client for the current thread.
        If a client doesn't exist for this thread, create a new one.
        """
        thread_id = threading.get_ident()
        
        with self._lock:
            if thread_id not in self._clients:
                logger.debug(f"Creating new ClickHouse client for thread {thread_id}")
                client = clickhouse_connect.get_client(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    port=self.port,
                    secure=self.secure
                )
                
                # Test connection
                try:
                    client.command("SELECT 1")
                    self._clients[thread_id] = client
                    logger.debug(f"ClickHouse connection established for thread {thread_id}")
                except Exception as e:
                    logger.error(f"Error connecting to ClickHouse: {e}")
                    raise
            
            return self._clients[thread_id]
    
    def close_all(self) -> None:
        """Close all client connections in the pool."""
        with self._lock:
            for thread_id, client in self._clients.items():
                try:
                    client.close()
                    logger.debug(f"Closed ClickHouse client for thread {thread_id}")
                except Exception as e:
                    logger.warning(f"Error closing ClickHouse client for thread {thread_id}: {e}")
            
            self._clients.clear()