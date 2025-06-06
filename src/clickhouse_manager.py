import os
import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.summary import QuerySummary
from typing import Dict, Any, List, Optional, Tuple, Union
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import settings
from .clickhouse_pool import ClickHouseConnectionPool
from .utils import read_parquet_to_pandas, parse_dataset_name_from_file


class ClickHouseManager:
    """Manager for ClickHouse database operations."""
    
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
        
        # Create connection pool
        self.connection_pool = ClickHouseConnectionPool(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            secure=secure
        )
        
        # Get a client from the pool for initial setup
        self.client = self.connection_pool.get_client()
        self._ensure_database_exists()
        
    def _connect(self) -> Client:
        """Get a ClickHouse client from the connection pool."""
        return self.connection_pool.get_client()
    
    def _ensure_database_exists(self) -> None:
        """Make sure the target database exists."""
        try:
            # Try to create the database if it doesn't exist
            client = self._connect()
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            
            # Set the current database
            client.command(f"USE {self.database}")
            logger.info(f"Using database: {self.database}")
        except Exception as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise
    
    def setup_tables(self) -> None:
        """Set up necessary tables for blockchain data."""
        try:
            # Tables are now managed by migration scripts
            # This method is maintained for backward compatibility
            logger.info("Tables will be created through migrations")
        except Exception as e:
            logger.error(f"Error setting up tables: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def insert_parquet_file(self, file_path: str) -> int:
        """
        Insert data from a parquet file into the appropriate ClickHouse table.
        
        Returns:
            Number of rows inserted
        """
        try:
            # Get a client from the pool
            client = self._connect()
            
            # Determine the target table
            dataset = parse_dataset_name_from_file(file_path)
            
            # Map Cryo dataset names to ClickHouse table names
            table_mappings = {
                # Default mappings (same name)
                'blocks': 'blocks',
                'transactions': 'transactions',
                'logs': 'logs',
                'contracts': 'contracts',
                'native_transfers': 'native_transfers',
                'traces': 'traces',
                
                # State diff tables
                'balance_diffs': 'balance_diffs',
                'code_diffs': 'code_diffs',
                'nonce_diffs': 'nonce_diffs',
                'storage_diffs': 'storage_diffs',
                
                # State read tables
                'balance_reads': 'balance_reads',
                'code_reads': 'code_reads',
                'nonce_reads': 'nonce_reads',
                'storage_reads': 'storage_reads',
                
                # ERC20/721 tables
                'erc20_transfers': 'erc20_transfers',
                'erc20_metadata': 'erc20_metadata',
                'erc20_balances': 'erc20_balances',
                'erc20_supplies': 'erc20_supplies',
                'erc20_approvals': 'erc20_approvals',
                'erc721_transfers': 'erc721_transfers',
                'erc721_metadata': 'erc721_metadata',
                
                # Aliases for compatibility
                'events': 'logs',
                'txs': 'transactions',
                'storages': 'storage_reads',
                'slot_diffs': 'storage_diffs',
                'slot_reads': 'storage_reads',
                'opcode_traces': 'vm_traces',
                '4byte_counts': 'four_byte_counts',
            }
            
            table_name = table_mappings.get(dataset, dataset)
        
            logger.info(f"Importing {file_path} into {table_name}")
            
            # Read the parquet file
            df = read_parquet_to_pandas(file_path)
            
            if df.empty:
                logger.warning(f"Empty DataFrame from {file_path}")
                return 0
            
            # Get table schema to ensure column compatibility
            table_exists = self._check_table_exists(table_name)
            if table_exists:
                table_columns = self._get_table_columns(table_name)
                df = self._adjust_dataframe_to_schema(df, table_columns)
                
                # Add timestamps and month partitioning columns
                if dataset != 'blocks' and 'block_number' in df.columns and 'block_timestamp' in table_columns:
                    self._add_timestamp_columns(df, table_name)
            
            # Convert binary columns to hex strings for ClickHouse
            self._convert_binary_columns(df)
            
            # Insert using a ReplacingMergeTree strategy
            # For ReplacingMergeTree tables, ClickHouse will eventually deduplicate rows
            # with the same primary key during merges
            result = client.insert_df(f"{self.database}.{table_name}", df)
            
            # Handle the QuerySummary object properly
            if isinstance(result, QuerySummary):
                row_count = result.written_rows if hasattr(result, 'written_rows') else len(df)
                logger.info(f"Inserted {row_count} rows into {table_name}")
                return row_count
            else:
                logger.info(f"Inserted {result} rows into {table_name}")
                return result
                
        except Exception as e:
            logger.error(f"Error inserting parquet file {file_path}: {e}")
            raise

    def _add_timestamp_columns(self, df, table_name):
        """Add timestamp and month columns to a dataframe based on block numbers."""
        try:
            # For each unique block number, get the timestamp
            unique_blocks = df['block_number'].dropna().unique()
            block_timestamps = {}
            
            for block_num in unique_blocks:
                try:
                    # Query the timestamp from blocks table
                    client = self._connect()
                    query = f"""
                    SELECT timestamp 
                    FROM {self.database}.blocks 
                    WHERE block_number = {int(block_num)} 
                    LIMIT 1
                    """
                    result = client.query(query)
                    if result.result_rows:
                        timestamp = result.result_rows[0][0]
                        block_timestamps[block_num] = timestamp
                except Exception as e:
                    logger.warning(f"Error getting timestamp for block {block_num}: {e}")
            
            # Map block numbers to timestamps
            df['block_timestamp'] = df['block_number'].map(
                lambda x: pd.Timestamp.fromtimestamp(block_timestamps.get(x, 0)) if pd.notna(x) else None
            )
        except Exception as e:
            logger.warning(f"Error adding timestamp columns: {e}")
        
    def _convert_binary_columns(self, df):
        """Convert binary columns to hex strings for ClickHouse."""
        for col in df.columns:
            # Check if column contains binary data
            if df[col].dtype == 'object' and df[col].notna().any():
                first_non_null = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_non_null, bytes):
                    logger.debug(f"Converting binary column {col} to hex strings")
                    df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else None)
                    
    
    def _check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            client = self._connect()
            result = client.query(f"""
            SELECT 1 FROM system.tables 
            WHERE database = '{self.database}' AND name = '{table_name}'
            """)
            return len(result.result_rows) > 0
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get the list of columns in a table."""
        try:
            client = self._connect()
            result = client.query(f"""
            SELECT name FROM system.columns 
            WHERE database = '{self.database}' AND table = '{table_name}'
            ORDER BY position
            """)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            logger.error(f"Error getting columns for table {table_name}: {e}")
            return []
    
    def _adjust_dataframe_to_schema(self, df: pd.DataFrame, table_columns: List[str]) -> pd.DataFrame:
        """Adjust DataFrame columns to match the table schema."""
        # Filter only columns that exist in the table
        existing_columns = [col for col in df.columns if col in table_columns]
        
        # Fill missing columns with None/NULL
        missing_columns = [col for col in table_columns if col not in df.columns]
        for col in missing_columns:
            if col not in ['block_timestamp', 'month']:  # Skip materialized columns
                logger.info(f"Adding missing column: {col}")
                df[col] = None
        
        # Ensure columns are in the correct order (except materialized columns)
        columns_to_select = [col for col in table_columns if col in df.columns and col not in ['block_timestamp', 'month']]
        return df[columns_to_select]
    
    def get_latest_processed_block(self) -> int:
        """Get the latest block number that has been processed and stored in ClickHouse."""
        try:
            client = self._connect()
            result = client.query(f"""
            SELECT MAX(block_number) as max_block 
            FROM {self.database}.blocks
            """)
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.error(f"Error getting latest processed block: {e}")
            return 0
    
    def get_blocks_in_range(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Get blocks in a specified range from the database.
        Used for reorg detection.
        """
        try:
            client = self._connect()
            result = client.query(f"""
            SELECT block_number, block_hash
            FROM {self.database}.blocks
            WHERE block_number BETWEEN {start_block} AND {end_block}
            ORDER BY block_number
            """)
            
            blocks = []
            for row in result.result_rows:
                blocks.append({
                    'block_number': row[0],
                    'block_hash': row[1]
                })
            
            return blocks
        except Exception as e:
            logger.error(f"Error getting blocks in range {start_block}-{end_block}: {e}")
            return []
    
    def rollback_to_block(self, block_number: int) -> bool:
        """
        Delete all data after a specific block number.
        Used for handling reorgs.
        """
        try:
            client = self._connect()
            
            # List of tables to rollback
            tables = [
                'blocks', 
                'transactions', 
                'logs',
                'contracts',
                'native_transfers',
                'traces',
                'balance_diffs',
                'code_diffs',
                'nonce_diffs',
                'storage_diffs',
                'balance_reads',
                'code_reads',
                'storage_reads',
                'erc20_transfers',
                'erc721_transfers'
            ]
            
            for table in tables:
                # Check if table exists before attempting to delete from it
                table_exists = client.query(f"""
                SELECT 1 FROM system.tables 
                WHERE database = '{self.database}' AND name = '{table}'
                """)
                
                if table_exists.result_rows:
                    client.command(f"""
                    ALTER TABLE {self.database}.{table}
                    DELETE WHERE block_number > {block_number}
                    """)
                    logger.info(f"Rolled back table {table} to block {block_number}")
            
            logger.info(f"Successfully rolled back database to block {block_number}")
            return True
        except Exception as e:
            logger.error(f"Error rolling back to block {block_number}: {e}")
            return False
    
    def run_migrations(self, migrations_dir: str) -> bool:
        """Run SQL migrations from the provided directory."""
        try:
            client = self._connect()
            
            # Get all SQL files in the migrations directory, sorted
            migration_files = sorted([
                f for f in os.listdir(migrations_dir)
                if f.endswith('.sql') and os.path.isfile(os.path.join(migrations_dir, f))
            ])
            
            logger.info(f"Found {len(migration_files)} migration files")
            
            # Create migrations table if it doesn't exist
            client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.migrations
            (
                `name` String,
                `executed_at` DateTime DEFAULT now(),
                `success` UInt8 DEFAULT 1
            )
            ENGINE = MergeTree()
            ORDER BY (name)
            """)
            
            # Get already executed migrations
            executed_migrations = client.query(f"""
            SELECT name FROM {self.database}.migrations
            """)
            
            executed = set([row[0] for row in executed_migrations.result_rows])
            
            # Execute each migration file if not already executed
            for file_name in migration_files:
                if file_name in executed:
                    logger.debug(f"Migration {file_name} already executed, skipping")
                    continue
                
                logger.info(f"Executing migration: {file_name}")
                
                # Read the SQL file content
                with open(os.path.join(migrations_dir, file_name), 'r') as f:
                    sql = f.read()
                
                # Replace {{database}} placeholder with the actual database name
                sql = sql.replace('{{database}}', self.database)
                
                # Execute the SQL
                try:
                    # Execute each statement separately (split by semicolon)
                    for statement in sql.split(';'):
                        statement = statement.strip()
                        if statement:
                            client.command(statement)
                    
                    # Record successful migration
                    client.command(f"""
                    INSERT INTO {self.database}.migrations (name) VALUES ('{file_name}')
                    """)
                    
                    logger.info(f"Migration {file_name} executed successfully")
                except Exception as e:
                    logger.error(f"Error executing migration {file_name}: {e}")
                    raise
            
            logger.info("All migrations completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error running migrations: {e}")
            return False
        
    def get_latest_block_for_table(self, table_name: str) -> int:
        """Get the latest block number for a specific table."""
        try:
            client = self._connect()
            
            # Check if the table exists first
            table_exists = self._check_table_exists(table_name)
            if not table_exists:
                logger.warning(f"Table {table_name} does not exist")
                return 0
                
            # Check if the table has a block_number column
            columns = self._get_table_columns(table_name)
            if 'block_number' not in columns:
                logger.warning(f"Table {table_name} does not have a block_number column")
                return 0
            
            # Query the max block number
            result = client.query(f"""
            SELECT MAX(block_number) as max_block 
            FROM {self.database}.{table_name}
            """)
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception as e:
            logger.error(f"Error getting latest block for table {table_name}: {e}")
            return 0