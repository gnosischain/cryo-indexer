import os
import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.summary import QuerySummary
from typing import Dict, Any, List, Optional, Tuple, Union
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import time

from ..config import settings
from .clickhouse_pool import ClickHouseConnectionPool
from ..core.utils import read_parquet_to_pandas, parse_dataset_name_from_file


class ClickHouseManager:
    """Manager for ClickHouse database operations with insert_version support."""
    
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
        
        # First ensure the database exists before creating the connection pool
        self._ensure_database_exists()
        
    def _connect(self) -> Client:
        """Get a ClickHouse client from the connection pool."""
        return self.connection_pool.get_client()
    
    def _ensure_database_exists(self) -> None:
        """Make sure the target database exists."""
        try:
            # First, get a client without specifying the database
            # This avoids the error when the database doesn't exist
            temp_client = clickhouse_connect.get_client(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                secure=self.secure
                # Note: no database parameter here
            )
            
            # Create the database if it doesn't exist
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Ensured database {self.database} exists")
            
            # Close the temporary client
            temp_client.close()
            
            # Now update the connection pool to use the database
            self.connection_pool = ClickHouseConnectionPool(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                secure=self.secure
            )
            
            # Get a new client with the database specified
            self.client = self.connection_pool.get_client()
            
        except Exception as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise
    
    def _normalize_dataframe_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all empty values to NULL for consistency.
        This ensures that empty strings, None, NaN all become NULL in ClickHouse.
        """
        # Replace empty strings with None (which becomes NULL in ClickHouse)
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                # Replace empty strings with None
                df[col] = df[col].replace('', None)
                # Also handle whitespace-only strings
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == '' else x)
        
        # Ensure NaN values are converted to None
        df = df.where(pd.notnull(df), None)
        
        return df
    
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
            
            # Special handling for blocks table - drop problematic columns
            if table_name == 'blocks':
                columns_to_drop = ['total_difficulty_f64', 'total_difficulty_binary', 'total_difficulty_string', 'difficulty']
                existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
                if existing_columns_to_drop:
                    logger.info(f"Dropping columns from blocks data: {existing_columns_to_drop}")
                    df = df.drop(columns=existing_columns_to_drop)
            
            # Normalize NULL values for consistency
            df = self._normalize_dataframe_nulls(df)
            
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
            
            # Final normalization after all transformations
            df = self._normalize_dataframe_nulls(df)
            
            # IMPORTANT: Do not include insert_version in the DataFrame
            # It's a MATERIALIZED column that ClickHouse will populate automatically
            if 'insert_version' in df.columns:
                df = df.drop(columns=['insert_version'])
            
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
            
            if len(unique_blocks) == 0:
                logger.warning(f"No valid block numbers found in {table_name} data")
                return
                
            block_timestamps = {}
            missing_blocks = []
            
            # Batch query for better performance
            if len(unique_blocks) > 0:
                client = self._connect()
                blocks_str = ','.join(str(int(b)) for b in unique_blocks)
                query = f"""
                SELECT block_number, timestamp 
                FROM {self.database}.blocks 
                WHERE block_number IN ({blocks_str})
                AND timestamp IS NOT NULL
                AND timestamp > 0
                """
                result = client.query(query)
                
                for row in result.result_rows:
                    block_timestamps[row[0]] = row[1]
                
                # Find missing blocks
                missing_blocks = [b for b in unique_blocks if b not in block_timestamps]
                
                if missing_blocks:
                    logger.warning(
                        f"Missing timestamp data for {len(missing_blocks)} blocks "
                        f"in {table_name}. Examples: {missing_blocks[:5]}"
                    )
                    
                    # In strict mode, raise an error
                    if hasattr(settings, 'strict_timestamp_mode') and settings.strict_timestamp_mode:
                        raise ValueError(
                            f"Cannot add timestamps for {table_name}: "
                            f"{len(missing_blocks)} blocks missing. "
                            f"First missing: {missing_blocks[0]}"
                        )
                    
                    # Record for later fixing
                    self._record_timestamp_issues(table_name, missing_blocks)
            
            # Map block numbers to timestamps
            # Use None for missing timestamps (will become NULL in ClickHouse)
            df['block_timestamp'] = df['block_number'].apply(
                lambda x: pd.Timestamp.fromtimestamp(block_timestamps.get(x, 0)) 
                if pd.notna(x) and x in block_timestamps and block_timestamps.get(x, 0) > 0
                else None  # Use None instead of a default timestamp
            )
            
            # Log statistics
            null_timestamps = df['block_timestamp'].isna().sum()
            if null_timestamps > 0:
                logger.warning(
                    f"{table_name}: {null_timestamps} rows will have NULL timestamps "
                    f"due to missing block data"
                )
                
        except Exception as e:
            logger.error(f"Error adding timestamp columns: {e}")
            # In non-strict mode, use NULL for all timestamps
            if not (hasattr(settings, 'strict_timestamp_mode') and settings.strict_timestamp_mode):
                df['block_timestamp'] = None
                logger.warning(f"Using NULL timestamp for all rows in {table_name} due to error")
            else:
                raise
    
    def _record_timestamp_issues(self, table_name: str, missing_blocks: List[int]):
        """Record blocks with missing timestamp data for later fixing."""
        try:
            # Only record a sample to avoid flooding the table
            sample_size = min(len(missing_blocks), 100)
            sample_blocks = missing_blocks[:sample_size]
            
            logger.debug(
                f"Recording {sample_size} missing blocks for {table_name} "
                f"(total: {len(missing_blocks)})"
            )
            
            # This is informational logging for now
            # Could be extended to write to a tracking table if needed
            
        except Exception as e:
            logger.error(f"Error recording timestamp issues: {e}")
        
    def _convert_binary_columns(self, df):
        """Convert binary columns to hex strings for ClickHouse."""
        for col in df.columns:
            # Check if column contains binary data
            if df[col].dtype == 'object' and df[col].notna().any():
                first_non_null = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_non_null, bytes):
                    logger.debug(f"Converting binary column {col} to hex strings")
                    # Use None for empty/null values consistently
                    df[col] = df[col].apply(
                        lambda x: x.hex() if isinstance(x, bytes) and len(x) > 0 else None
                    )
                    
    
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
        # Filter only columns that exist in the table (excluding MATERIALIZED columns)
        materialized_columns = ['block_timestamp', 'month', 'insert_version']
        existing_columns = [col for col in df.columns if col in table_columns and col not in materialized_columns]
        
        # Fill missing columns with None (NULL) - not empty strings
        missing_columns = [col for col in table_columns if col not in df.columns and col not in materialized_columns]
        for col in missing_columns:
            logger.debug(f"Adding missing column: {col}")
            df[col] = None  # Consistently use None for NULL
        
        # Ensure columns are in the correct order (excluding MATERIALIZED columns)
        columns_to_select = [col for col in table_columns if col in df.columns and col not in materialized_columns]
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
            
            # Ensure we're using the correct database
            client.command(f"USE {self.database}")
            
            # Check if migrations table exists (it might not if this is the first run)
            try:
                executed_migrations = client.query(f"""
                SELECT name FROM {self.database}.migrations
                """)
                executed = set([row[0] for row in executed_migrations.result_rows])
            except Exception:
                # Table doesn't exist yet, will be created by 001_create_database.sql
                executed = set()
            
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
                    statements = []
                    current_statement = ""
                    
                    for line in sql.split('\n'):
                        # Skip empty lines and comments
                        if line.strip() and not line.strip().startswith('--'):
                            current_statement += line + '\n'
                            if line.strip().endswith(';'):
                                statements.append(current_statement.strip())
                                current_statement = ""
                    
                    # Add any remaining statement
                    if current_statement.strip():
                        statements.append(current_statement.strip())
                    
                    logger.info(f"Found {len(statements)} SQL statements in {file_name}")
                    
                    for i, statement in enumerate(statements):
                        if statement and not statement.startswith('--'):
                            # Skip INSERT INTO migrations statements if they're duplicates
                            if 'INSERT INTO' in statement and '.migrations' in statement and 'VALUES' in statement:
                                # Extract the migration name
                                import re
                                match = re.search(r"VALUES\s*\(\s*'([^']+)'", statement)
                                if match and match.group(1) in executed:
                                    logger.debug(f"Skipping duplicate migration record for {match.group(1)}")
                                    continue
                            
                            logger.debug(f"Executing statement {i+1}: {statement[:100]}...")
                            client.command(statement)
                            logger.debug(f"Statement {i+1} executed successfully")
                    
                    logger.info(f"Migration {file_name} executed successfully")
                    executed.add(file_name)  # Add to executed set
                    
                except Exception as e:
                    logger.error(f"Error executing migration {file_name}: {e}")
                    raise
            
            logger.info("All migrations completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error running migrations: {e}")
            return False
    
    def close(self) -> None:
        """Close all connections in the pool."""
        try:
            self.connection_pool.close_all()
        except Exception as e:
            logger.error(f"Error closing connections: {e}")