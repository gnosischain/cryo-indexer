import os
import pandas as pd
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.summary import QuerySummary
from typing import Dict, Any, List, Optional
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from .clickhouse_pool import ClickHouseConnectionPool
from ..core.utils import read_parquet_to_pandas, parse_dataset_name_from_file


class ClickHouseManager:
    """Simplified ClickHouse manager with strict timestamp requirements."""
    
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
        
        # First ensure the database exists
        self._ensure_database_exists()
        
    def _connect(self) -> Client:
        """Get a ClickHouse client from the connection pool."""
        return self.connection_pool.get_client()
    
    def _ensure_database_exists(self) -> None:
        """Make sure the target database exists."""
        try:
            # Create client without database specified
            temp_client = clickhouse_connect.get_client(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                secure=self.secure
            )
            
            # Create the database if it doesn't exist
            temp_client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Ensured database {self.database} exists")
            
            # Close the temporary client
            temp_client.close()
            
            # Now create the connection pool with the database
            self.connection_pool = ClickHouseConnectionPool(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                secure=self.secure
            )
            
        except Exception as e:
            logger.error(f"Error ensuring database exists: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    def insert_parquet_file(self, file_path: str) -> int:
        """
        Insert data from a parquet file into ClickHouse.
        Simplified with strict timestamp requirements.
        """
        try:
            client = self._connect()
            
            # Determine the target table
            dataset = parse_dataset_name_from_file(file_path)
            
            # Table mappings
            table_mappings = {
                'blocks': 'blocks',
                'transactions': 'transactions',
                'logs': 'logs',
                'contracts': 'contracts',
                'native_transfers': 'native_transfers',
                'traces': 'traces',
                'balance_diffs': 'balance_diffs',
                'code_diffs': 'code_diffs',
                'nonce_diffs': 'nonce_diffs',
                'storage_diffs': 'storage_diffs'
            }
            
            table_name = table_mappings.get(dataset, dataset)
            logger.info(f"Importing {file_path} into {table_name}")
            
            # Read the parquet file
            df = read_parquet_to_pandas(file_path)
            
            if df.empty:
                logger.warning(f"Empty DataFrame from {file_path}")
                return 0
            
            # Clean the data
            df = self._clean_dataframe(df, table_name)
            
            # Add timestamps for non-blocks datasets
            if dataset != 'blocks' and 'block_number' in df.columns:
                self._add_timestamp_columns(df, table_name)
            
            # Final validation
            self._validate_dataframe(df, table_name)
            
            # Insert the data
            result = client.insert_df(f"{self.database}.{table_name}", df)
            
            # Handle the result
            if isinstance(result, QuerySummary):
                row_count = result.written_rows if hasattr(result, 'written_rows') else len(df)
            else:
                row_count = result or len(df)
                
            logger.info(f"Inserted {row_count} rows into {table_name}")
            return row_count
                
        except Exception as e:
            logger.error(f"Error inserting parquet file {file_path}: {e}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Clean and prepare DataFrame for insertion."""
        # Drop problematic columns for blocks table
        if table_name == 'blocks':
            columns_to_drop = ['total_difficulty_f64', 'total_difficulty_binary', 
                             'total_difficulty_string', 'difficulty']
            existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
            if existing_columns_to_drop:
                df = df.drop(columns=existing_columns_to_drop)
        
        # Normalize NULL values
        df = df.where(pd.notnull(df), None)
        
        # Replace empty strings with None
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: None if isinstance(x, str) and x.strip() == '' else x)
        
        # Convert binary columns to hex strings
        self._convert_binary_columns(df)
        
        # Remove insert_version if present (it's auto-generated)
        if 'insert_version' in df.columns:
            df = df.drop(columns=['insert_version'])
        
        return df
    
    def _add_timestamp_columns(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Add timestamp columns by joining with blocks table.
        STRICT: Fails if any blocks are missing.
        """
        try:
            unique_blocks = df['block_number'].dropna().unique()
            
            if len(unique_blocks) == 0:
                logger.warning(f"No valid block numbers found in {table_name} data")
                return
            
            # Get timestamps for all required blocks
            client = self._connect()
            blocks_str = ','.join(str(int(b)) for b in unique_blocks)
            
            query = f"""
            SELECT block_number, timestamp 
            FROM {self.database}.blocks 
            WHERE block_number IN ({blocks_str})
            AND timestamp IS NOT NULL
            AND timestamp > 0
            AND toDateTime(timestamp) > toDateTime('1971-01-01 00:00:00')
            """
            result = client.query(query)
            
            block_timestamps = {}
            for row in result.result_rows:
                block_timestamps[row[0]] = row[1]
            
            # Check for missing blocks
            missing_blocks = [b for b in unique_blocks if b not in block_timestamps]
            
            if missing_blocks:
                error_msg = (
                    f"CRITICAL: Cannot add timestamps for {table_name}! "
                    f"Missing valid timestamps for {len(missing_blocks)} blocks. "
                    f"Examples: {missing_blocks[:5]}. "
                    f"Process blocks first!"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Add timestamps
            df['block_timestamp'] = df['block_number'].apply(
                lambda x: pd.Timestamp.fromtimestamp(block_timestamps[x]) 
                if pd.notna(x) and x in block_timestamps else None
            )
            
            logger.debug(f"Successfully added timestamps to {len(df)} rows in {table_name}")
                
        except Exception as e:
            logger.error(f"Error adding timestamp columns: {e}")
            raise
    
    def _convert_binary_columns(self, df: pd.DataFrame) -> None:
        """Convert binary columns to hex strings."""
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].notna().any():
                first_non_null = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_non_null, bytes):
                    logger.debug(f"Converting binary column {col} to hex strings")
                    df[col] = df[col].apply(
                        lambda x: x.hex() if isinstance(x, bytes) and len(x) > 0 else None
                    )
    
    def _validate_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """Validate DataFrame before insertion."""
        # Check for timestamp issues in non-blocks tables
        if 'block_timestamp' in df.columns:
            null_timestamps = df['block_timestamp'].isna().sum()
            if null_timestamps > 0:
                raise ValueError(f"Cannot insert {table_name}: {null_timestamps} rows have NULL timestamps!")
            
            # Check for epoch timestamps
            epoch_timestamps = (df['block_timestamp'] <= pd.Timestamp('1971-01-01 00:00:00')).sum()
            if epoch_timestamps > 0:
                raise ValueError(f"Cannot insert {table_name}: {epoch_timestamps} rows have invalid timestamps!")
    
    def run_migrations(self, migrations_dir: str) -> bool:
        """Run SQL migrations from the provided directory."""
        try:
            client = self._connect()
            
            # Get all SQL files, sorted
            migration_files = sorted([
                f for f in os.listdir(migrations_dir)
                if f.endswith('.sql') and os.path.isfile(os.path.join(migrations_dir, f))
            ])
            
            logger.info(f"Found {len(migration_files)} migration files")
            
            # Check executed migrations
            try:
                executed_migrations = client.query(f"SELECT name FROM {self.database}.migrations")
                executed = set([row[0] for row in executed_migrations.result_rows])
            except Exception:
                executed = set()
            
            # Execute each migration
            for file_name in migration_files:
                if file_name in executed:
                    logger.debug(f"Migration {file_name} already executed, skipping")
                    continue
                
                logger.info(f"Executing migration: {file_name}")
                
                # Read and execute the SQL
                with open(os.path.join(migrations_dir, file_name), 'r') as f:
                    sql = f.read()
                
                # Replace database placeholder
                sql = sql.replace('{{database}}', self.database)
                
                # Execute statements
                statements = []
                current_statement = ""
                
                for line in sql.split('\n'):
                    if line.strip() and not line.strip().startswith('--'):
                        current_statement += line + '\n'
                        if line.strip().endswith(';'):
                            statements.append(current_statement.strip())
                            current_statement = ""
                
                if current_statement.strip():
                    statements.append(current_statement.strip())
                
                for statement in statements:
                    if statement and not statement.startswith('--'):
                        client.command(statement)
                
                logger.info(f"Migration {file_name} executed successfully")
            
            logger.info("All migrations completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error running migrations: {e}")
            return False
    
    def close(self) -> None:
        """Close all connections."""
        try:
            self.connection_pool.close_all()
        except Exception as e:
            logger.error(f"Error closing connections: {e}")