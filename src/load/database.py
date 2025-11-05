"""
Database connection and operations module
"""
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import NullPool
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pathlib import Path
from loguru import logger
from src.utils.config_loader import get_config
from src.utils.logger import log_execution_time


class DatabaseManager:
    """Manage database connections and operations"""
    
    def __init__(self):
        """Initialize database manager"""
        self.config = get_config()
        self.db_config = self.config.database_config
        self.engine = None
        self._connect()
        
        logger.info("Database Manager initialized")
    
    def _connect(self):
        """Create database connection"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            
            self.engine = create_engine(
                connection_string,
                poolclass=NullPool,  # Disable connection pooling for simplicity
                echo=False
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Connected to database: {self.db_config['database']}")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def _split_sql_statements(self, sql: str) -> List[str]:
        """
        Split SQL into statements, respecting dollar-quoted strings
        
        Args:
            sql: SQL string to split
            
        Returns:
            List of SQL statements
        """
        statements = []
        current_statement = []
        in_dollar_quote = False
        dollar_tag = None
        
        i = 0
        while i < len(sql):
            # If we're inside a dollar quote, check for end first
            if in_dollar_quote:
                # Safety check: dollar_tag should be defined
                if dollar_tag is None:
                    raise ValueError("Invalid state: in_dollar_quote is True but dollar_tag is None")
                
                # Check if we've reached the end of the dollar quote
                tag_len = len(dollar_tag)
                if i + tag_len <= len(sql) and sql[i:i+tag_len] == dollar_tag:
                    current_statement.append(dollar_tag)
                    in_dollar_quote = False
                    i += tag_len
                    dollar_tag = None
                    continue
                else:
                    # Still inside dollar quote, add character
                    current_statement.append(sql[i])
                    i += 1
                    continue
            
            # Not in dollar quote, check for start
            if sql[i] == '$':
                # Find the full dollar tag (could be $$ or $tag$)
                tag_start = i
                i += 1
                # Collect tag name (between $ and $)
                while i < len(sql) and sql[i] != '$':
                    i += 1
                if i < len(sql):
                    # Found closing $, we have a complete dollar tag
                    dollar_tag = sql[tag_start:i+1]
                    in_dollar_quote = True
                    current_statement.append(dollar_tag)
                    i += 1
                    continue
                else:
                    # No closing $ found, treat as regular character
                    current_statement.append(sql[tag_start])
                    i = tag_start + 1
                    continue
            
            # Regular character processing
            char = sql[i]
            current_statement.append(char)
            
            # If we hit a semicolon (and not in dollar quote), it's a statement separator
            if char == ';':
                statement = ''.join(current_statement).strip()
                if statement:
                    statements.append(statement)
                current_statement = []
            
            i += 1
        
        # Add any remaining statement
        if current_statement:
            statement = ''.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        return statements
    
    def initialize_schema(self, schema_file: str = "sql/schema.sql"):
        """
        Initialize database schema from SQL file
        
        Args:
            schema_file: Path to schema SQL file
        """
        schema_path = Path(schema_file)
        
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_file}")
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        try:
            with self.engine.begin() as conn:
                # Split SQL statements respecting dollar-quoted strings
                statements = self._split_sql_statements(schema_sql)
                
                for statement in statements:
                    if statement.strip():
                        conn.execute(text(statement))
            
            logger.info("Database schema initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize schema: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table exists
        """
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    @log_execution_time
    def upsert_dimension_cryptocurrencies(self, df: pd.DataFrame) -> int:
        """
        Insert or update cryptocurrency dimension data
        
        Args:
            df: DataFrame with cryptocurrency data
            
        Returns:
            Number of records processed
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for cryptocurrency dimension")
            return 0
        
        # Prepare data
        dim_data = df[['id', 'name', 'symbol']].copy()
        dim_data.columns = ['coin_id', 'name', 'symbol']
        dim_data = dim_data.drop_duplicates(subset=['coin_id'])
        
        inserted = 0
        
        with self.engine.begin() as conn:
            for _, row in dim_data.iterrows():
                # Insert or update (ON CONFLICT DO UPDATE)
                sql = text("""
                    INSERT INTO dim_cryptocurrency (coin_id, name, symbol)
                    VALUES (:coin_id, :name, :symbol)
                    ON CONFLICT (coin_id) 
                    DO UPDATE SET 
                        name = EXCLUDED.name,
                        symbol = EXCLUDED.symbol,
                        last_updated = CURRENT_TIMESTAMP
                """)
                
                conn.execute(sql, {
                    'coin_id': row['coin_id'],
                    'name': row['name'],
                    'symbol': row['symbol']
                })
                inserted += 1
        
        logger.info(f"Upserted {inserted} cryptocurrency records")
        return inserted
    
    @log_execution_time
    def insert_fact_crypto_metrics(self, df: pd.DataFrame, extracted_date: date = None) -> int:
        """
        Insert main fact table data
        
        Args:
            df: DataFrame with crypto metrics
            extracted_date: Date of extraction (default: today)
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for fact metrics")
            return 0
        
        if extracted_date is None:
            extracted_date = date.today()
        
        # First ensure dimension table is updated
        self.upsert_dimension_cryptocurrencies(df)
        
        # Prepare fact data
        fact_columns = [
            'id', 'current_price', 'market_cap', 'total_volume', 
            'circulating_supply', 'price_change_24h', 'price_change_7d', 
            'price_change_30d', 'price_change_percentage_24h',
            'price_change_percentage_7d', 'price_change_percentage_30d',
            'market_dominance_pct', 'dominance_rank', 'volatility_7d',
            'volatility_30d', 'sharpe_ratio', 'annualized_return',
            'annualized_volatility', 'fear_greed_score', 'sentiment',
            'ath', 'ath_change_percentage', 'ath_date',
            'atl', 'atl_change_percentage', 'atl_date'
        ]
        
        # Only use columns that exist
        existing_columns = [col for col in fact_columns if col in df.columns]
        fact_data = df[existing_columns].copy()
        
        # Add metadata
        fact_data['extracted_date'] = extracted_date
        fact_data['extracted_timestamp'] = datetime.now()
        
        # Rename id to coin_id
        fact_data = fact_data.rename(columns={'id': 'coin_id'})
        
        # Delete existing records for this date (idempotency)
        with self.engine.begin() as conn:
            delete_sql = text("""
                DELETE FROM fact_crypto_metrics 
                WHERE extracted_date = :extracted_date
            """)
            result = conn.execute(delete_sql, {'extracted_date': extracted_date})
            deleted = result.rowcount
            if deleted > 0:
                logger.info(f"Deleted {deleted} existing records for {extracted_date}")
        
        # Insert new records
        inserted = fact_data.to_sql(
            'fact_crypto_metrics',
            self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Inserted {inserted} fact metric records for {extracted_date}")
        return inserted
    
    @log_execution_time
    def insert_correlation_matrix(self, correlation_df: pd.DataFrame, extracted_date: date = None) -> int:
        """
        Insert correlation matrix data
        
        Args:
            correlation_df: Correlation matrix DataFrame
            extracted_date: Date of extraction
            
        Returns:
            Number of records inserted
        """
        if correlation_df.empty:
            logger.warning("Empty correlation matrix provided")
            return 0
        
        if extracted_date is None:
            extracted_date = date.today()
        
        # Convert correlation matrix to long format
        correlation_long = []
        
        for coin_1 in correlation_df.index:
            for coin_2 in correlation_df.columns:
                if coin_1 != coin_2:  # Skip diagonal
                    correlation_long.append({
                        'extracted_date': extracted_date,
                        'coin_id_1': coin_1,
                        'coin_id_2': coin_2,
                        'correlation_coefficient': correlation_df.loc[coin_1, coin_2]
                    })
        
        corr_df = pd.DataFrame(correlation_long)
        
        # Delete existing records for this date
        with self.engine.begin() as conn:
            delete_sql = text("""
                DELETE FROM correlation_matrix 
                WHERE extracted_date = :extracted_date
            """)
            conn.execute(delete_sql, {'extracted_date': extracted_date})
        
        # Insert new records
        inserted = corr_df.to_sql(
            'correlation_matrix',
            self.engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"Inserted {inserted} correlation records")
        return inserted
    
    @log_execution_time
    def log_etl_run(
        self, 
        status: str,
        coins_processed: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        execution_time: float = 0,
        error_message: str = None,
        metadata: Dict = None
    ) -> int:
        """
        Log ETL run information
        
        Args:
            status: Status of ETL run (SUCCESS, FAILED, RUNNING)
            coins_processed: Number of coins processed
            records_inserted: Number of records inserted
            records_updated: Number of records updated
            execution_time: Execution time in seconds
            error_message: Error message if failed
            metadata: Additional metadata as JSON
            
        Returns:
            Run ID
        """
        log_data = pd.DataFrame([{
            'run_timestamp': datetime.now(),
            'status': status,
            'coins_processed': coins_processed,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'execution_time_seconds': execution_time,
            'error_message': error_message,
            'metadata': str(metadata) if metadata else None
        }])
        
        log_data.to_sql(
            'etl_run_log',
            self.engine,
            if_exists='append',
            index=False
        )
        
        logger.info(f"ETL run logged: {status}")
        
        # Get the run_id of the inserted record
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(run_id) as run_id FROM etl_run_log"))
            run_id = result.fetchone()[0]
        
        return run_id
    
    def query(self, sql: str, params: Dict = None) -> pd.DataFrame:
        """
        Execute a query and return results as DataFrame
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame with query results
        """
        try:
            df = pd.read_sql(sql, self.engine, params=params)
            return df
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")