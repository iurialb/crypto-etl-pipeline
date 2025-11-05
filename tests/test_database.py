"""
Unit tests for database module

Note: These tests require a PostgreSQL database to be running.
Set TEST_DATABASE environment variable to run against a test database.
"""
import pytest
import pandas as pd
from datetime import date
import os


# Skip all tests if no test database is configured
pytestmark = pytest.mark.skipif(
    os.getenv('TEST_DATABASE') != 'true',
    reason="Database tests skipped. Set TEST_DATABASE=true to run"
)


@pytest.fixture
def db_manager():
    """Create database manager for testing"""
    from src.load.database import DatabaseManager
    
    manager = DatabaseManager()
    yield manager
    manager.close()


@pytest.fixture
def sample_crypto_df():
    """Sample cryptocurrency data"""
    return pd.DataFrame({
        'id': ['bitcoin', 'ethereum'],
        'name': ['Bitcoin', 'Ethereum'],
        'symbol': ['btc', 'eth'],
        'current_price': [43000, 2300],
        'market_cap': [840000000000, 276000000000],
        'total_volume': [25000000000, 15000000000]
    })


def test_database_connection(db_manager):
    """Test database connection"""
    assert db_manager.engine is not None
    
    # Test query execution
    result = db_manager.query("SELECT 1 as test")
    assert not result.empty
    assert result.iloc[0]['test'] == 1


def test_table_exists(db_manager):
    """Test checking if table exists"""
    # Should return boolean
    exists = db_manager.table_exists('dim_cryptocurrency')
    assert isinstance(exists, bool)


def test_upsert_dimension(db_manager, sample_crypto_df):
    """Test upserting dimension data"""
    count = db_manager.upsert_dimension_cryptocurrencies(sample_crypto_df)
    assert count == 2
    
    # Verify data was inserted
    result = db_manager.query("SELECT * FROM dim_cryptocurrency WHERE coin_id IN ('bitcoin', 'ethereum')")
    assert len(result) == 2


def test_insert_fact_metrics(db_manager, sample_crypto_df):
    """Test inserting fact metrics"""
    test_date = date(2024, 1, 1)
    count = db_manager.insert_fact_crypto_metrics(sample_crypto_df, test_date)
    assert count > 0
    
    # Verify data was inserted
    result = db_manager.query(
        "SELECT * FROM fact_crypto_metrics WHERE extracted_date = :date",
        params={'date': test_date}
    )
    assert len(result) == 2


def test_log_etl_run(db_manager):
    """Test logging ETL run"""
    run_id = db_manager.log_etl_run(
        status='SUCCESS',
        coins_processed=10,
        records_inserted=100,
        execution_time=15.5
    )
    
    assert run_id is not None
    
    # Verify log was created
    result = db_manager.query(
        "SELECT * FROM etl_run_log WHERE run_id = :run_id",
        params={'run_id': run_id}
    )
    assert len(result) == 1
    assert result.iloc[0]['status'] == 'SUCCESS'


def test_insert_correlation_matrix(db_manager):
    """Test inserting correlation matrix"""
    # Create sample correlation matrix
    corr_matrix = pd.DataFrame(
        [[1.0, 0.85], [0.85, 1.0]],
        index=['bitcoin', 'ethereum'],
        columns=['bitcoin', 'ethereum']
    )
    
    test_date = date(2024, 1, 1)
    count = db_manager.insert_correlation_matrix(corr_matrix, test_date)
    
    # Should insert 2 records (excluding diagonal)
    assert count == 2


def test_idempotency(db_manager, sample_crypto_df):
    """Test that running twice doesn't duplicate data"""
    test_date = date(2024, 1, 1)
    
    # Insert first time
    count1 = db_manager.insert_fact_crypto_metrics(sample_crypto_df, test_date)
    
    # Insert again with same date
    count2 = db_manager.insert_fact_crypto_metrics(sample_crypto_df, test_date)
    
    # Both should succeed
    assert count1 > 0
    assert count2 > 0
    
    # Check there are no duplicates
    result = db_manager.query(
        "SELECT COUNT(*) as count FROM fact_crypto_metrics WHERE extracted_date = :date",
        params={'date': test_date}
    )
    assert result.iloc[0]['count'] == len(sample_crypto_df)