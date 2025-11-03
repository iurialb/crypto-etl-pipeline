"""
Unit tests for transformation modules
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.transform.metrics_calculator import MetricsCalculator
from src.transform.data_quality import DataQualityChecker


@pytest.fixture
def sample_current_prices():
    """Sample current price data"""
    return [
        {
            'id': 'bitcoin',
            'name': 'Bitcoin',
            'symbol': 'btc',
            'current_price': 43000,
            'market_cap': 840000000000,
            'total_volume': 25000000000,
            'price_change_percentage_24h': 2.5,
            'extracted_at': datetime.now().isoformat()
        },
        {
            'id': 'ethereum',
            'name': 'Ethereum',
            'symbol': 'eth',
            'current_price': 2300,
            'market_cap': 276000000000,
            'total_volume': 15000000000,
            'price_change_percentage_24h': 1.8,
            'extracted_at': datetime.now().isoformat()
        }
    ]


@pytest.fixture
def sample_global_data():
    """Sample global market data"""
    return {
        'total_market_cap': {'usd': 1500000000000},
        'total_volume': {'usd': 75000000000},
        'market_cap_percentage': {'btc': 56, 'eth': 18}
    }


@pytest.fixture
def sample_historical_data():
    """Sample historical data"""
    # Generate 30 days of price data
    timestamps = pd.date_range(end=datetime.now(), periods=30, freq='D')
    
    bitcoin_prices = [[int(ts.timestamp() * 1000), 40000 + i * 100 + np.random.randn() * 500] 
                      for i, ts in enumerate(timestamps)]
    bitcoin_volumes = [[int(ts.timestamp() * 1000), 20000000000 + np.random.randn() * 5000000000] 
                       for i, ts in enumerate(timestamps)]
    
    ethereum_prices = [[int(ts.timestamp() * 1000), 2000 + i * 10 + np.random.randn() * 50] 
                       for i, ts in enumerate(timestamps)]
    ethereum_volumes = [[int(ts.timestamp() * 1000), 10000000000 + np.random.randn() * 2000000000] 
                        for i, ts in enumerate(timestamps)]
    
    return [
        {
            'coin_id': 'bitcoin',
            'prices': bitcoin_prices,
            'total_volumes': bitcoin_volumes
        },
        {
            'coin_id': 'ethereum',
            'prices': ethereum_prices,
            'total_volumes': ethereum_volumes
        }
    ]


@pytest.fixture
def metrics_calculator():
    """Create metrics calculator instance"""
    return MetricsCalculator()


@pytest.fixture
def quality_checker():
    """Create data quality checker instance"""
    return DataQualityChecker()


def test_calculate_market_dominance(metrics_calculator, sample_current_prices, sample_global_data):
    """Test market dominance calculation"""
    result = metrics_calculator.calculate_market_dominance(
        sample_current_prices, sample_global_data
    )
    
    assert not result.empty
    assert 'market_dominance_pct' in result.columns
    assert 'dominance_rank' in result.columns
    assert len(result) == 2
    
    # Bitcoin should have higher dominance
    btc_row = result[result['id'] == 'bitcoin'].iloc[0]
    assert btc_row['market_dominance_pct'] > 0


def test_calculate_volatility(metrics_calculator, sample_historical_data):
    """Test volatility calculation"""
    result = metrics_calculator.calculate_volatility(sample_historical_data)
    
    assert not result.empty
    assert 'volatility_30d' in result.columns
    assert 'coin_id' in result.columns
    assert len(result) == 2


def test_calculate_correlation_matrix(metrics_calculator, sample_historical_data):
    """Test correlation matrix calculation"""
    result = metrics_calculator.calculate_correlation_matrix(sample_historical_data)
    
    assert not result.empty
    assert result.shape[0] == result.shape[1]  # Square matrix
    assert 'bitcoin' in result.columns
    assert 'ethereum' in result.columns


def test_calculate_sharpe_ratio(metrics_calculator, sample_historical_data):
    """Test Sharpe ratio calculation"""
    result = metrics_calculator.calculate_sharpe_ratio(sample_historical_data)
    
    assert not result.empty
    assert 'sharpe_ratio' in result.columns
    assert 'annualized_return' in result.columns
    assert 'annualized_volatility' in result.columns


def test_calculate_fear_greed_score(metrics_calculator, sample_current_prices, sample_historical_data):
    """Test Fear & Greed score calculation"""
    result = metrics_calculator.calculate_fear_greed_score(
        sample_current_prices, sample_historical_data
    )
    
    assert not result.empty
    assert 'fear_greed_score' in result.columns
    assert 'sentiment' in result.columns
    
    # Score should be between 0 and 100
    assert result['fear_greed_score'].min() >= 0
    assert result['fear_greed_score'].max() <= 100


def test_data_quality_null_check(quality_checker):
    """Test null value checking"""
    df = pd.DataFrame({
        'id': ['bitcoin', 'ethereum', 'cardano'],
        'price': [43000, 2300, None],
        'volume': [1000000, 500000, 250000]
    })
    
    passed, results = quality_checker.check_null_values(df)
    
    assert isinstance(passed, bool)
    assert 'null_counts' in results
    assert results['null_counts']['price'] == 1


def test_data_quality_price_validity(quality_checker):
    """Test price validity checking"""
    df = pd.DataFrame({
        'id': ['bitcoin', 'ethereum'],
        'current_price': [43000, -100]  # Negative price should fail
    })
    
    passed, results = quality_checker.check_price_validity(df)
    
    assert not passed
    assert results['invalid_count'] == 1


def test_data_quality_duplicates(quality_checker):
    """Test duplicate checking"""
    df = pd.DataFrame({
        'id': ['bitcoin', 'ethereum', 'bitcoin'],  # Duplicate
        'price': [43000, 2300, 43000]
    })
    
    passed, results = quality_checker.check_duplicate_records(df)
    
    assert not passed
    assert results['duplicate_count'] == 2  # Both bitcoin rows


def test_transform_all_data(metrics_calculator, sample_current_prices, sample_global_data, sample_historical_data):
    """Test complete transformation pipeline"""
    extracted_data = {
        'current_prices': sample_current_prices,
        'global_data': sample_global_data,
        'historical_data': sample_historical_data
    }
    
    result = metrics_calculator.transform_all_data(extracted_data)
    
    assert isinstance(result, dict)
    assert 'fact_crypto_metrics' in result
    assert 'market_dominance' in result
    assert 'volatility' in result
    assert 'sharpe_ratio' in result
    assert 'fear_greed' in result
    
    # Main fact table should have merged data
    fact_table = result['fact_crypto_metrics']
    assert not fact_table.empty