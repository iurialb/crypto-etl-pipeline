"""
Unit tests for CoinGecko extractor
"""
import pytest
from src.extract.coingecko_api import CoinGeckoExtractor
from unittest.mock import patch, MagicMock


@pytest.fixture
def extractor():
    """Create extractor instance for testing"""
    return CoinGeckoExtractor()


def test_extractor_initialization(extractor):
    """Test that extractor initializes correctly"""
    assert extractor is not None
    assert extractor.base_url is not None
    assert extractor.timeout > 0


def test_get_current_prices_structure(extractor):
    """Test that get_current_prices returns correct structure"""
    # This makes a real API call - use with caution
    coin_ids = ['bitcoin', 'ethereum']
    result = extractor.get_current_prices(coin_ids)
    
    assert isinstance(result, list)
    if len(result) > 0:
        # Check first item has expected fields
        assert 'id' in result[0]
        assert 'current_price' in result[0]
        assert 'market_cap' in result[0]
        assert 'extracted_at' in result[0]


def test_get_global_data_structure(extractor):
    """Test that get_global_data returns correct structure"""
    result = extractor.get_global_data()
    
    assert isinstance(result, dict)
    if result:
        assert 'total_market_cap' in result or 'extracted_at' in result


@patch('src.extract.coingecko_api.requests.get')
def test_make_request_with_retry(mock_get, extractor):
    """Test retry logic on failed requests"""
    import requests
    # Simulate failed requests using proper requests exception
    mock_get.side_effect = requests.exceptions.RequestException("Connection error")
    
    result = extractor._make_request("test_endpoint")
    
    # Should have tried multiple times
    assert mock_get.call_count == extractor.retry_attempts
    assert result is None


@patch('src.extract.coingecko_api.requests.get')
def test_make_request_success(mock_get, extractor):
    """Test successful request"""
    # Simulate successful request
    mock_response = MagicMock()
    mock_response.json.return_value = {'data': 'test'}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extractor._make_request("test_endpoint")
    
    assert result == {'data': 'test'}
    assert mock_get.call_count == 1


def test_get_historical_data_structure(extractor):
    """Test historical data structure"""
    result = extractor.get_historical_data('bitcoin', days=7)
    
    assert isinstance(result, dict)
    if result:
        assert 'coin_id' in result
        assert 'prices' in result
        assert 'extracted_at' in result