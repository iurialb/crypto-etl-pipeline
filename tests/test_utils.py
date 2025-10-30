"""
Unit tests for utility modules
"""
import pytest
from src.utils.config_loader import ConfigLoader, get_config
from src.utils.logger import setup_logger, log_execution_time
from loguru import logger
import time


def test_config_loader_initialization():
    """Test that config loader initializes correctly"""
    config = ConfigLoader()
    assert config is not None
    assert config.get_all() is not None


def test_config_get_cryptocurrencies():
    """Test getting cryptocurrency list from config"""
    config = get_config()
    cryptos = config.cryptocurrencies
    assert isinstance(cryptos, list)
    assert len(cryptos) > 0
    assert 'bitcoin' in cryptos


def test_config_get_nested_value():
    """Test getting nested configuration values"""
    config = get_config()
    batch_size = config.get('extraction.batch_size')
    assert batch_size is not None
    assert isinstance(batch_size, int)


def test_config_get_with_default():
    """Test getting non-existent key with default value"""
    config = get_config()
    value = config.get('non.existent.key', 'default_value')
    assert value == 'default_value'


def test_logger_setup():
    """Test logger setup without errors"""
    test_logger = setup_logger(log_level="INFO", format_type="text")
    assert test_logger is not None


def test_log_execution_time_decorator():
    """Test the execution time logging decorator"""
    
    @log_execution_time
    def dummy_function():
        time.sleep(0.1)
        return "success"
    
    result = dummy_function()
    assert result == "success"


def test_singleton_config():
    """Test that get_config returns the same instance"""
    config1 = get_config()
    config2 = get_config()
    assert config1 is config2