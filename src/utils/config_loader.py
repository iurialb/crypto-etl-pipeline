"""
Configuration loader utility
"""
import yaml
import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


class ConfigLoader:
    """Load and manage configuration from YAML and environment variables"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize configuration loader
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = Path(config_path)
        self._config = None
        self._load_env()
        self._load_yaml()
    
    def _load_env(self):
        """Load environment variables from .env file"""
        env_path = Path(".env")
        if env_path.exists():
            load_dotenv(env_path)
    
    def _load_yaml(self):
        """Load YAML configuration file"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self._config = yaml.safe_load(f)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports nested keys with dot notation)
        
        Args:
            key: Configuration key (e.g., 'database.host' or 'extraction.batch_size')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary"""
        return self._config.copy()
    
    @property
    def cryptocurrencies(self) -> list:
        """Get list of cryptocurrencies to track"""
        return self.get('cryptocurrencies', [])
    
    @property
    def vs_currency(self) -> str:
        """Get vs_currency setting"""
        return self.get('vs_currency', 'usd')
    
    @property
    def database_config(self) -> Dict[str, Any]:
        """Get database configuration from environment variables"""
        return {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'crypto_etl'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
        }
    
    @property
    def api_config(self) -> Dict[str, Any]:
        """Get API configuration"""
        return {
            'base_url': os.getenv('COINGECKO_API_URL', 'https://api.coingecko.com/api/v3'),
            'timeout': self.get('extraction.timeout', 30),
            'retry_attempts': self.get('extraction.retry_attempts', 3),
            'retry_delay': self.get('extraction.retry_delay', 5),
        }


# Singleton instance
_config_instance = None


def get_config() -> ConfigLoader:
    """Get singleton configuration instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigLoader()
    return _config_instance