"""
CoinGecko API extractor module
"""
import requests
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from loguru import logger
from src.utils.config_loader import get_config
from src.utils.logger import log_execution_time


class CoinGeckoExtractor:
    """Extract cryptocurrency data from CoinGecko API"""
    
    def __init__(self):
        """Initialize CoinGecko API extractor"""
        self.config = get_config()
        self.api_config = self.config.api_config
        self.base_url = self.api_config['base_url']
        self.timeout = self.api_config['timeout']
        self.retry_attempts = self.api_config['retry_attempts']
        self.retry_delay = self.api_config['retry_delay']
        
        logger.info("CoinGecko Extractor initialized", extra={
            "base_url": self.base_url,
            "timeout": self.timeout
        })
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Make HTTP request to CoinGecko API with retry logic
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response or None if failed
        """
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(self.retry_attempts):
            try:
                logger.debug(f"Making request to {endpoint}", extra={
                    "attempt": attempt + 1,
                    "params": params
                })
                
                response = requests.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                
                logger.info(f"Successfully fetched data from {endpoint}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.retry_attempts})", extra={
                    "endpoint": endpoint,
                    "error": str(e)
                })
                
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"All retry attempts failed for {endpoint}")
                    return None
    
    @log_execution_time
    def get_current_prices(self, coin_ids: List[str], vs_currency: str = 'usd') -> List[Dict[str, Any]]:
        """
        Get current price data for cryptocurrencies
        
        Args:
            coin_ids: List of cryptocurrency IDs
            vs_currency: Currency to compare against (default: usd)
            
        Returns:
            List of price data dictionaries
        """
        endpoint = "coins/markets"
        params = {
            'ids': ','.join(coin_ids),
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': 250,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h,7d,30d'
        }
        
        data = self._make_request(endpoint, params)
        
        if data:
            logger.info(f"Retrieved price data for {len(data)} cryptocurrencies")
            # Add extraction timestamp
            for item in data:
                item['extracted_at'] = datetime.now().isoformat()
            return data
        
        return []
    
    @log_execution_time
    def get_historical_data(
        self, 
        coin_id: str, 
        vs_currency: str = 'usd',
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get historical market data for a cryptocurrency
        
        Args:
            coin_id: Cryptocurrency ID
            vs_currency: Currency to compare against
            days: Number of days of historical data
            
        Returns:
            Dictionary with historical price, market cap, and volume data
        """
        endpoint = f"coins/{coin_id}/market_chart"
        params = {
            'vs_currency': vs_currency,
            'days': days,
            'interval': 'daily'
        }
        
        data = self._make_request(endpoint, params)
        
        if data:
            logger.info(f"Retrieved {days} days of historical data for {coin_id}")
            return {
                'coin_id': coin_id,
                'vs_currency': vs_currency,
                'days': days,
                'prices': data.get('prices', []),
                'market_caps': data.get('market_caps', []),
                'total_volumes': data.get('total_volumes', []),
                'extracted_at': datetime.now().isoformat()
            }
        
        return {}
    
    @log_execution_time
    def get_global_data(self) -> Dict[str, Any]:
        """
        Get global cryptocurrency market data
        
        Returns:
            Dictionary with global market statistics
        """
        endpoint = "global"
        data = self._make_request(endpoint)
        
        if data and 'data' in data:
            global_data = data['data']
            logger.info("Retrieved global market data", extra={
                "total_market_cap_usd": global_data.get('total_market_cap', {}).get('usd'),
                "total_volume_usd": global_data.get('total_volume', {}).get('usd')
            })
            global_data['extracted_at'] = datetime.now().isoformat()
            return global_data
        
        return {}
    
    @log_execution_time
    def extract_all_data(self) -> Dict[str, Any]:
        """
        Extract all data: current prices, global data, and recent historical data
        
        Returns:
            Dictionary containing all extracted data
        """
        coin_ids = self.config.cryptocurrencies
        vs_currency = self.config.vs_currency
        
        logger.info(f"Starting full data extraction for {len(coin_ids)} cryptocurrencies")
        
        # Get current prices
        current_prices = self.get_current_prices(coin_ids, vs_currency)
        
        # Get global data
        global_data = self.get_global_data()
        
        # Get historical data for correlation analysis (last 30 days)
        historical_data = []
        for coin_id in coin_ids:
            hist = self.get_historical_data(coin_id, vs_currency, days=30)
            if hist:
                historical_data.append(hist)
            # Rate limiting - be nice to the API
            time.sleep(1)
        
        result = {
            'current_prices': current_prices,
            'global_data': global_data,
            'historical_data': historical_data,
            'extraction_timestamp': datetime.now().isoformat(),
            'total_coins': len(current_prices)
        }
        
        logger.info("Full data extraction completed", extra={
            "coins_extracted": len(current_prices),
            "historical_datasets": len(historical_data)
        })
        
        return result