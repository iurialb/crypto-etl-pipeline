"""
Advanced metrics calculator for cryptocurrency data
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
from src.utils.config_loader import get_config
from src.utils.logger import log_execution_time


class MetricsCalculator:
    """Calculate advanced cryptocurrency metrics"""
    
    def __init__(self):
        """Initialize metrics calculator"""
        self.config = get_config()
        self.metrics_config = self.config.get('metrics', {})
        logger.info("Metrics Calculator initialized")
    
    @log_execution_time
    def calculate_market_dominance(self, current_prices: List[Dict], global_data: Dict) -> pd.DataFrame:
        """
        Calculate market dominance for each cryptocurrency
        
        Market Dominance = (Coin Market Cap / Total Market Cap) * 100
        
        Args:
            current_prices: List of current price data
            global_data: Global market data
            
        Returns:
            DataFrame with market dominance metrics
        """
        df = pd.DataFrame(current_prices)
        
        if df.empty or not global_data:
            logger.warning("Empty data provided for market dominance calculation")
            return pd.DataFrame()
        
        total_market_cap = global_data.get('total_market_cap', {}).get('usd', 0)
        
        if total_market_cap == 0:
            logger.error("Total market cap is zero, cannot calculate dominance")
            return pd.DataFrame()
        
        df['market_dominance_pct'] = (df['market_cap'] / total_market_cap) * 100
        df['dominance_rank'] = df['market_dominance_pct'].rank(ascending=False)
        
        logger.info(f"Calculated market dominance for {len(df)} cryptocurrencies")
        
        return df[['id', 'name', 'symbol', 'market_cap', 'market_dominance_pct', 'dominance_rank']]
    
    @log_execution_time
    def calculate_volatility(self, historical_data: List[Dict]) -> pd.DataFrame:
        """
        Calculate realized volatility for cryptocurrencies
        
        Volatility = Standard Deviation of Daily Returns
        
        Args:
            historical_data: List of historical price data
            
        Returns:
            DataFrame with volatility metrics
        """
        volatility_results = []
        window = self.metrics_config.get('volatility_window', 7)
        
        for coin_data in historical_data:
            coin_id = coin_data.get('coin_id')
            prices = coin_data.get('prices', [])
            
            if not prices or len(prices) < 2:
                continue
            
            # Convert to DataFrame
            df = pd.DataFrame(prices, columns=['timestamp', 'price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp')
            
            # Calculate daily returns
            df['returns'] = df['price'].pct_change()
            
            # Calculate volatility (annualized)
            volatility = df['returns'].std() * np.sqrt(365)
            volatility_7d = df['returns'].tail(window).std() * np.sqrt(365)
            
            # Calculate price momentum
            price_change_pct = ((df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]) * 100
            
            volatility_results.append({
                'coin_id': coin_id,
                'volatility_30d': volatility,
                f'volatility_{window}d': volatility_7d,
                'price_change_30d_pct': price_change_pct,
                'avg_price_30d': df['price'].mean(),
                'max_price_30d': df['price'].max(),
                'min_price_30d': df['price'].min()
            })
        
        result_df = pd.DataFrame(volatility_results)
        logger.info(f"Calculated volatility for {len(result_df)} cryptocurrencies")
        
        return result_df
    
    @log_execution_time
    def calculate_correlation_matrix(self, historical_data: List[Dict]) -> pd.DataFrame:
        """
        Calculate correlation matrix between cryptocurrencies
        
        Args:
            historical_data: List of historical price data
            
        Returns:
            Correlation matrix DataFrame
        """
        # Build a dataframe with all prices aligned by timestamp
        all_prices = {}
        
        for coin_data in historical_data:
            coin_id = coin_data.get('coin_id')
            prices = coin_data.get('prices', [])
            
            if not prices:
                continue
            
            df = pd.DataFrame(prices, columns=['timestamp', 'price'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.set_index('timestamp')
            all_prices[coin_id] = df['price']
        
        if not all_prices:
            logger.warning("No price data available for correlation calculation")
            return pd.DataFrame()
        
        # Combine into single DataFrame
        prices_df = pd.DataFrame(all_prices)
        
        # Calculate returns
        returns_df = prices_df.pct_change().dropna()
        
        # Calculate correlation matrix
        correlation_matrix = returns_df.corr()
        
        logger.info(f"Calculated correlation matrix for {len(correlation_matrix)} cryptocurrencies")
        
        return correlation_matrix
    
    @log_execution_time
    def calculate_sharpe_ratio(self, historical_data: List[Dict]) -> pd.DataFrame:
        """
        Calculate Sharpe Ratio (risk-adjusted return)
        
        Sharpe Ratio = (Return - Risk Free Rate) / Standard Deviation
        
        Args:
            historical_data: List of historical price data
            
        Returns:
            DataFrame with Sharpe ratios
        """
        risk_free_rate = self.metrics_config.get('sharpe_ratio_risk_free_rate', 0.02)
        sharpe_results = []
        
        for coin_data in historical_data:
            coin_id = coin_data.get('coin_id')
            prices = coin_data.get('prices', [])
            
            if not prices or len(prices) < 2:
                continue
            
            df = pd.DataFrame(prices, columns=['timestamp', 'price'])
            df['returns'] = df['price'].pct_change()
            
            # Annualized return
            total_return = (df['price'].iloc[-1] - df['price'].iloc[0]) / df['price'].iloc[0]
            days = len(df)
            annualized_return = (1 + total_return) ** (365 / days) - 1
            
            # Annualized volatility
            volatility = df['returns'].std() * np.sqrt(365)
            
            # Sharpe ratio
            if volatility > 0:
                sharpe_ratio = (annualized_return - risk_free_rate) / volatility
            else:
                sharpe_ratio = 0
            
            sharpe_results.append({
                'coin_id': coin_id,
                'annualized_return': annualized_return,
                'annualized_volatility': volatility,
                'sharpe_ratio': sharpe_ratio
            })
        
        result_df = pd.DataFrame(sharpe_results)
        logger.info(f"Calculated Sharpe ratio for {len(result_df)} cryptocurrencies")
        
        return result_df
    
    @log_execution_time
    def calculate_fear_greed_score(
        self, 
        current_prices: List[Dict], 
        historical_data: List[Dict]
    ) -> pd.DataFrame:
        """
        Calculate custom Fear & Greed score based on volatility and volume
        
        Score components:
        - Price momentum (30%)
        - Volatility (30%) - inverse (low volatility = greed)
        - Volume change (40%)
        
        Score: 0-100 (0 = Extreme Fear, 100 = Extreme Greed)
        
        Args:
            current_prices: Current price data
            historical_data: Historical data for calculations
            
        Returns:
            DataFrame with Fear & Greed scores
        """
        scores = []
        
        # Create lookup for current data
        current_lookup = {item['id']: item for item in current_prices}
        
        for coin_data in historical_data:
            coin_id = coin_data.get('coin_id')
            prices = coin_data.get('prices', [])
            volumes = coin_data.get('total_volumes', [])
            
            if not prices or len(prices) < 7:
                continue
            
            current = current_lookup.get(coin_id, {})
            
            # Price momentum score (0-100)
            price_df = pd.DataFrame(prices, columns=['timestamp', 'price'])
            price_change = ((price_df['price'].iloc[-1] - price_df['price'].iloc[0]) / 
                           price_df['price'].iloc[0]) * 100
            # Normalize: -50% to +50% -> 0 to 100
            momentum_score = min(max((price_change + 50), 0), 100)
            
            # Volatility score (0-100, inverse)
            returns = price_df['price'].pct_change()
            volatility = returns.std()
            # Lower volatility = higher score (greed)
            volatility_score = max(0, 100 - (volatility * 1000))
            
            # Volume score (0-100)
            if volumes and len(volumes) > 7:
                volume_df = pd.DataFrame(volumes, columns=['timestamp', 'volume'])
                recent_vol = volume_df['volume'].tail(7).mean()
                older_vol = volume_df['volume'].head(7).mean()
                
                if older_vol > 0:
                    volume_change = ((recent_vol - older_vol) / older_vol) * 100
                    volume_score = min(max((volume_change + 50), 0), 100)
                else:
                    volume_score = 50
            else:
                volume_score = 50
            
            # Calculate weighted Fear & Greed score
            fg_score = (momentum_score * 0.3 + 
                       volatility_score * 0.3 + 
                       volume_score * 0.4)
            
            # Classify sentiment
            if fg_score < 20:
                sentiment = "Extreme Fear"
            elif fg_score < 40:
                sentiment = "Fear"
            elif fg_score < 60:
                sentiment = "Neutral"
            elif fg_score < 80:
                sentiment = "Greed"
            else:
                sentiment = "Extreme Greed"
            
            scores.append({
                'coin_id': coin_id,
                'fear_greed_score': fg_score,
                'sentiment': sentiment,
                'momentum_component': momentum_score,
                'volatility_component': volatility_score,
                'volume_component': volume_score
            })
        
        result_df = pd.DataFrame(scores)
        logger.info(f"Calculated Fear & Greed scores for {len(result_df)} cryptocurrencies")
        
        return result_df
    
    @log_execution_time
    def transform_all_data(self, extracted_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Apply all transformations to extracted data
        
        Args:
            extracted_data: Dictionary with extracted data from API
            
        Returns:
            Dictionary with all transformed DataFrames
        """
        logger.info("Starting full data transformation")
        
        current_prices = extracted_data.get('current_prices', [])
        global_data = extracted_data.get('global_data', {})
        historical_data = extracted_data.get('historical_data', [])
        
        transformed = {}
        
        # Calculate all metrics
        transformed['market_dominance'] = self.calculate_market_dominance(
            current_prices, global_data
        )
        
        transformed['volatility'] = self.calculate_volatility(historical_data)
        
        transformed['correlation_matrix'] = self.calculate_correlation_matrix(historical_data)
        
        transformed['sharpe_ratio'] = self.calculate_sharpe_ratio(historical_data)
        
        transformed['fear_greed'] = self.calculate_fear_greed_score(
            current_prices, historical_data
        )
        
        # Create main fact table by merging all metrics
        main_df = pd.DataFrame(current_prices)
        
        for key, df in transformed.items():
            if key == 'correlation_matrix':
                continue  # Skip correlation matrix for main table
            
            if not df.empty and 'coin_id' in df.columns:
                main_df = main_df.merge(
                    df, 
                    left_on='id', 
                    right_on='coin_id', 
                    how='left',
                    suffixes=('', '_dup')
                )
                # Remove duplicate coin_id column
                if 'coin_id' in main_df.columns:
                    main_df = main_df.drop('coin_id', axis=1)
        
        transformed['fact_crypto_metrics'] = main_df
        
        logger.info(f"Transformation complete. Generated {len(transformed)} tables")
        
        return transformed