"""
Script to manually test data extraction from CoinGecko API
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract.coingecko_api import CoinGeckoExtractor
from loguru import logger


def main():
    """Test extraction functionality"""
    logger.info("=" * 50)
    logger.info("Testing CoinGecko Data Extraction")
    logger.info("=" * 50)
    
    # Initialize extractor
    extractor = CoinGeckoExtractor()
    
    # Test 1: Get current prices for a few coins
    logger.info("\n1. Testing current prices extraction...")
    test_coins = ['bitcoin', 'ethereum', 'cardano']
    prices = extractor.get_current_prices(test_coins)
    
    if prices:
        logger.info(f"✓ Successfully extracted {len(prices)} coins")
        for coin in prices:
            logger.info(f"  - {coin['name']}: ${coin['current_price']:,.2f} (Market Cap: ${coin['market_cap']:,.0f})")
    else:
        logger.error("✗ Failed to extract current prices")
    
    # Test 2: Get global market data
    logger.info("\n2. Testing global market data extraction...")
    global_data = extractor.get_global_data()
    
    if global_data:
        logger.info("✓ Successfully extracted global data")
        if 'total_market_cap' in global_data:
            total_mc = global_data['total_market_cap'].get('usd', 0)
            logger.info(f"  - Total Market Cap: ${total_mc:,.0f}")
        if 'total_volume' in global_data:
            total_vol = global_data['total_volume'].get('usd', 0)
            logger.info(f"  - 24h Volume: ${total_vol:,.0f}")
    else:
        logger.error("✗ Failed to extract global data")
    
    # Test 3: Get historical data
    logger.info("\n3. Testing historical data extraction...")
    historical = extractor.get_historical_data('bitcoin', days=7)
    
    if historical and historical.get('prices'):
        logger.info(f"✓ Successfully extracted {len(historical['prices'])} days of historical data")
        logger.info(f"  - Latest price: ${historical['prices'][-1][1]:,.2f}")
    else:
        logger.error("✗ Failed to extract historical data")
    
    # Test 4: Full extraction (commented out to avoid rate limiting during tests)
    # logger.info("\n4. Testing full data extraction...")
    # all_data = extractor.extract_all_data()
    # logger.info(f"✓ Full extraction completed: {all_data['total_coins']} coins")
    
    logger.info("\n" + "=" * 50)
    logger.info("Extraction tests completed!")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()