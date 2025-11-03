"""
Script to test data transformation and quality checks
"""
import sys
import os
from pathlib import Path

# Setup path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from src.extract.coingecko_api import CoinGeckoExtractor
from src.transform.metrics_calculator import MetricsCalculator
from src.transform.data_quality import DataQualityChecker
from loguru import logger


def main():
    """Test transformation functionality"""
    logger.info("=" * 60)
    logger.info("Testing Data Transformation & Quality Checks")
    logger.info("=" * 60)
    
    # Step 1: Extract data
    logger.info("\n[1/3] Extracting data from API...")
    extractor = CoinGeckoExtractor()
    
    # Extract limited data for testing
    test_coins = ['bitcoin', 'ethereum', 'cardano']
    current_prices = extractor.get_current_prices(test_coins)
    global_data = extractor.get_global_data()
    
    # Get historical data
    historical_data = []
    for coin_id in test_coins:
        hist = extractor.get_historical_data(coin_id, days=30)
        if hist:
            historical_data.append(hist)
    
    extracted_data = {
        'current_prices': current_prices,
        'global_data': global_data,
        'historical_data': historical_data
    }
    
    logger.info(f"✓ Extracted data for {len(current_prices)} coins")
    
    # Step 2: Transform data
    logger.info("\n[2/3] Transforming data and calculating metrics...")
    calculator = MetricsCalculator()
    
    transformed = calculator.transform_all_data(extracted_data)
    
    logger.info(f"✓ Generated {len(transformed)} transformed tables:")
    for table_name, df in transformed.items():
        if not df.empty:
            logger.info(f"  - {table_name}: {len(df)} rows, {len(df.columns)} columns")
    
    # Display some results
    if 'market_dominance' in transformed and not transformed['market_dominance'].empty:
        logger.info("\n📊 Market Dominance:")
        dominance = transformed['market_dominance']
        for _, row in dominance.iterrows():
            logger.info(f"  {row['name']:12} - {row['market_dominance_pct']:.4f}%")
    
    if 'fear_greed' in transformed and not transformed['fear_greed'].empty:
        logger.info("\n😨😁 Fear & Greed Index:")
        fg = transformed['fear_greed']
        for _, row in fg.iterrows():
            logger.info(f"  {row['coin_id']:12} - Score: {row['fear_greed_score']:.1f} ({row['sentiment']})")
    
    if 'sharpe_ratio' in transformed and not transformed['sharpe_ratio'].empty:
        logger.info("\n📈 Sharpe Ratios (Risk-Adjusted Returns):")
        sharpe = transformed['sharpe_ratio'].sort_values('sharpe_ratio', ascending=False)
        for _, row in sharpe.iterrows():
            logger.info(f"  {row['coin_id']:12} - {row['sharpe_ratio']:.4f}")
    
    # Step 3: Quality checks
    logger.info("\n[3/3] Running data quality checks...")
    checker = DataQualityChecker()
    
    fact_table = transformed.get('fact_crypto_metrics')
    if fact_table is not None and not fact_table.empty:
        quality_results = checker.run_all_checks(fact_table, "fact_crypto_metrics")
        
        logger.info(f"\n📋 Quality Check Results:")
        logger.info(f"  Total checks: {quality_results['total_checks']}")
        logger.info(f"  Passed: {quality_results['passed_count']}")
        logger.info(f"  Overall: {'✓ PASSED' if quality_results['all_passed'] else '✗ FAILED'}")
        
        # Show details of failed checks
        if not quality_results['all_passed']:
            logger.warning("\nFailed checks:")
            for check in quality_results['checks']:
                if not check.get('passed', True):
                    logger.warning(f"  - {check['check']}: {check}")
    
    logger.info("\n" + "=" * 60)
    logger.info("Transformation & Quality Check Testing Complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()