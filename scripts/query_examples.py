"""
Script to demonstrate querying the database
"""
import sys
import os
from pathlib import Path

# Setup path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from src.load.database import DatabaseManager
from loguru import logger
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 30)


def main():
    """Run example queries"""
    logger.info("=" * 70)
    logger.info("Database Query Examples")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    
    try:
        # Query 1: Latest metrics
        logger.info("\n📊 Latest Cryptocurrency Metrics:")
        df = db.query("""
            SELECT 
                coin_id,
                current_price,
                market_dominance_pct,
                price_change_percentage_24h,
                fear_greed_score,
                sentiment
            FROM v_latest_crypto_metrics
            ORDER BY market_dominance_pct DESC
            LIMIT 10
        """)
        print(df.to_string(index=False))
        
        # Query 2: Top performers
        logger.info("\n🚀 Top 5 Performers (24h):")
        df = db.query("""
            SELECT 
                coin_id,
                name,
                price_change_percentage_24h,
                current_price,
                sentiment
            FROM v_latest_crypto_metrics
            ORDER BY price_change_percentage_24h DESC
            LIMIT 5
        """)
        print(df.to_string(index=False))
        
        # Query 3: Best Sharpe ratios
        logger.info("\n📈 Best Risk-Adjusted Returns:")
        df = db.query("""
            SELECT 
                coin_id,
                sharpe_ratio,
                annualized_return,
                annualized_volatility
            FROM v_latest_crypto_metrics
            WHERE sharpe_ratio IS NOT NULL
            ORDER BY sharpe_ratio DESC
            LIMIT 5
        """)
        print(df.to_string(index=False))
        
        # Query 4: Sentiment distribution
        logger.info("\n😨😁 Fear & Greed Sentiment:")
        df = db.query("""
            SELECT 
                sentiment,
                COUNT(*) as count,
                ROUND(AVG(fear_greed_score)::numeric, 2) as avg_score
            FROM v_latest_crypto_metrics
            GROUP BY sentiment
            ORDER BY avg_score DESC
        """)
        print(df.to_string(index=False))
        
        # Query 5: Correlations
        logger.info("\n🔗 Highly Correlated Pairs:")
        df = db.query("""
            SELECT 
                coin_id_1,
                coin_id_2,
                ROUND(correlation_coefficient::numeric, 3) as correlation
            FROM correlation_matrix
            WHERE extracted_date = (SELECT MAX(extracted_date) FROM correlation_matrix)
                AND correlation_coefficient > 0.8
                AND coin_id_1 < coin_id_2
            ORDER BY correlation_coefficient DESC
            LIMIT 5
        """)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No correlation data available yet")
        
        # Query 6: Volatility ranking
        logger.info("\n⚡ Most Volatile Cryptocurrencies:")
        df = db.query("""
            SELECT 
                coin_id,
                name,
                volatility_7d,
                volatility_30d,
                price_change_percentage_24h
            FROM v_latest_crypto_metrics
            WHERE volatility_30d IS NOT NULL
            ORDER BY volatility_30d DESC
            LIMIT 5
        """)
        if not df.empty:
            print(df.to_string(index=False))
        else:
            print("No volatility data available yet")
        
        # Query 7: Price vs ATH comparison
        logger.info("\n📉 Distance from All-Time High:")
        df = db.query("""
            SELECT 
                coin_id,
                name,
                ROUND(current_price::numeric, 2) as current_price,
                ROUND(ath::numeric, 2) as ath,
                ROUND(ath_change_percentage::numeric, 2) as pct_from_ath
            FROM v_latest_crypto_metrics
            ORDER BY ath_change_percentage
            LIMIT 5
        """)
        print(df.to_string(index=False))
        
        # Query 8: Market dominance breakdown
        logger.info("\n🥧 Market Dominance Distribution:")
        df = db.query("""
            SELECT 
                coin_id,
                name,
                ROUND(market_dominance_pct::numeric, 2) as dominance_pct,
                ROUND((market_cap / 1000000000)::numeric, 2) as market_cap_billions
            FROM v_latest_crypto_metrics
            ORDER BY market_dominance_pct DESC
            LIMIT 5
        """)
        print(df.to_string(index=False))
        
        # Query 9: ETL logs
        logger.info("\n📝 Recent ETL Runs:")
        df = db.query("""
            SELECT 
                run_timestamp,
                status,
                coins_processed,
                records_inserted,
                ROUND(execution_time_seconds::numeric, 2) as exec_time_sec
            FROM etl_run_log
            ORDER BY run_timestamp DESC
            LIMIT 5
        """)
        print(df.to_string(index=False))
        
        logger.info("\n" + "=" * 70)
        logger.info("Query examples complete!")
        logger.info("=" * 70)
        logger.info("\nFor more queries, see: sql/queries/analytics.sql")
        
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
    finally:
        db.close()


if __name__ == "__main__":
    main()