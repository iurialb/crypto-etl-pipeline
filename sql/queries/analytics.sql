-- Analytics Queries for Crypto ETL Pipeline

-- 1. Latest snapshot of all cryptocurrencies with key metrics
SELECT 
    coin_id,
    current_price,
    market_cap,
    market_dominance_pct,
    price_change_percentage_24h,
    price_change_percentage_7d,
    volatility_7d,
    sharpe_ratio,
    fear_greed_score,
    sentiment
FROM v_latest_crypto_metrics
ORDER BY market_dominance_pct DESC;

-- 2. Top 5 performers (24h)
SELECT 
    coin_id,
    name,
    current_price,
    price_change_percentage_24h,
    total_volume,
    sentiment
FROM v_latest_crypto_metrics
ORDER BY price_change_percentage_24h DESC
LIMIT 5;

-- 3. Top 5 worst performers (24h)
SELECT 
    coin_id,
    name,
    current_price,
    price_change_percentage_24h,
    total_volume,
    sentiment
FROM v_latest_crypto_metrics
ORDER BY price_change_percentage_24h ASC
LIMIT 5;

-- 4. Best risk-adjusted returns (Sharpe Ratio)
SELECT 
    coin_id,
    name,
    sharpe_ratio,
    annualized_return,
    annualized_volatility,
    volatility_30d
FROM v_latest_crypto_metrics
WHERE sharpe_ratio IS NOT NULL
ORDER BY sharpe_ratio DESC
LIMIT 10;

-- 5. Fear & Greed sentiment distribution
SELECT 
    sentiment,
    COUNT(*) as count,
    AVG(fear_greed_score) as avg_score,
    AVG(price_change_percentage_24h) as avg_price_change_24h
FROM v_latest_crypto_metrics
GROUP BY sentiment
ORDER BY avg_score DESC;

-- 6. Market dominance leaders
SELECT 
    coin_id,
    name,
    market_cap,
    market_dominance_pct,
    dominance_rank
FROM v_latest_crypto_metrics
ORDER BY market_dominance_pct DESC
LIMIT 10;

-- 7. Most volatile cryptocurrencies
SELECT 
    coin_id,
    name,
    volatility_7d,
    volatility_30d,
    price_change_percentage_7d
FROM v_latest_crypto_metrics
WHERE volatility_30d IS NOT NULL
ORDER BY volatility_30d DESC
LIMIT 10;

-- 8. Price trends over time (last 30 days)
SELECT 
    extracted_date,
    coin_id,
    current_price,
    market_cap,
    price_change_percentage_24h
FROM fact_crypto_metrics
WHERE coin_id IN ('bitcoin', 'ethereum', 'cardano')
    AND extracted_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY coin_id, extracted_date;

-- 9. Correlation analysis - which coins move together?
SELECT 
    cm.coin_id_1,
    cm.coin_id_2,
    cm.correlation_coefficient,
    cm.extracted_date
FROM correlation_matrix cm
WHERE cm.extracted_date = (SELECT MAX(extracted_date) FROM correlation_matrix)
    AND cm.correlation_coefficient > 0.8
    AND cm.coin_id_1 < cm.coin_id_2  -- Avoid duplicates
ORDER BY cm.correlation_coefficient DESC;

-- 10. ETL Pipeline performance metrics
SELECT 
    run_timestamp,
    status,
    coins_processed,
    records_inserted,
    execution_time_seconds,
    ROUND(records_inserted::numeric / NULLIF(execution_time_seconds, 0), 2) as records_per_second
FROM etl_run_log
ORDER BY run_timestamp DESC
LIMIT 10;

-- 11. Daily aggregated statistics
SELECT 
    extracted_date,
    COUNT(DISTINCT coin_id) as total_coins,
    AVG(current_price) as avg_price,
    SUM(market_cap) as total_market_cap,
    AVG(volatility_7d) as avg_volatility,
    AVG(fear_greed_score) as avg_fear_greed
FROM fact_crypto_metrics
GROUP BY extracted_date
ORDER BY extracted_date DESC
LIMIT 30;

-- 12. Coins with extreme sentiment (Fear or Greed)
SELECT 
    coin_id,
    name,
    fear_greed_score,
    sentiment,
    price_change_percentage_24h,
    volatility_7d
FROM v_latest_crypto_metrics
WHERE sentiment IN ('Extreme Fear', 'Extreme Greed')
ORDER BY fear_greed_score;

-- 13. Portfolio diversification candidates (low correlation with Bitcoin)
SELECT 
    cm.coin_id_2 as coin_id,
    dc.name,
    cm.correlation_coefficient as correlation_with_btc,
    fcm.volatility_30d,
    fcm.sharpe_ratio
FROM correlation_matrix cm
JOIN dim_cryptocurrency dc ON cm.coin_id_2 = dc.coin_id
LEFT JOIN v_latest_crypto_metrics fcm ON cm.coin_id_2 = fcm.coin_id
WHERE cm.coin_id_1 = 'bitcoin'
    AND cm.extracted_date = (SELECT MAX(extracted_date) FROM correlation_matrix)
    AND cm.correlation_coefficient < 0.5
ORDER BY cm.correlation_coefficient ASC;

-- 14. Risk-Return scatter data
SELECT 
    coin_id,
    name,
    annualized_return as return,
    annualized_volatility as risk,
    sharpe_ratio,
    market_dominance_pct
FROM v_latest_crypto_metrics
WHERE annualized_return IS NOT NULL 
    AND annualized_volatility IS NOT NULL
ORDER BY sharpe_ratio DESC NULLS LAST;

-- 15. Historical price comparison (% change from first record)
WITH first_prices AS (
    SELECT 
        coin_id,
        MIN(extracted_date) as first_date,
        FIRST_VALUE(current_price) OVER (PARTITION BY coin_id ORDER BY extracted_date) as first_price
    FROM fact_crypto_metrics
    GROUP BY coin_id, current_price, extracted_date
)
SELECT 
    fcm.coin_id,
    fcm.extracted_date,
    fcm.current_price,
    fp.first_price,
    ROUND(((fcm.current_price - fp.first_price) / fp.first_price * 100)::numeric, 2) as change_from_start_pct
FROM fact_crypto_metrics fcm
JOIN first_prices fp ON fcm.coin_id = fp.coin_id
WHERE fcm.coin_id IN ('bitcoin', 'ethereum')
ORDER BY fcm.coin_id, fcm.extracted_date;