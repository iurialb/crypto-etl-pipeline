-- Crypto ETL Pipeline Database Schema
-- PostgreSQL 14+

-- Drop tables if they exist (for development)
DROP TABLE IF EXISTS fact_crypto_metrics CASCADE;
DROP TABLE IF EXISTS dim_cryptocurrency CASCADE;
DROP TABLE IF EXISTS fact_market_dominance CASCADE;
DROP TABLE IF EXISTS fact_volatility CASCADE;
DROP TABLE IF EXISTS fact_sharpe_ratio CASCADE;
DROP TABLE IF EXISTS fact_fear_greed CASCADE;
DROP TABLE IF EXISTS correlation_matrix CASCADE;
DROP TABLE IF EXISTS etl_run_log CASCADE;

-- Dimension table: Cryptocurrency master data
CREATE TABLE dim_cryptocurrency (
    crypto_key SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Fact table: Main crypto metrics (daily snapshot)
CREATE TABLE fact_crypto_metrics (
    metric_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    extracted_date DATE NOT NULL,
    extracted_timestamp TIMESTAMP NOT NULL,
    
    -- Price metrics
    current_price DECIMAL(20, 8),
    market_cap BIGINT,
    total_volume BIGINT,
    circulating_supply DECIMAL(20, 2),
    
    -- Price changes
    price_change_24h DECIMAL(10, 4),
    price_change_7d DECIMAL(10, 4),
    price_change_30d DECIMAL(10, 4),
    price_change_percentage_24h DECIMAL(10, 4),
    price_change_percentage_7d DECIMAL(10, 4),
    price_change_percentage_30d DECIMAL(10, 4),
    
    -- Market metrics
    market_dominance_pct DECIMAL(10, 6),
    dominance_rank INTEGER,
    
    -- Volatility metrics
    volatility_7d DECIMAL(10, 6),
    volatility_30d DECIMAL(10, 6),
    
    -- Risk metrics
    sharpe_ratio DECIMAL(10, 6),
    annualized_return DECIMAL(10, 6),
    annualized_volatility DECIMAL(10, 6),
    
    -- Sentiment metrics
    fear_greed_score DECIMAL(5, 2),
    sentiment VARCHAR(20),
    
    -- Additional metrics
    ath DECIMAL(20, 8),
    atl_change_percentage DECIMAL(15, 6),
    ath_date TIMESTAMP,
    atl DECIMAL(20, 8),
    ath_change_percentage DECIMAL(15, 6),
    atl_date TIMESTAMP,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_coin_date UNIQUE (coin_id, extracted_date),
    CONSTRAINT fk_crypto FOREIGN KEY (coin_id) REFERENCES dim_cryptocurrency(coin_id)
);

-- Fact table: Market dominance history
CREATE TABLE fact_market_dominance (
    dominance_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    extracted_date DATE NOT NULL,
    market_cap BIGINT,
    market_dominance_pct DECIMAL(10, 6),
    dominance_rank INTEGER,
    total_market_cap BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_dominance_coin_date UNIQUE (coin_id, extracted_date)
);

-- Fact table: Volatility metrics
CREATE TABLE fact_volatility (
    volatility_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    extracted_date DATE NOT NULL,
    volatility_7d DECIMAL(10, 6),
    volatility_30d DECIMAL(10, 6),
    price_change_30d_pct DECIMAL(10, 4),
    avg_price_30d DECIMAL(20, 8),
    max_price_30d DECIMAL(20, 8),
    min_price_30d DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_volatility_coin_date UNIQUE (coin_id, extracted_date)
);

-- Fact table: Sharpe ratios
CREATE TABLE fact_sharpe_ratio (
    sharpe_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    extracted_date DATE NOT NULL,
    sharpe_ratio DECIMAL(10, 6),
    annualized_return DECIMAL(10, 6),
    annualized_volatility DECIMAL(10, 6),
    risk_free_rate DECIMAL(5, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_sharpe_coin_date UNIQUE (coin_id, extracted_date)
);

-- Fact table: Fear & Greed index
CREATE TABLE fact_fear_greed (
    fg_id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    extracted_date DATE NOT NULL,
    fear_greed_score DECIMAL(5, 2),
    sentiment VARCHAR(20),
    momentum_component DECIMAL(5, 2),
    volatility_component DECIMAL(5, 2),
    volume_component DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_fg_coin_date UNIQUE (coin_id, extracted_date)
);

-- Table: Correlation matrix (stored as key-value pairs)
CREATE TABLE correlation_matrix (
    correlation_id SERIAL PRIMARY KEY,
    extracted_date DATE NOT NULL,
    coin_id_1 VARCHAR(50) NOT NULL,
    coin_id_2 VARCHAR(50) NOT NULL,
    correlation_coefficient DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_correlation UNIQUE (extracted_date, coin_id_1, coin_id_2)
);

-- ETL run log table
CREATE TABLE etl_run_log (
    run_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, RUNNING
    coins_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    execution_time_seconds DECIMAL(10, 2),
    error_message TEXT,
    metadata JSONB
);

-- Create indexes for performance
CREATE INDEX idx_fact_metrics_coin_date ON fact_crypto_metrics(coin_id, extracted_date);
CREATE INDEX idx_fact_metrics_date ON fact_crypto_metrics(extracted_date);
CREATE INDEX idx_dominance_date ON fact_market_dominance(extracted_date);
CREATE INDEX idx_volatility_date ON fact_volatility(extracted_date);
CREATE INDEX idx_sharpe_date ON fact_sharpe_ratio(extracted_date);
CREATE INDEX idx_fg_date ON fact_fear_greed(extracted_date);
CREATE INDEX idx_correlation_date ON correlation_matrix(extracted_date);
CREATE INDEX idx_etl_log_timestamp ON etl_run_log(run_timestamp);

-- Create views for easy querying
CREATE OR REPLACE VIEW v_latest_crypto_metrics AS
SELECT 
    fcm.*,
    dc.name,
    dc.symbol
FROM fact_crypto_metrics fcm
INNER JOIN dim_cryptocurrency dc ON fcm.coin_id = dc.coin_id
WHERE fcm.extracted_date = (SELECT MAX(extracted_date) FROM fact_crypto_metrics);

CREATE OR REPLACE VIEW v_top_performers AS
SELECT 
    coin_id,
    name,
    symbol,
    current_price,
    price_change_percentage_24h,
    price_change_percentage_7d,
    price_change_percentage_30d,
    market_dominance_pct,
    sharpe_ratio,
    fear_greed_score,
    sentiment
FROM v_latest_crypto_metrics
ORDER BY price_change_percentage_24h DESC;

CREATE OR REPLACE VIEW v_risk_metrics AS
SELECT 
    coin_id,
    name,
    symbol,
    volatility_7d,
    volatility_30d,
    sharpe_ratio,
    annualized_return,
    annualized_volatility
FROM v_latest_crypto_metrics
ORDER BY sharpe_ratio DESC NULLS LAST;

-- Function to update last_updated timestamp
CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for dim_cryptocurrency
CREATE TRIGGER trg_update_crypto_timestamp
BEFORE UPDATE ON dim_cryptocurrency
FOR EACH ROW
EXECUTE FUNCTION update_last_updated();

COMMENT ON TABLE dim_cryptocurrency IS 'Dimension table containing cryptocurrency master data';
COMMENT ON TABLE fact_crypto_metrics IS 'Main fact table with comprehensive crypto metrics';
COMMENT ON TABLE etl_run_log IS 'Log table tracking ETL pipeline executions';