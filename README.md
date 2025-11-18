# 🚀 Crypto ETL Pipeline

> A production-ready end-to-end ETL pipeline for cryptocurrency market analysis with advanced financial metrics and data quality monitoring.

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Metrics & Analytics](#metrics--analytics)
- [Data Quality](#data-quality)

---

## Project Overview

### The Challenge
Cryptocurrency markets generate massive amounts of real-time data. Making informed investment decisions requires not just raw prices, but sophisticated metrics like volatility, risk-adjusted returns, market sentiment, and asset correlations.

### The Solution
This project implements a complete **ETL (Extract, Transform, Load) pipeline** that:
1. **Extracts** real-time cryptocurrency data from CoinGecko API;
2. **Transforms** raw data into actionable insights using advanced financial metrics;
3. **Loads** processed data into a PostgreSQL data warehouse for analysis;
4. **Validates** data quality at every step to ensure reliability.

### Project Goals
- ✅ Build a **scalable** and **maintainable** data pipeline;
- ✅ Implement **advanced financial metrics** beyond basic price tracking;
- ✅ Ensure **data quality** with automated validation checks;
- ✅ Create a **professional-grade** portfolio project demonstrating data engineering skills;
- ✅ Enable **data-driven insights** for cryptocurrency market analysis.

---

## Key Features

### 🔄 Complete ETL Pipeline
- **Extraction**: Reliable API integration with retry logic and rate limiting;
- **Transformation**: Advanced metrics calculation with pandas;
- **Loading**: Efficient bulk inserts with idempotent operations.

### 📊 Advanced Financial Metrics
- **Market Dominance Index** - Track each coin's share of total market cap;
- **Realized Volatility** - Measure price stability over 7 and 30-day windows;
- **Sharpe Ratio** - Calculate risk-adjusted returns;
- **Correlation Matrix** - Identify relationships between different cryptocurrencies;
- **Fear & Greed Score** - Custom sentiment indicator combining volatility, momentum, and volume.

### 🛡️ Data Quality & Reliability
- Automated validation checks (null values, duplicates, anomalies);
- Structured logging with execution time tracking;
- Idempotent pipeline (safe to re-run multiple times);
- Comprehensive error handling with retry mechanisms.

### 🗄️ Professional Data Warehouse
- Dimensional modeling (star schema);
- Optimized indexes for query performance;
- Pre-built analytical views;
- Full audit trail with ETL run logs.

---

## Architecture

```
┌─────────────────┐
│  CoinGecko API  │
│   (Raw Data)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    EXTRACT      │  ← Retry logic, rate limiting
│  coingecko_api  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   TRANSFORM     │  ← Calculate metrics
│ metrics_calc +  │  ← Validate quality
│ data_quality    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     LOAD        │  ← Dimensional model
│  PostgreSQL     │  ← Idempotent inserts
└─────────────────┘
         │
         ▼
┌─────────────────┐
│   ANALYTICS     │  ← Pre-built queries
│  SQL Views +    │  ← Business insights
│  Dashboards     │
└─────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Language** | Python 3.9+ | Core pipeline logic |
| **Data Processing** | Pandas, NumPy | Data transformation and metrics |
| **Database** | PostgreSQL 14+ | Data warehouse |
| **ORM** | SQLAlchemy | Database operations |
| **Validation** | Pydantic | Data quality checks |
| **Logging** | Loguru | Structured logging |
| **Testing** | Pytest | Unit and integration tests |
| **API** | Requests | HTTP client with retry logic |
| **Containerization** | Docker | PostgreSQL deployment |

---

## Getting Started

### Prerequisites
- Python 3.9 or higher
- Docker & Docker Compose (for PostgreSQL)
- Git

### Step 1: Clone the Repository
```bash
git clone https://github.com/yourusername/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

### Step 2: Setup Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate  # Mac/Linux
# OR
venv\Scripts\activate  # Windows
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Configure Environment
```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your settings (default values work for Docker setup)
```

### Step 5: Start PostgreSQL Database
```bash
# Using Docker (recommended)
docker compose up -d

# Wait for database to be ready
sleep 10
```

### Step 6: Initialize Database Schema
```bash
python scripts/init_database.py
```

### Step 7: Run the Pipeline! 🎉
```bash
python main.py
```

You should see output like:
```
CRYPTO ETL PIPELINE STARTED
======================================================================
STEP 1: EXTRACT
✓ Extraction complete: Coins: 9

STEP 2: TRANSFORM
✓ Transformation complete

STEP 3: VALIDATE
✓ All quality checks passed (4/5)

STEP 4: LOAD
✓ Loaded 21 records

✓ PIPELINE COMPLETED SUCCESSFULLY
======================================================================
Execution time: 79.17 seconds
Coins processed: 9
Records inserted: 21
```

---

## Usage

### Running the Pipeline
```bash
# Full ETL execution
python main.py
```

### Exploring the Data
```bash
# Run pre-built analytics queries
python scripts/query_examples.py
```

This will show:
- Latest cryptocurrency metrics;
- Top 24h performers;
- Best risk-adjusted returns (Sharpe ratio);
- Fear & Greed sentiment distribution;
- Highly correlated asset pairs;
- Most volatile cryptocurrencies;
- Distance from all-time highs;
- Market dominance breakdown.

### Custom SQL Queries
```bash
# Connect to database
psql -U postgres -d crypto_etl

# Example: Latest metrics
SELECT coin_id, current_price, market_dominance_pct, fear_greed_score
FROM v_latest_crypto_metrics
ORDER BY market_dominance_pct DESC;
```

### Testing
```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

## Project Structure

```
crypto-etl-pipeline/
│
├── main.py                      # Pipeline orchestrator
├── requirements.txt             # Python dependencies
├── docker-compose.yml           # PostgreSQL setup
├── .env.example                 # Environment template
│
├── src/                         # Source code
│   ├── extract/                 # Data extraction
│   │   └── coingecko_api.py    # API client with retry logic
│   │
│   ├── transform/               # Data transformation
│   │   ├── metrics_calculator.py   # Financial metrics
│   │   └── data_quality.py         # Validation checks
│   │
│   ├── load/                    # Data loading
│   │   └── database.py         # Database operations
│   │
│   └── utils/                   # Utilities
│       ├── logger.py           # Structured logging
│       └── config_loader.py    # Configuration management
│
├── sql/                         # SQL scripts
│   ├── schema.sql              # Database schema
│   └── queries/
│       └── analytics.sql       # Pre-built queries
│
├── scripts/                     # Utility scripts
│   ├── init_database.py        # Database initialization
│   ├── test_extraction.py      # Test data extraction
│   ├── test_transformation.py  # Test transformations
│   └── query_examples.py       # Example analytics
│
├── tests/                       # Unit tests
│   ├── test_extractor.py
│   ├── test_transform.py
│   └── test_database.py
│
├── config/                      # Configuration
│   └── config.yaml             # Pipeline settings
│
└── docs/                        # Documentation
    └── DATABASE_SETUP.md       # Database setup guide
```

---

## Metrics & Analytics

### Market Dominance Index
Calculates each cryptocurrency's percentage of total market capitalization.

```python
Market Dominance (%) = (Coin Market Cap / Total Market Cap) × 100
```

**Use Case**: Identify market leaders and track market share changes over time.

### Realized Volatility
Standard deviation of daily returns, annualized.

```python
Volatility = std(daily_returns) × √365
```

**Use Case**: Measure price stability and risk level for portfolio management.

### Sharpe Ratio
Risk-adjusted return metric.

```python
Sharpe Ratio = (Return - Risk-Free Rate) / Volatility
```

**Use Case**: Compare investment efficiency across different assets.

### Fear & Greed Score (Custom)
Composite sentiment indicator (0-100 scale).

**Components**:
- Price momentum (30%);
- Volatility level (30%, inverse);
- Volume changes (40%).

**Scale**:
- 0-20: Extreme Fear;
- 20-40: Fear;
- 40-60: Neutral;
- 60-80: Greed;
- 80-100: Extreme Greed.

**Use Case**: Gauge market sentiment for contrarian investment strategies.

### Correlation Matrix
Pearson correlation between cryptocurrency returns.

**Use Case**: Portfolio diversification and risk management.

---

## Data Quality

The pipeline includes comprehensive data quality checks:

### Validation Checks
- ✅ **Null Value Check**: Ensures critical fields are populated;
- ✅ **Duplicate Check**: Prevents duplicate records;
- ✅ **Price Validity**: Verifies prices are positive and reasonable;
- ✅ **Anomaly Detection**: Flags unusual price changes (>50% in 24h);
- ✅ **Data Freshness**: Ensures data is recent (<24 hours old).

### Quality Metrics
All checks are logged with:
- Pass/fail status;
- Detailed failure reasons;
- Affected records;
- Execution timestamps.

### Handling Failures
- Pipeline continues on quality warnings;
- Critical errors stop execution;
- All issues logged for investigation;
- Idempotent design allows safe re-runs.

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 👤 Author

**Iuri Albuquerque**
- GitHub: [@iurialb](https://github.com/iurialb)
- [Linkedin] (https://www.linkedin.com/in/iurialbuquerque/)

---

## Acknowledgments

- CoinGecko for providing free cryptocurrency data API
- The open-source community for amazing tools and libraries