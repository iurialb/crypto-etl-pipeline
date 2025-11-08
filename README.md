# 🚀 Crypto ETL Pipeline

An end-to-end ETL pipeline for cryptocurrency data with advanced analytics and data quality checks.

## 📊 Features

- **Real-time data extraction** from CoinGecko API
- **Advanced metrics calculation**:
  - Market dominance index
  - Realized volatility
  - Correlation matrix
  - Fear & Greed score
  - Sharpe ratio
- **Data quality checks** with automated validation
- **Dimensional modeling** with SCD Type 2
- **Idempotent operations** for reliable re-runs
- **Structured logging** with JSON format
- **PostgreSQL** with partitioned tables

## 🏗️ Architecture

```
Extract (CoinGecko API) → Transform (Metrics + Validation) → Load (PostgreSQL)
```

## 🛠️ Tech Stack

- **Python 3.11+**
- **PostgreSQL** for data warehouse
- **SQLAlchemy** for database operations
- **Pandas** for data manipulation
- **Pydantic** for data validation
- **Loguru** for structured logging

## 📁 Project Structure

```
crypto-etl-pipeline/
├── src/
│   ├── extract/        # API data extraction
│   ├── transform/      # Data transformation and metrics
│   ├── load/          # Database loading operations
│   └── utils/         # Logging and utilities
├── config/            # Configuration files
├── sql/              # SQL schemas and queries
├── notebooks/        # Exploratory analysis
└── tests/           # Unit tests
```

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Git

### Installation

1. Clone the repository:
```bash
git clone https://github.com/iurialb/crypto-etl-pipeline.git
cd crypto-etl-pipeline
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

5. Setup database:
```bash
python src/load/database.py --init
```

### Running the Pipeline

```bash
# Run the complete ETL pipeline
python main.py
```

The pipeline will:
1. **Extract** data from CoinGecko API
2. **Transform** and calculate advanced metrics
3. **Validate** data quality
4. **Load** to PostgreSQL database

### Query Your Data

```bash
# Connect to database
psql -U postgres -d crypto_etl

# Run analytics queries
\i sql/queries/analytics.sql
```

Or use any SQL client to connect and explore!

## 📈 Metrics Explained

- **Market Dominance**: Percentage of total market cap
- **Volatility**: Standard deviation of returns over N days
- **Sharpe Ratio**: Risk-adjusted return metric
- **Fear & Greed Score**: Custom sentiment indicator based on volatility and volume

## 🧪 Testing

```bash
pytest tests/ -v --cov=src
```

## 📝 License

MIT License