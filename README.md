# crypto-etl-pipeline
End-to-end ETL pipeline for cryptocurrency data with advanced metrics

Crypto ETL Pipeline
An end-to-end ETL pipeline for cryptocurrency data with advanced analytics and data quality checks.
📊 Features

Real-time data extraction from CoinGecko API
Advanced metrics calculation:

Market dominance index
Realized volatility
Correlation matrix
Fear & Greed score
Sharpe ratio


Data quality checks with automated validation
Dimensional modeling with SCD Type 2
Idempotent operations for reliable re-runs
Structured logging with JSON format
PostgreSQL with partitioned tables

🏗️ Architecture
Extract (CoinGecko API) → Transform (Metrics + Validation) → Load (PostgreSQL)
🛠️ Tech Stack

Python 3.11+
PostgreSQL for data warehouse
SQLAlchemy for database operations
Pandas for data manipulation
Pydantic for data validation
Loguru for structured logging

📁 Project Structure
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
🚀 Getting Started
Prerequisites

Python 3.11+
PostgreSQL 14+
Git

Installation

Clone the repository:

bashgit clone https://github.com/YOUR_USERNAME/crypto-etl-pipeline.git
cd crypto-etl-pipeline

Create virtual environment:

bashpython -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install dependencies:

bashpip install -r requirements.txt

Configure environment:

bashcp .env.example .env
# Edit .env with your database credentials

Setup database:

bashpython src/load/database.py --init
Running the Pipeline
bashpython main.py
📈 Metrics Explained

Market Dominance: Percentage of total market cap
Volatility: Standard deviation of returns over N days
Sharpe Ratio: Risk-adjusted return metric
Fear & Greed Score: Custom sentiment indicator based on volatility and volume

🧪 Testing
bashpytest tests/ -v --cov=src