# Database Setup Guide

This guide will help you set up PostgreSQL for the Crypto ETL Pipeline.

## Option 1: Local PostgreSQL Installation (Recommended for Development)

### macOS

```bash
# Install PostgreSQL using Homebrew
brew install postgresql@14

# Start PostgreSQL service
brew services start postgresql@14

# Create database
createdb crypto_etl

# Set password for postgres user (if needed)
psql postgres -c "ALTER USER postgres PASSWORD 'your_password';"
```

### Linux (Ubuntu/Debian)

```bash
# Install PostgreSQL
sudo apt update
sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database
sudo -u postgres createdb crypto_etl

# Set password
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_password';"
```

### Windows

1. Download PostgreSQL installer from https://www.postgresql.org/download/windows/
2. Run installer and follow wizard
3. Remember the password you set for postgres user
4. Open pgAdmin or psql and create database:

```sql
CREATE DATABASE crypto_etl;
```

## Option 2: Docker (Easiest for All Platforms)

```bash
# Create docker-compose.yml in project root
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  postgres:
    image: postgres:14
    container_name: crypto_etl_db
    environment:
      POSTGRES_DB: crypto_etl
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
EOF

# Start PostgreSQL container
docker-compose up -d

# Check if running
docker ps
```

## Configure Environment Variables

Update your `.env` file with database credentials:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crypto_etl
DB_USER=postgres
DB_PASSWORD=your_password_here
```

## Initialize Database Schema

Run the initialization script:

```bash
# Using Python
python scripts/init_database.py

# Or using psql directly
psql -U postgres -d crypto_etl -f sql/schema.sql
```

## Verify Setup

Test database connection:

```python
from src.load.database import DatabaseManager

db = DatabaseManager()
print("✓ Database connection successful!")

# Check tables
tables = db.query("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
""")
print(f"✓ Found {len(tables)} tables")
print(tables)
```

## Troubleshooting

### Connection Refused

- **Check if PostgreSQL is running:**
  ```bash
  # macOS/Linux
  pg_isready
  
  # Check service status
  brew services list  # macOS
  sudo systemctl status postgresql  # Linux
  ```

- **Check PostgreSQL is listening on correct port:**
  ```bash
  sudo lsof -i :5432
  ```

### Authentication Failed

- Verify credentials in `.env` match your PostgreSQL setup
- Check `pg_hba.conf` authentication method (usually at `/usr/local/var/postgres/pg_hba.conf`)

### Database Does Not Exist

```bash
# Create database manually
createdb crypto_etl

# Or using psql
psql -U postgres -c "CREATE DATABASE crypto_etl;"
```

## Useful Commands

```bash
# Connect to database
psql -U postgres -d crypto_etl

# List all databases
\l

# List all tables
\dt

# Describe table structure
\d fact_crypto_metrics

# View data
SELECT * FROM dim_cryptocurrency LIMIT 10;

# Check ETL run logs
SELECT * FROM etl_run_log ORDER BY run_timestamp DESC LIMIT 5;

# Drop and recreate database (CAUTION: deletes all data)
dropdb crypto_etl && createdb crypto_etl
```

## Next Steps

After setting up the database:

1. Run `python scripts/init_database.py` to create tables
2. Test the full ETL pipeline with `python main.py`
3. Query your data using the provided views or custom SQL