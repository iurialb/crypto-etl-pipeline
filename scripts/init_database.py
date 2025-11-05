"""
Script to initialize database schema
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


def main():
    """Initialize database schema"""
    logger.info("=" * 60)
    logger.info("Database Initialization")
    logger.info("=" * 60)
    
    try:
        # Create database manager
        logger.info("\n[1/3] Connecting to database...")
        db = DatabaseManager()
        logger.info("✓ Connected successfully")
        
        # Initialize schema
        logger.info("\n[2/3] Creating database schema...")
        db.initialize_schema()
        logger.info("✓ Schema created successfully")
        
        # Verify tables
        logger.info("\n[3/3] Verifying tables...")
        tables = db.query("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        logger.info(f"✓ Found {len(tables)} tables:")
        for _, row in tables.iterrows():
            logger.info(f"  - {row['table_name']}")
        
        # Verify views
        views = db.query("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        if not views.empty:
            logger.info(f"\n✓ Found {len(views)} views:")
            for _, row in views.iterrows():
                logger.info(f"  - {row['table_name']}")
        
        logger.info("\n" + "=" * 60)
        logger.info("Database initialization complete! ✓")
        logger.info("=" * 60)
        logger.info("\nYou can now run the ETL pipeline with: python main.py")
        
        db.close()
        
    except Exception as e:
        logger.error(f"\n✗ Database initialization failed: {str(e)}")
        logger.error("\nPlease check:")
        logger.error("1. PostgreSQL is running")
        logger.error("2. Database credentials in .env are correct")
        logger.error("3. Database 'crypto_etl' exists")
        logger.error("\nSee docs/DATABASE_SETUP.md for setup instructions")
        sys.exit(1)


if __name__ == "__main__":
    main()