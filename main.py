"""
Main ETL Pipeline Orchestrator
Extracts cryptocurrency data, transforms with advanced metrics, and loads to database
"""
import sys
from datetime import datetime, date
from pathlib import Path
from loguru import logger

from src.extract.coingecko_api import CoinGeckoExtractor
from src.transform.metrics_calculator import MetricsCalculator
from src.transform.data_quality import DataQualityChecker
from src.load.database import DatabaseManager
from src.utils.config_loader import get_config


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self):
        """Initialize ETL pipeline"""
        self.config = get_config()
        self.extractor = CoinGeckoExtractor()
        self.transformer = MetricsCalculator()
        self.quality_checker = DataQualityChecker()
        self.db_manager = DatabaseManager()
        
        self.start_time = None
        self.run_id = None
        
        logger.info("ETL Pipeline initialized")
    
    def extract(self):
        """
        Extract data from CoinGecko API
        
        Returns:
            Dictionary with extracted data
        """
        logger.info("=" * 70)
        logger.info("STEP 1: EXTRACT")
        logger.info("=" * 70)
        
        try:
            extracted_data = self.extractor.extract_all_data()
            
            logger.info(f"✓ Extraction complete:")
            logger.info(f"  - Coins: {extracted_data['total_coins']}")
            logger.info(f"  - Historical datasets: {len(extracted_data.get('historical_data', []))}")
            
            return extracted_data
            
        except Exception as e:
            logger.error(f"✗ Extraction failed: {str(e)}")
            raise
    
    def transform(self, extracted_data):
        """
        Transform data and calculate advanced metrics
        
        Args:
            extracted_data: Raw data from extraction
            
        Returns:
            Dictionary with transformed DataFrames
        """
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: TRANSFORM")
        logger.info("=" * 70)
        
        try:
            # Transform data
            transformed_data = self.transformer.transform_all_data(extracted_data)
            
            logger.info(f"✓ Transformation complete:")
            for table_name, df in transformed_data.items():
                if not df.empty:
                    logger.info(f"  - {table_name}: {len(df)} rows")
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"✗ Transformation failed: {str(e)}")
            raise
    
    def validate(self, transformed_data):
        """
        Run data quality checks
        
        Args:
            transformed_data: Transformed DataFrames
            
        Returns:
            Dictionary with quality check results
        """
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: VALIDATE")
        logger.info("=" * 70)
        
        try:
            fact_table = transformed_data.get('fact_crypto_metrics')
            
            if fact_table is None or fact_table.empty:
                logger.warning("No fact table to validate")
                return {"all_passed": False}
            
            quality_results = self.quality_checker.run_all_checks(
                fact_table, 
                "fact_crypto_metrics"
            )
            
            if quality_results['all_passed']:
                logger.info(f"✓ All quality checks passed ({quality_results['passed_count']}/{quality_results['total_checks']})")
            else:
                logger.warning(f"⚠ Some quality checks failed ({quality_results['passed_count']}/{quality_results['total_checks']})")
                logger.warning("Pipeline will continue, but data quality issues detected")
            
            return quality_results
            
        except Exception as e:
            logger.error(f"✗ Validation failed: {str(e)}")
            raise
    
    def load(self, transformed_data, extracted_date=None):
        """
        Load data to database
        
        Args:
            transformed_data: Transformed DataFrames
            extracted_date: Date of extraction (default: today)
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: LOAD")
        logger.info("=" * 70)
        
        if extracted_date is None:
            extracted_date = date.today()
        
        stats = {
            'fact_metrics': 0,
            'correlations': 0,
            'total_records': 0
        }
        
        try:
            # Load main fact table
            fact_table = transformed_data.get('fact_crypto_metrics')
            if fact_table is not None and not fact_table.empty:
                count = self.db_manager.insert_fact_crypto_metrics(
                    fact_table, 
                    extracted_date
                )
                stats['fact_metrics'] = count
                stats['total_records'] += count
                logger.info(f"✓ Loaded {count} fact metric records")
            
            # Load correlation matrix
            correlation_matrix = transformed_data.get('correlation_matrix')
            if correlation_matrix is not None and not correlation_matrix.empty:
                count = self.db_manager.insert_correlation_matrix(
                    correlation_matrix,
                    extracted_date
                )
                stats['correlations'] = count
                stats['total_records'] += count
                logger.info(f"✓ Loaded {count} correlation records")
            
            logger.info(f"✓ Load complete: {stats['total_records']} total records")
            
            return stats
            
        except Exception as e:
            logger.error(f"✗ Load failed: {str(e)}")
            raise
    
    def run(self):
        """
        Execute complete ETL pipeline
        
        Returns:
            Dictionary with pipeline execution results
        """
        self.start_time = datetime.now()
        
        logger.info("\n" + "=" * 70)
        logger.info("🚀 CRYPTO ETL PIPELINE STARTED")
        logger.info("=" * 70)
        logger.info(f"Start time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Cryptocurrencies: {', '.join(self.config.cryptocurrencies)}")
        
        try:
            # Log pipeline start
            self.run_id = self.db_manager.log_etl_run(
                status='RUNNING',
                metadata={'start_time': str(self.start_time)}
            )
            
            # Execute pipeline steps
            extracted_data = self.extract()
            transformed_data = self.transform(extracted_data)
            quality_results = self.validate(transformed_data)
            load_stats = self.load(transformed_data)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - self.start_time).total_seconds()
            
            # Log success
            self.db_manager.log_etl_run(
                status='SUCCESS',
                coins_processed=extracted_data['total_coins'],
                records_inserted=load_stats['total_records'],
                execution_time_seconds=execution_time,
                metadata={
                    'quality_checks_passed': quality_results.get('all_passed', False),
                    'end_time': str(end_time)
                }
            )
            
            # Print summary
            logger.info("\n" + "=" * 70)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"Execution time: {execution_time:.2f} seconds")
            logger.info(f"Coins processed: {extracted_data['total_coins']}")
            logger.info(f"Records inserted: {load_stats['total_records']}")
            logger.info(f"Quality checks: {'✓ PASSED' if quality_results.get('all_passed') else '⚠ WARNING'}")
            logger.info("=" * 70)
            
            return {
                'status': 'SUCCESS',
                'execution_time': execution_time,
                'coins_processed': extracted_data['total_coins'],
                'records_inserted': load_stats['total_records'],
                'quality_passed': quality_results.get('all_passed', False)
            }
            
        except Exception as e:
            # Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - self.start_time).total_seconds()
            
            # Log failure
            self.db_manager.log_etl_run(
                status='FAILED',
                execution_time_seconds=execution_time,
                error_message=str(e),
                metadata={
                    'error_type': type(e).__name__,
                    'end_time': str(end_time)
                }
            )
            
            logger.error("\n" + "=" * 70)
            logger.error("✗ PIPELINE FAILED")
            logger.error("=" * 70)
            logger.error(f"Error: {str(e)}")
            logger.error(f"Execution time: {execution_time:.2f} seconds")
            logger.error("=" * 70)
            
            return {
                'status': 'FAILED',
                'error': str(e),
                'execution_time': execution_time
            }
        
        finally:
            # Close database connection
            self.db_manager.close()


def main():
    """Main entry point"""
    try:
        pipeline = ETLPipeline()
        result = pipeline.run()
        
        # Exit with appropriate code
        sys.exit(0 if result['status'] == 'SUCCESS' else 1)
        
    except KeyboardInterrupt:
        logger.warning("\n\nPipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nUnexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()