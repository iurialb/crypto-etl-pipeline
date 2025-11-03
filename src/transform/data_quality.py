"""
Data quality checks and validation
"""
import pandas as pd
from typing import Dict, List, Tuple
from loguru import logger
from src.utils.config_loader import get_config


class DataQualityChecker:
    """Perform data quality checks on cryptocurrency data"""
    
    def __init__(self):
        """Initialize data quality checker"""
        self.config = get_config()
        self.quality_config = self.config.get('quality_checks', {})
        self.max_null_pct = self.quality_config.get('max_null_percentage', 0.05)
        self.min_price = self.quality_config.get('min_price_value', 0)
        self.max_price_change = self.quality_config.get('max_price_change_percentage', 50)
        
        logger.info("Data Quality Checker initialized")
    
    def check_null_values(self, df: pd.DataFrame, table_name: str = "data") -> Tuple[bool, Dict]:
        """
        Check for null values in DataFrame
        
        Args:
            df: DataFrame to check
            table_name: Name of the table for logging
            
        Returns:
            Tuple of (passed: bool, results: dict)
        """
        if df.empty:
            logger.warning(f"{table_name}: DataFrame is empty")
            return False, {"error": "Empty DataFrame"}
        
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df)) * 100
        
        failed_columns = null_percentages[null_percentages > (self.max_null_pct * 100)]
        
        passed = len(failed_columns) == 0
        
        results = {
            "check": "null_values",
            "table": table_name,
            "passed": passed,
            "total_rows": len(df),
            "null_counts": null_counts.to_dict(),
            "failed_columns": failed_columns.to_dict() if not passed else {}
        }
        
        if passed:
            logger.info(f"{table_name}: Null value check passed")
        else:
            logger.warning(f"{table_name}: Null value check failed", extra=results)
        
        return passed, results
    
    def check_price_validity(self, df: pd.DataFrame, price_column: str = 'current_price') -> Tuple[bool, Dict]:
        """
        Check if prices are valid (positive and reasonable)
        
        Args:
            df: DataFrame with price data
            price_column: Name of price column
            
        Returns:
            Tuple of (passed: bool, results: dict)
        """
        if price_column not in df.columns:
            logger.warning(f"Price column '{price_column}' not found")
            return False, {"error": f"Column {price_column} not found"}
        
        invalid_prices = df[df[price_column] <= self.min_price]
        passed = len(invalid_prices) == 0
        
        results = {
            "check": "price_validity",
            "passed": passed,
            "min_price_threshold": self.min_price,
            "invalid_count": len(invalid_prices),
            "invalid_coins": invalid_prices['id'].tolist() if 'id' in df.columns and not passed else []
        }
        
        if passed:
            logger.info("Price validity check passed")
        else:
            logger.warning("Price validity check failed", extra=results)
        
        return passed, results
    
    def check_price_change_anomalies(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Check for unusual price changes that might indicate data issues
        
        Args:
            df: DataFrame with price change data
            
        Returns:
            Tuple of (passed: bool, results: dict)
        """
        anomalies = []
        
        price_change_columns = [
            'price_change_percentage_24h',
            'price_change_percentage_7d',
            'price_change_percentage_30d'
        ]
        
        for col in price_change_columns:
            if col in df.columns:
                extreme_changes = df[
                    (df[col] > self.max_price_change) | 
                    (df[col] < -self.max_price_change)
                ]
                
                if len(extreme_changes) > 0:
                    anomalies.extend([
                        {
                            'coin_id': row['id'],
                            'column': col,
                            'value': row[col]
                        }
                        for _, row in extreme_changes.iterrows()
                        if 'id' in df.columns
                    ])
        
        passed = len(anomalies) == 0
        
        results = {
            "check": "price_change_anomalies",
            "passed": passed,
            "threshold": self.max_price_change,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:10]  # Limit to first 10
        }
        
        if passed:
            logger.info("Price change anomaly check passed")
        else:
            logger.warning(f"Found {len(anomalies)} price change anomalies", extra=results)
        
        return passed, results
    
    def check_duplicate_records(self, df: pd.DataFrame, key_columns: List[str] = None) -> Tuple[bool, Dict]:
        """
        Check for duplicate records
        
        Args:
            df: DataFrame to check
            key_columns: Columns that should be unique
            
        Returns:
            Tuple of (passed: bool, results: dict)
        """
        if key_columns is None:
            key_columns = ['id']
        
        # Only check columns that exist
        existing_key_columns = [col for col in key_columns if col in df.columns]
        
        if not existing_key_columns:
            logger.warning(f"None of the key columns {key_columns} found in DataFrame")
            return True, {"warning": "No key columns to check"}
        
        duplicates = df[df.duplicated(subset=existing_key_columns, keep=False)]
        passed = len(duplicates) == 0
        
        results = {
            "check": "duplicate_records",
            "passed": passed,
            "key_columns": existing_key_columns,
            "duplicate_count": len(duplicates),
            "duplicate_ids": duplicates[existing_key_columns[0]].tolist() if not passed and len(existing_key_columns) > 0 else []
        }
        
        if passed:
            logger.info("Duplicate check passed")
        else:
            logger.warning(f"Found {len(duplicates)} duplicate records", extra=results)
        
        return passed, results
    
    def check_data_freshness(self, df: pd.DataFrame, timestamp_column: str = 'extracted_at') -> Tuple[bool, Dict]:
        """
        Check if data is fresh (recently extracted)
        
        Args:
            df: DataFrame with timestamp
            timestamp_column: Name of timestamp column
            
        Returns:
            Tuple of (passed: bool, results: dict)
        """
        if timestamp_column not in df.columns:
            return True, {"warning": f"Timestamp column '{timestamp_column}' not found"}
        
        try:
            df[timestamp_column] = pd.to_datetime(df[timestamp_column])
            latest_timestamp = df[timestamp_column].max()
            now = pd.Timestamp.now()
            age_hours = (now - latest_timestamp).total_seconds() / 3600
            
            # Data should be less than 24 hours old
            passed = age_hours < 24
            
            results = {
                "check": "data_freshness",
                "passed": passed,
                "latest_timestamp": str(latest_timestamp),
                "age_hours": age_hours,
                "threshold_hours": 24
            }
            
            if passed:
                logger.info(f"Data freshness check passed (age: {age_hours:.2f} hours)")
            else:
                logger.warning(f"Data is stale (age: {age_hours:.2f} hours)", extra=results)
            
            return passed, results
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            return False, {"error": str(e)}
    
    def run_all_checks(self, df: pd.DataFrame, table_name: str = "data") -> Dict:
        """
        Run all data quality checks
        
        Args:
            df: DataFrame to validate
            table_name: Name of table for logging
            
        Returns:
            Dictionary with all check results
        """
        logger.info(f"Running data quality checks on {table_name}")
        
        results = {
            "table": table_name,
            "total_rows": len(df),
            "checks": []
        }
        
        # Run all checks
        checks = [
            self.check_null_values(df, table_name),
            self.check_duplicate_records(df),
            self.check_data_freshness(df)
        ]
        
        # Run price-specific checks if applicable
        if 'current_price' in df.columns:
            checks.append(self.check_price_validity(df))
            checks.append(self.check_price_change_anomalies(df))
        
        # Aggregate results
        all_passed = True
        for passed, check_result in checks:
            results["checks"].append(check_result)
            if not passed:
                all_passed = False
        
        results["all_passed"] = all_passed
        results["passed_count"] = sum(1 for p, _ in checks if p)
        results["total_checks"] = len(checks)
        
        if all_passed:
            logger.info(f"{table_name}: All quality checks passed ✓")
        else:
            logger.warning(f"{table_name}: Some quality checks failed", extra={
                "passed": results["passed_count"],
                "total": results["total_checks"]
            })
        
        return results