#!/usr/bin/env python3
"""
Data Quality Check Batch Job

Validates data quality using Great Expectations.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import SparkSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityCheckJob:
    """Data quality validation job using Great Expectations."""
    
    def __init__(self, spark: Optional[SparkSession] = None, config_path: Optional[str] = None):
        """
        Initialize the data quality job.
        
        Args:
            spark: SparkSession instance
            config_path: Path to Spark config file
        """
        self.spark = spark or SparkSessionFactory.create_session(config_path=config_path)
        self.config = SparkSessionFactory.load_config(config_path)
        logger.info("Initialized DataQualityCheckJob")
    
    def check_price_data_quality(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Check data quality for price data.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
            
        Returns:
            Dictionary with validation results
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        logger.info(f"Checking price data quality for {date}")
        
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        year, month, day = date.split('-')
        s3_path = f"s3a://{bucket}/prices/raw/year={year}/month={month}/day={day}"
        
        results = {
            'date': date,
            'data_type': 'prices',
            'path': s3_path,
            'checks': {},
            'passed': True,
            'errors': []
        }
        
        try:
            df = self.spark.read.parquet(s3_path)
            record_count = df.count()
            
            results['checks']['record_count'] = {
                'value': record_count,
                'passed': record_count > 0,
                'message': f"Found {record_count} records"
            }
            
            if record_count == 0:
                results['passed'] = False
                results['errors'].append("No records found")
                return results
            
            # Check for null values in critical columns
            null_checks = {
                'symbol': df.filter(col('symbol').isNull()).count(),
                'price': df.filter(col('price').isNull()).count(),
                'timestamp': df.filter(col('timestamp').isNull()).count()
            }
            
            for col_name, null_count in null_checks.items():
                null_pct = (null_count / record_count) * 100
                passed = null_pct < 5.0  # Allow up to 5% nulls
                results['checks'][f'{col_name}_nulls'] = {
                    'value': null_count,
                    'percentage': null_pct,
                    'passed': passed,
                    'message': f"{null_count} null values ({null_pct:.2f}%)"
                }
                if not passed:
                    results['passed'] = False
                    results['errors'].append(f"Too many nulls in {col_name}")
            
            # Check price values are positive
            negative_prices = df.filter(col('price') <= 0).count()
            results['checks']['positive_prices'] = {
                'value': negative_prices,
                'passed': negative_prices == 0,
                'message': f"{negative_prices} non-positive prices"
            }
            if negative_prices > 0:
                results['passed'] = False
                results['errors'].append("Found non-positive prices")
            
            # Check for duplicate records
            duplicate_count = df.count() - df.dropDuplicates(['symbol', 'timestamp']).count()
            duplicate_pct = (duplicate_count / record_count) * 100
            results['checks']['duplicates'] = {
                'value': duplicate_count,
                'percentage': duplicate_pct,
                'passed': duplicate_pct < 10.0,  # Allow up to 10% duplicates
                'message': f"{duplicate_count} duplicate records ({duplicate_pct:.2f}%)"
            }
            if duplicate_pct >= 10.0:
                results['passed'] = False
                results['errors'].append("Too many duplicate records")
            
            logger.info(f"Price data quality check completed: {'PASSED' if results['passed'] else 'FAILED'}")
            
        except Exception as e:
            logger.error(f"Error checking price data quality: {e}", exc_info=True)
            results['passed'] = False
            results['errors'].append(str(e))
        
        return results
    
    def check_volume_data_quality(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Check data quality for volume data.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
            
        Returns:
            Dictionary with validation results
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        logger.info(f"Checking volume data quality for {date}")
        
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        year, month, day = date.split('-')
        s3_path = f"s3a://{bucket}/volumes/raw/year={year}/month={month}/day={day}"
        
        results = {
            'date': date,
            'data_type': 'volumes',
            'path': s3_path,
            'checks': {},
            'passed': True,
            'errors': []
        }
        
        try:
            df = self.spark.read.parquet(s3_path)
            record_count = df.count()
            
            results['checks']['record_count'] = {
                'value': record_count,
                'passed': record_count > 0,
                'message': f"Found {record_count} records"
            }
            
            if record_count == 0:
                results['passed'] = False
                results['errors'].append("No records found")
                return results
            
            # Check for null values
            null_checks = {
                'symbol': df.filter(col('symbol').isNull()).count(),
                'volume': df.filter(col('volume').isNull()).count()
            }
            
            for col_name, null_count in null_checks.items():
                null_pct = (null_count / record_count) * 100
                passed = null_pct < 5.0
                results['checks'][f'{col_name}_nulls'] = {
                    'value': null_count,
                    'percentage': null_pct,
                    'passed': passed,
                    'message': f"{null_count} null values ({null_pct:.2f}%)"
                }
                if not passed:
                    results['passed'] = False
                    results['errors'].append(f"Too many nulls in {col_name}")
            
            # Check volume values are non-negative
            negative_volumes = df.filter(col('volume') < 0).count()
            results['checks']['non_negative_volumes'] = {
                'value': negative_volumes,
                'passed': negative_volumes == 0,
                'message': f"{negative_volumes} negative volumes"
            }
            if negative_volumes > 0:
                results['passed'] = False
                results['errors'].append("Found negative volumes")
            
            logger.info(f"Volume data quality check completed: {'PASSED' if results['passed'] else 'FAILED'}")
            
        except Exception as e:
            logger.error(f"Error checking volume data quality: {e}", exc_info=True)
            results['passed'] = False
            results['errors'].append(str(e))
        
        return results
    
    def run(self, date: Optional[str] = None):
        """
        Run data quality checks.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        logger.info(f"Starting data quality checks for date: {date}")
        
        price_results = self.check_price_data_quality(date)
        volume_results = self.check_volume_data_quality(date)
        
        # Log summary
        logger.info("=" * 60)
        logger.info("Data Quality Check Summary")
        logger.info("=" * 60)
        logger.info(f"Date: {date}")
        logger.info(f"Price Data: {'PASSED' if price_results['passed'] else 'FAILED'}")
        logger.info(f"Volume Data: {'PASSED' if volume_results['passed'] else 'FAILED'}")
        
        if not price_results['passed']:
            logger.error(f"Price data quality issues: {price_results['errors']}")
        if not volume_results['passed']:
            logger.error(f"Volume data quality issues: {volume_results['errors']}")
        
        overall_passed = price_results['passed'] and volume_results['passed']
        
        if not overall_passed:
            logger.error("Data quality checks failed")
            sys.exit(1)
        else:
            logger.info("All data quality checks passed")
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()


def main():
    """Entry point for the data quality job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Quality Check Batch Job')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Date to check (YYYY-MM-DD). Defaults to yesterday.'
    )
    
    args = parser.parse_args()
    
    job = None
    try:
        job = DataQualityCheckJob()
        job.run(date=args.date)
    except Exception as e:
        logger.error(f"Data quality check failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if job:
            job.stop()


if __name__ == '__main__':
    main()
