#!/usr/bin/env python3
"""
Daily Aggregation Batch Job

Aggregates stock data from raw storage and creates daily summaries.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, avg, max as spark_max, min as spark_min, sum as spark_sum,
    count, date_format, to_date, lit
)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import SparkSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DailyAggregationJob:
    """Daily aggregation batch processing job."""
    
    def __init__(self, spark: Optional[SparkSession] = None, config_path: Optional[str] = None):
        """
        Initialize the batch job.
        
        Args:
            spark: SparkSession instance
            config_path: Path to Spark config file
        """
        self.spark = spark or SparkSessionFactory.create_session(config_path=config_path)
        self.config = SparkSessionFactory.load_config(config_path)
        logger.info("Initialized DailyAggregationJob")
    
    def read_raw_prices(self, date: Optional[str] = None) -> 'DataFrame':
        """
        Read raw price data from MinIO or HDFS.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
            
        Returns:
            DataFrame with raw price data
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        # Construct path with date partitioning
        year, month, day = date.split('-')
        s3_path = f"s3a://{bucket}/prices/raw/year={year}/month={month}/day={day}"
        
        logger.info(f"Reading raw price data from: {s3_path}")
        
        try:
            df = self.spark.read.parquet(s3_path)
            logger.info(f"Read {df.count()} records from raw prices")
            return df
        except Exception as e:
            logger.error(f"Error reading raw prices: {e}")
            # Return empty DataFrame with schema
            schema = SparkSessionFactory.get_stock_price_schema()
            return self.spark.createDataFrame([], schema)
    
    def read_raw_volumes(self, date: Optional[str] = None) -> 'DataFrame':
        """
        Read raw volume data from MinIO or HDFS.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
            
        Returns:
            DataFrame with raw volume data
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        # Construct path with date partitioning
        year, month, day = date.split('-')
        s3_path = f"s3a://{bucket}/volumes/raw/year={year}/month={month}/day={day}"
        
        logger.info(f"Reading raw volume data from: {s3_path}")
        
        try:
            df = self.spark.read.parquet(s3_path)
            logger.info(f"Read {df.count()} records from raw volumes")
            return df
        except Exception as e:
            logger.error(f"Error reading raw volumes: {e}")
            # Return empty DataFrame with schema
            schema = SparkSessionFactory.get_stock_volume_schema()
            return self.spark.createDataFrame([], schema)
    
    def aggregate_daily_prices(self, df: 'DataFrame', date: str) -> 'DataFrame':
        """
        Aggregate price data by symbol for a given date.
        
        Args:
            df: Input DataFrame with price data
            date: Date string (YYYY-MM-DD)
            
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Aggregating daily prices for {date}")
        
        # Convert timestamp to date if needed
        if 'event_timestamp' in df.columns:
            date_col = to_date(col('event_timestamp'))
        elif 'timestamp' in df.columns:
            date_col = to_date(col('timestamp'))
        else:
            date_col = to_date(lit(date))
        
        aggregated = df.groupBy(
            col('symbol'),
            date_col.alias('date')
        ).agg(
            avg('price').alias('avg_price'),
            spark_max('price').alias('max_price'),
            spark_min('price').alias('min_price'),
            spark_max('high').alias('max_high'),
            spark_min('low').alias('min_low'),
            spark_sum('volume').alias('total_volume'),
            count('*').alias('record_count'),
            avg('price_change').alias('avg_price_change'),
            avg('price_change_pct').alias('avg_price_change_pct')
        ).withColumn(
            'price_range',
            col('max_price') - col('min_price')
        ).withColumn(
            'price_range_pct',
            (col('price_range') / col('avg_price')) * 100
        )
        
        return aggregated
    
    def aggregate_daily_volumes(self, df: 'DataFrame', date: str) -> 'DataFrame':
        """
        Aggregate volume data by symbol for a given date.
        
        Args:
            df: Input DataFrame with volume data
            date: Date string (YYYY-MM-DD)
            
        Returns:
            Aggregated DataFrame
        """
        logger.info(f"Aggregating daily volumes for {date}")
        
        # Convert timestamp to date if needed
        if 'event_timestamp' in df.columns:
            date_col = to_date(col('event_timestamp'))
        elif 'timestamp' in df.columns:
            date_col = to_date(col('timestamp'))
        else:
            date_col = to_date(lit(date))
        
        aggregated = df.groupBy(
            col('symbol'),
            date_col.alias('date')
        ).agg(
            spark_sum('volume').alias('total_volume'),
            avg('volume').alias('avg_volume'),
            spark_max('volume').alias('max_volume'),
            spark_min('volume').alias('min_volume'),
            avg('price').alias('avg_price'),
            count('*').alias('record_count')
        )
        
        return aggregated
    
    def write_aggregated_data(self, df: 'DataFrame', data_type: str, date: str):
        """
        Write aggregated data to MinIO/HDFS.
        
        Args:
            df: Aggregated DataFrame
            data_type: 'prices' or 'volumes'
            date: Date string (YYYY-MM-DD)
        """
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_lakehouse', 'stock-data-lakehouse')
        
        # Construct output path with date partitioning
        year, month, day = date.split('-')
        output_path = f"s3a://{bucket}/{data_type}/daily/year={year}/month={month}/day={day}"
        
        logger.info(f"Writing aggregated {data_type} to: {output_path}")
        
        df.write.mode('overwrite').parquet(output_path)
        logger.info(f"Successfully wrote aggregated {data_type} data")
    
    def run(self, date: Optional[str] = None):
        """
        Run the daily aggregation job.
        
        Args:
            date: Date string (YYYY-MM-DD). If None, uses yesterday.
        """
        if date is None:
            yesterday = datetime.now() - timedelta(days=1)
            date = yesterday.strftime("%Y-%m-%d")
        
        logger.info(f"Starting daily aggregation job for date: {date}")
        
        try:
            # Process prices
            price_df = self.read_raw_prices(date)
            if price_df.count() > 0:
                aggregated_prices = self.aggregate_daily_prices(price_df, date)
                self.write_aggregated_data(aggregated_prices, 'prices', date)
            else:
                logger.warning(f"No price data found for {date}")
            
            # Process volumes
            volume_df = self.read_raw_volumes(date)
            if volume_df.count() > 0:
                aggregated_volumes = self.aggregate_daily_volumes(volume_df, date)
                self.write_aggregated_data(aggregated_volumes, 'volumes', date)
            else:
                logger.warning(f"No volume data found for {date}")
            
            logger.info(f"Daily aggregation job completed for {date}")
            
        except Exception as e:
            logger.error(f"Error in daily aggregation job: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()


def main():
    """Entry point for the batch job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Aggregation Batch Job')
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Date to process (YYYY-MM-DD). Defaults to yesterday.'
    )
    
    args = parser.parse_args()
    
    job = None
    try:
        job = DailyAggregationJob()
        job.run(date=args.date)
    except Exception as e:
        logger.error(f"Batch job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if job:
            job.stop()


if __name__ == '__main__':
    main()
