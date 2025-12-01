#!/usr/bin/env python3
"""
Data Migration Script

Migrates data from raw storage (MinIO/HDFS) to Apache Iceberg tables.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, lit

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import SparkSessionFactory
from lakehouse.iceberg_tables import IcebergTableManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataMigration:
    """Migrates data to Iceberg tables."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize data migration.
        
        Args:
            spark: SparkSession instance
        """
        self.manager = IcebergTableManager(spark=spark)
        self.spark = self.manager.spark
        logger.info("Initialized DataMigration")
    
    def migrate_prices(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Migrate price data to Iceberg table.
        
        Args:
            start_date: Start date (YYYY-MM-DD). If None, uses yesterday.
            end_date: End date (YYYY-MM-DD). If None, uses start_date.
        """
        if start_date is None:
            yesterday = datetime.now() - timedelta(days=1)
            start_date = yesterday.strftime("%Y-%m-%d")
        
        if end_date is None:
            end_date = start_date
        
        logger.info(f"Migrating price data from {start_date} to {end_date}")
        
        minio_config = SparkSessionFactory.load_config().get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        # Read raw data
        year, month, day = start_date.split('-')
        s3_path = f"s3a://{bucket}/prices/raw/year={year}/month={month}/day={day}"
        
        try:
            df = self.spark.read.parquet(s3_path)
            
            # Transform data for Iceberg
            df = df.withColumn("date", to_date(col("event_timestamp"))) \
                   .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                   .select(
                       col("symbol"),
                       col("timestamp"),
                       col("date"),
                       col("price"),
                       col("open"),
                       col("high"),
                       col("low"),
                       col("volume"),
                       col("previous_close"),
                       col("price_change"),
                       col("price_change_pct"),
                       col("market_cap"),
                       col("currency")
                   )
            
            # Write to Iceberg table
            table_name = "stock_analytics.stock_analytics.stock_prices"
            df.writeTo(table_name).append()
            
            logger.info(f"Successfully migrated {df.count()} price records")
            
        except Exception as e:
            logger.error(f"Error migrating price data: {e}", exc_info=True)
            raise
    
    def migrate_volumes(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Migrate volume data to Iceberg table.
        
        Args:
            start_date: Start date (YYYY-MM-DD). If None, uses yesterday.
            end_date: End date (YYYY-MM-DD). If None, uses start_date.
        """
        if start_date is None:
            yesterday = datetime.now() - timedelta(days=1)
            start_date = yesterday.strftime("%Y-%m-%d")
        
        if end_date is None:
            end_date = start_date
        
        logger.info(f"Migrating volume data from {start_date} to {end_date}")
        
        minio_config = SparkSessionFactory.load_config().get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        # Read raw data
        year, month, day = start_date.split('-')
        s3_path = f"s3a://{bucket}/volumes/raw/year={year}/month={month}/day={day}"
        
        try:
            df = self.spark.read.parquet(s3_path)
            
            # Transform data for Iceberg
            df = df.withColumn("date", to_date(col("event_timestamp"))) \
                   .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                   .select(
                       col("symbol"),
                       col("timestamp"),
                       col("date"),
                       col("volume"),
                       col("price")
                   )
            
            # Write to Iceberg table
            table_name = "stock_analytics.stock_analytics.stock_volumes"
            df.writeTo(table_name).append()
            
            logger.info(f"Successfully migrated {df.count()} volume records")
            
        except Exception as e:
            logger.error(f"Error migrating volume data: {e}", exc_info=True)
            raise
    
    def migrate_daily_aggregates(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Migrate daily aggregate data to Iceberg table.
        
        Args:
            start_date: Start date (YYYY-MM-DD). If None, uses yesterday.
            end_date: End date (YYYY-MM-DD). If None, uses start_date.
        """
        if start_date is None:
            yesterday = datetime.now() - timedelta(days=1)
            start_date = yesterday.strftime("%Y-%m-%d")
        
        if end_date is None:
            end_date = start_date
        
        logger.info(f"Migrating daily aggregate data from {start_date} to {end_date}")
        
        minio_config = SparkSessionFactory.load_config().get('minio', {})
        bucket = minio_config.get('bucket_lakehouse', 'stock-data-lakehouse')
        
        # Read aggregated data
        year, month, day = start_date.split('-')
        s3_path = f"s3a://{bucket}/prices/daily/year={year}/month={month}/day={day}"
        
        try:
            df = self.spark.read.parquet(s3_path)
            
            # Transform data for Iceberg
            df = df.withColumn("date", to_date(col("window_start"))) \
                   .select(
                       col("symbol"),
                       col("date"),
                       col("avg_price"),
                       col("max_price"),
                       col("min_price"),
                       col("max_high"),
                       col("min_low"),
                       col("total_volume"),
                       col("record_count"),
                       col("avg_price_change"),
                       col("avg_price_change_pct"),
                       col("price_range"),
                       col("price_range_pct")
                   )
            
            # Write to Iceberg table
            table_name = "stock_analytics.stock_analytics.daily_aggregates"
            df.writeTo(table_name).append()
            
            logger.info(f"Successfully migrated {df.count()} daily aggregate records")
            
        except Exception as e:
            logger.error(f"Error migrating daily aggregate data: {e}", exc_info=True)
            raise
    
    def migrate_all(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Migrate all data types to Iceberg tables.
        
        Args:
            start_date: Start date (YYYY-MM-DD). If None, uses yesterday.
            end_date: End date (YYYY-MM-DD). If None, uses start_date.
        """
        logger.info("Starting migration of all data types")
        
        self.migrate_prices(start_date, end_date)
        self.migrate_volumes(start_date, end_date)
        self.migrate_daily_aggregates(start_date, end_date)
        
        logger.info("Migration completed successfully")
    
    def stop(self):
        """Stop Spark session."""
        if self.manager:
            self.manager.stop()


def main():
    """Entry point for data migration."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate data to Iceberg tables')
    parser.add_argument(
        '--type',
        type=str,
        choices=['prices', 'volumes', 'daily_aggregates', 'all'],
        default='all',
        help='Data type to migrate'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date (YYYY-MM-DD)'
    )
    
    args = parser.parse_args()
    
    migration = None
    try:
        migration = DataMigration()
        
        if args.type == 'all':
            migration.migrate_all(args.start_date, args.end_date)
        elif args.type == 'prices':
            migration.migrate_prices(args.start_date, args.end_date)
        elif args.type == 'volumes':
            migration.migrate_volumes(args.start_date, args.end_date)
        elif args.type == 'daily_aggregates':
            migration.migrate_daily_aggregates(args.start_date, args.end_date)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if migration:
            migration.stop()


if __name__ == '__main__':
    main()
