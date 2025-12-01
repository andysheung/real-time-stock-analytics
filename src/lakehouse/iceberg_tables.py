#!/usr/bin/env python3
"""
Apache Iceberg Table Definitions

Defines schemas and creates Iceberg tables for stock analytics.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    TimestampType, DateType, LongType
)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import SparkSessionFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IcebergTableManager:
    """Manages Apache Iceberg tables."""
    
    def __init__(self, spark: Optional[SparkSession] = None, config_path: Optional[str] = None):
        """
        Initialize Iceberg table manager.
        
        Args:
            spark: SparkSession instance
            config_path: Path to Iceberg config file
        """
        self.spark = spark or self._create_iceberg_spark_session(config_path)
        self.config = self._load_config(config_path)
        logger.info("Initialized IcebergTableManager")
    
    @staticmethod
    def _load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Iceberg configuration."""
        import yaml
        
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "iceberg_config.yaml"
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config.get('iceberg', {})
    
    @staticmethod
    def _create_iceberg_spark_session(config_path: Optional[str] = None) -> SparkSession:
        """
        Create Spark session configured for Iceberg.
        
        Args:
            config_path: Path to Spark config file
            
        Returns:
            Configured SparkSession
        """
        spark = SparkSessionFactory.create_session(config_path=config_path)
        
        # Configure Iceberg
        spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        spark.conf.set("spark.sql.catalog.stock_analytics", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.stock_analytics.type", "hadoop")
        spark.conf.set("spark.sql.catalog.stock_analytics.warehouse", "s3a://stock-data-lakehouse/iceberg-warehouse")
        
        # Configure S3/MinIO for Iceberg
        minio_config = SparkSessionFactory.load_config(config_path).get('minio', {})
        spark.conf.set("spark.sql.catalog.stock_analytics.s3.endpoint", minio_config.get('endpoint', 'http://localhost:9000'))
        spark.conf.set("spark.sql.catalog.stock_analytics.s3.access-key", minio_config.get('access_key', 'minioadmin'))
        spark.conf.set("spark.sql.catalog.stock_analytics.s3.secret-key", minio_config.get('secret_key', 'minioadmin'))
        spark.conf.set("spark.sql.catalog.stock_analytics.s3.path-style-access", "true")
        
        logger.info("Created Spark session with Iceberg support")
        return spark
    
    @staticmethod
    def get_price_table_schema() -> StructType:
        """
        Get schema for stock price Iceberg table.
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("date", DateType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("open", DoubleType(), nullable=True),
            StructField("high", DoubleType(), nullable=True),
            StructField("low", DoubleType(), nullable=True),
            StructField("volume", IntegerType(), nullable=True),
            StructField("previous_close", DoubleType(), nullable=True),
            StructField("price_change", DoubleType(), nullable=True),
            StructField("price_change_pct", DoubleType(), nullable=True),
            StructField("market_cap", DoubleType(), nullable=True),
            StructField("currency", StringType(), nullable=True),
        ])
    
    @staticmethod
    def get_volume_table_schema() -> StructType:
        """
        Get schema for stock volume Iceberg table.
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("date", DateType(), nullable=False),
            StructField("volume", IntegerType(), nullable=False),
            StructField("price", DoubleType(), nullable=True),
        ])
    
    @staticmethod
    def get_daily_aggregate_schema() -> StructType:
        """
        Get schema for daily aggregates Iceberg table.
        
        Returns:
            StructType schema
        """
        return StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("date", DateType(), nullable=False),
            StructField("avg_price", DoubleType(), nullable=True),
            StructField("max_price", DoubleType(), nullable=True),
            StructField("min_price", DoubleType(), nullable=True),
            StructField("max_high", DoubleType(), nullable=True),
            StructField("min_low", DoubleType(), nullable=True),
            StructField("total_volume", LongType(), nullable=True),
            StructField("record_count", LongType(), nullable=True),
            StructField("avg_price_change", DoubleType(), nullable=True),
            StructField("avg_price_change_pct", DoubleType(), nullable=True),
            StructField("price_range", DoubleType(), nullable=True),
            StructField("price_range_pct", DoubleType(), nullable=True),
        ])
    
    def create_price_table(self):
        """Create stock prices Iceberg table."""
        table_config = self.config.get('tables', {}).get('prices', {})
        table_name = f"stock_analytics.{table_config.get('database', 'stock_analytics')}.{table_config.get('name', 'stock_prices')}"
        location = table_config.get('location', 's3a://stock-data-lakehouse/iceberg/prices')
        
        logger.info(f"Creating Iceberg table: {table_name}")
        
        schema = self.get_price_table_schema()
        schema_ddl = self._schema_to_ddl(schema)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_ddl}
        ) USING ICEBERG
        PARTITIONED BY (symbol, days(date))
        LOCATION '{location}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        try:
            self.spark.sql(create_sql)
            logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    
    def create_volume_table(self):
        """Create stock volumes Iceberg table."""
        table_config = self.config.get('tables', {}).get('volumes', {})
        table_name = f"stock_analytics.{table_config.get('database', 'stock_analytics')}.{table_config.get('name', 'stock_volumes')}"
        location = table_config.get('location', 's3a://stock-data-lakehouse/iceberg/volumes')
        
        logger.info(f"Creating Iceberg table: {table_name}")
        
        schema = self.get_volume_table_schema()
        schema_ddl = self._schema_to_ddl(schema)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_ddl}
        ) USING ICEBERG
        PARTITIONED BY (symbol, days(date))
        LOCATION '{location}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        try:
            self.spark.sql(create_sql)
            logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    
    def create_daily_aggregate_table(self):
        """Create daily aggregates Iceberg table."""
        table_config = self.config.get('tables', {}).get('daily_aggregates', {})
        table_name = f"stock_analytics.{table_config.get('database', 'stock_analytics')}.{table_config.get('name', 'daily_aggregates')}"
        location = table_config.get('location', 's3a://stock-data-lakehouse/iceberg/daily_aggregates')
        
        logger.info(f"Creating Iceberg table: {table_name}")
        
        schema = self.get_daily_aggregate_schema()
        schema_ddl = self._schema_to_ddl(schema)
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_ddl}
        ) USING ICEBERG
        PARTITIONED BY (symbol, days(date))
        LOCATION '{location}'
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        try:
            self.spark.sql(create_sql)
            logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise
    
    def create_all_tables(self):
        """Create all Iceberg tables."""
        logger.info("Creating all Iceberg tables...")
        self.create_price_table()
        self.create_volume_table()
        self.create_daily_aggregate_table()
        logger.info("All Iceberg tables created successfully")
    
    @staticmethod
    def _schema_to_ddl(schema: StructType) -> str:
        """Convert Spark StructType to DDL string."""
        fields = []
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            fields.append(f"  {field.name} {field.dataType.simpleString().upper()} {nullable}")
        return ",\n".join(fields)
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()


def main():
    """Entry point for creating Iceberg tables."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create Apache Iceberg tables')
    parser.add_argument(
        '--table',
        type=str,
        choices=['prices', 'volumes', 'daily_aggregates', 'all'],
        default='all',
        help='Table to create (default: all)'
    )
    
    args = parser.parse_args()
    
    manager = None
    try:
        manager = IcebergTableManager()
        
        if args.table == 'all':
            manager.create_all_tables()
        elif args.table == 'prices':
            manager.create_price_table()
        elif args.table == 'volumes':
            manager.create_volume_table()
        elif args.table == 'daily_aggregates':
            manager.create_daily_aggregate_table()
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if manager:
            manager.stop()


if __name__ == '__main__':
    main()
