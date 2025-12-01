#!/usr/bin/env python3
"""
Spark Session Factory Utility

Creates and configures Spark sessions for streaming and batch processing.
"""

import logging
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

logger = logging.getLogger(__name__)


class SparkSessionFactory:
    """Factory for creating configured Spark sessions."""
    
    @staticmethod
    def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load Spark configuration from YAML file.
        
        Args:
            config_path: Path to config file. Defaults to config/spark_config.yaml
            
        Returns:
            Configuration dictionary
        """
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "spark_config.yaml"
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config.get('spark', {})
    
    @staticmethod
    def create_session(
        app_name: Optional[str] = None,
        master: Optional[str] = None,
        config_path: Optional[str] = None,
        additional_config: Optional[Dict[str, str]] = None
    ) -> SparkSession:
        """
        Create a configured Spark session.
        
        Args:
            app_name: Spark application name
            master: Spark master URL (e.g., "local[*]")
            config_path: Path to config file
            additional_config: Additional Spark config options
            
        Returns:
            Configured SparkSession
        """
        config = SparkSessionFactory.load_config(config_path)
        
        # Build Spark session builder
        builder = SparkSession.builder
        
        # Set app name
        if app_name:
            builder = builder.appName(app_name)
        elif config.get('app_name'):
            builder = builder.appName(config['app_name'])
        else:
            builder = builder.appName("StockAnalytics")
        
        # Set master
        if master:
            builder = builder.master(master)
        elif config.get('master'):
            builder = builder.master(config['master'])
        
        # Configure for Kafka integration and Iceberg
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
        )
        
        # Configure for S3/MinIO access
        minio_config = config.get('minio', {})
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint", minio_config.get('endpoint', 'http://localhost:9000')
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.access.key", minio_config.get('access_key', 'minioadmin')
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.secret.key", minio_config.get('secret_key', 'minioadmin')
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.path.style.access", str(minio_config.get('path_style_access', True))
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        builder = builder.config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
        )
        
        # Configure for HDFS access
        hdfs_config = config.get('hdfs', {})
        if hdfs_config:
            namenode_url = hdfs_config.get('namenode_url', 'hdfs://localhost:9000')
            # Spark uses HDFS by default when path starts with hdfs://
            # No additional configuration needed for basic HDFS access
            logger.info(f"HDFS NameNode URL configured: {namenode_url}")
        
        # Apply session settings from config
        session_config = config.get('session', {})
        sql_config = session_config.get('sql', {})
        
        if sql_config.get('adaptive', {}).get('enabled'):
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            if sql_config['adaptive'].get('coalescePartitions', {}).get('enabled'):
                builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        if sql_config.get('shuffle', {}).get('partitions'):
            builder = builder.config(
                "spark.sql.shuffle.partitions",
                str(sql_config['shuffle']['partitions'])
            )
        
        # Apply additional config if provided
        if additional_config:
            for key, value in additional_config.items():
                builder = builder.config(key, value)
        
        # Create session
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Created Spark session: {spark.sparkContext.appName}")
        logger.info(f"Spark version: {spark.version}")
        
        return spark
    
    @staticmethod
    def get_stock_price_schema() -> StructType:
        """
        Get schema for stock price data from Kafka.
        
        Returns:
            StructType schema for stock price messages
        """
        return StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("price", DoubleType(), nullable=False),
            StructField("open", DoubleType(), nullable=True),
            StructField("high", DoubleType(), nullable=True),
            StructField("low", DoubleType(), nullable=True),
            StructField("volume", IntegerType(), nullable=True),
            StructField("previous_close", DoubleType(), nullable=True),
            StructField("market_cap", DoubleType(), nullable=True),
            StructField("currency", StringType(), nullable=True)
        ])
    
    @staticmethod
    def get_stock_volume_schema() -> StructType:
        """
        Get schema for stock volume data from Kafka.
        
        Returns:
            StructType schema for stock volume messages
        """
        return StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("timestamp", StringType(), nullable=False),
            StructField("volume", IntegerType(), nullable=False),
            StructField("price", DoubleType(), nullable=True)
        ])
