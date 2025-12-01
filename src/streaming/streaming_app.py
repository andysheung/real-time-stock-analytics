#!/usr/bin/env python3
"""
Spark Structured Streaming Application for Stock Data

Consumes stock data from Kafka topics, processes it in real-time,
and writes results to MinIO/S3 storage.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import SparkSessionFactory
from streaming.transformations import StockDataTransformations

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockStreamingApp:
    """Main Spark Structured Streaming application."""
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config_path: Optional[str] = None
    ):
        """
        Initialize the streaming application.
        
        Args:
            spark: SparkSession instance (creates new if None)
            config_path: Path to Spark config file
        """
        self.config_path = config_path
        self.spark = spark or SparkSessionFactory.create_session(config_path=config_path)
        self.config = SparkSessionFactory.load_config(config_path)
        self.transformations = StockDataTransformations()
        
        logger.info("Initialized StockStreamingApp")
    
    def read_from_kafka(self, topic: str, starting_offsets: str = "latest") -> DataFrame:
        """
        Read stream from Kafka topic.
        
        Args:
            topic: Kafka topic name
            starting_offsets: Starting offset ("earliest" or "latest")
            
        Returns:
            Streaming DataFrame
        """
        kafka_config = self.config.get('kafka', {})
        bootstrap_servers = kafka_config.get('bootstrap_servers', 'localhost:9092')
        consumer_group_id = kafka_config.get('consumer_group_id', 'stock-analytics-group')
        
        logger.info(f"Reading from Kafka topic: {topic} with consumer group: {consumer_group_id}")
        
        stream_reader = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", kafka_config.get('fail_on_data_loss', False))
        
        # Add consumer group ID if specified (for monitoring purposes)
        # Note: Spark Structured Streaming manages offsets via checkpoints,
        # but setting group.id helps with Kafka monitoring
        if consumer_group_id:
            stream_reader = stream_reader.option("kafka.group.id", consumer_group_id)
        
        return stream_reader.load()
    
    def process_price_stream(self) -> DataFrame:
        """
        Process stock price stream from Kafka.
        
        Returns:
            Processed streaming DataFrame
        """
        kafka_config = self.config.get('kafka', {})
        topic = kafka_config.get('topics', {}).get('stock_prices', 'stock-prices')
        
        # Read from Kafka
        kafka_df = self.read_from_kafka(topic)
        
        # Parse JSON
        schema = SparkSessionFactory.get_stock_price_schema()
        parsed_df = self.transformations.parse_kafka_json(kafka_df, schema)
        
        # Add processing timestamp
        parsed_df = self.transformations.add_processing_timestamp(parsed_df)
        
        # Convert timestamp
        parsed_df = self.transformations.convert_timestamp_to_datetime(parsed_df)
        
        # Filter valid records
        parsed_df = self.transformations.filter_valid_records(parsed_df)
        
        # Calculate price changes
        parsed_df = self.transformations.calculate_price_change(parsed_df)
        
        # Add partition columns
        parsed_df = self.transformations.add_partition_columns(parsed_df)
        
        logger.info("Price stream processing pipeline configured")
        
        return parsed_df
    
    def process_volume_stream(self) -> DataFrame:
        """
        Process stock volume stream from Kafka.
        
        Returns:
            Processed streaming DataFrame
        """
        kafka_config = self.config.get('kafka', {})
        topic = kafka_config.get('topics', {}).get('stock_volumes', 'stock-volumes')
        
        # Read from Kafka
        kafka_df = self.read_from_kafka(topic)
        
        # Parse JSON
        schema = SparkSessionFactory.get_stock_volume_schema()
        parsed_df = self.transformations.parse_kafka_json(kafka_df, schema)
        
        # Add processing timestamp
        parsed_df = self.transformations.add_processing_timestamp(parsed_df)
        
        # Convert timestamp
        parsed_df = self.transformations.convert_timestamp_to_datetime(parsed_df)
        
        # Filter valid records
        parsed_df = self.transformations.filter_valid_records(parsed_df)
        
        # Add partition columns
        parsed_df = self.transformations.add_partition_columns(parsed_df)
        
        logger.info("Volume stream processing pipeline configured")
        
        return parsed_df
    
    def write_to_minio(
        self,
        df: DataFrame,
        bucket: str,
        path: str,
        checkpoint_location: Optional[str] = None
    ):
        """
        Write streaming DataFrame to MinIO/S3.
        
        Args:
            df: Streaming DataFrame to write
            bucket: MinIO bucket name
            path: Path within bucket
            checkpoint_location: Checkpoint location for fault tolerance
        """
        streaming_config = self.config.get('streaming', {})
        minio_config = self.config.get('minio', {})
        
        if checkpoint_location is None:
            checkpoint_location = streaming_config.get('checkpoint_location', './checkpoints')
        
        # Construct S3 path
        s3_path = f"s3a://{bucket}/{path}"
        
        logger.info(f"Writing stream to: {s3_path}")
        
        query = df.writeStream \
            .outputMode(streaming_config.get('output_mode', 'append')) \
            .format(streaming_config.get('format', 'parquet')) \
            .option("path", s3_path) \
            .option("checkpointLocation", f"{checkpoint_location}/{path}") \
            .option("partitionBy", "year,month,day,hour,symbol") \
            .trigger(processingTime=streaming_config.get('trigger_interval', '10 seconds')) \
            .start()
        
        return query
    
    def write_to_hdfs(
        self,
        df: DataFrame,
        hdfs_path: str,
        checkpoint_location: Optional[str] = None
    ):
        """
        Write streaming DataFrame to HDFS.
        
        Args:
            df: Streaming DataFrame to write
            hdfs_path: HDFS path (full path starting with hdfs://)
            checkpoint_location: Checkpoint location for fault tolerance
        """
        streaming_config = self.config.get('streaming', {})
        hdfs_config = self.config.get('hdfs', {})
        
        if checkpoint_location is None:
            checkpoint_location = streaming_config.get('checkpoint_location', './checkpoints')
        
        # Ensure path starts with hdfs://
        if not hdfs_path.startswith('hdfs://'):
            namenode_url = hdfs_config.get('namenode_url', 'hdfs://localhost:9000')
            hdfs_path = f"{namenode_url}{hdfs_path}"
        
        logger.info(f"Writing stream to HDFS: {hdfs_path}")
        
        query = df.writeStream \
            .outputMode(streaming_config.get('output_mode', 'append')) \
            .format(streaming_config.get('format', 'parquet')) \
            .option("path", hdfs_path) \
            .option("checkpointLocation", f"{checkpoint_location}/hdfs{hdfs_path.replace('/', '_')}") \
            .option("partitionBy", "year,month,day,hour,symbol") \
            .trigger(processingTime=streaming_config.get('trigger_interval', '10 seconds')) \
            .start()
        
        return query
    
    def run_price_stream(self, write_to_hdfs: bool = False):
        """
        Run the price streaming pipeline.
        
        Args:
            write_to_hdfs: If True, also write to HDFS in addition to MinIO
        """
        logger.info("Starting price stream processing...")
        
        # Process price stream
        price_df = self.process_price_stream()
        
        queries = []
        
        # Write to MinIO
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        minio_query = self.write_to_minio(
            price_df,
            bucket=bucket,
            path="prices/raw",
            checkpoint_location=f"./checkpoints/prices"
        )
        queries.append(minio_query)
        
        # Optionally write to HDFS
        if write_to_hdfs:
            hdfs_config = self.config.get('hdfs', {})
            hdfs_path = hdfs_config.get('paths', {}).get('raw', {}).get('prices', '/stock-data/raw/prices')
            hdfs_query = self.write_to_hdfs(
                price_df,
                hdfs_path=hdfs_path,
                checkpoint_location=f"./checkpoints/prices_hdfs"
            )
            queries.append(hdfs_query)
            logger.info("Price stream writing to both MinIO and HDFS")
        
        logger.info("Price stream query started")
        return queries[0] if len(queries) == 1 else queries
    
    def run_volume_stream(self, write_to_hdfs: bool = False):
        """
        Run the volume streaming pipeline.
        
        Args:
            write_to_hdfs: If True, also write to HDFS in addition to MinIO
        """
        logger.info("Starting volume stream processing...")
        
        # Process volume stream
        volume_df = self.process_volume_stream()
        
        queries = []
        
        # Write to MinIO
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_raw', 'stock-data-raw')
        
        minio_query = self.write_to_minio(
            volume_df,
            bucket=bucket,
            path="volumes/raw",
            checkpoint_location=f"./checkpoints/volumes"
        )
        queries.append(minio_query)
        
        # Optionally write to HDFS
        if write_to_hdfs:
            hdfs_config = self.config.get('hdfs', {})
            hdfs_path = hdfs_config.get('paths', {}).get('raw', {}).get('volumes', '/stock-data/raw/volumes')
            hdfs_query = self.write_to_hdfs(
                volume_df,
                hdfs_path=hdfs_path,
                checkpoint_location=f"./checkpoints/volumes_hdfs"
            )
            queries.append(hdfs_query)
            logger.info("Volume stream writing to both MinIO and HDFS")
        
        logger.info("Volume stream query started")
        return queries[0] if len(queries) == 1 else queries
    
    def run_aggregated_streams(self):
        """
        Run aggregated streaming pipelines with windowing.
        
        Returns:
            List of streaming queries
        """
        logger.info("Starting aggregated stream processing...")
        
        queries = []
        
        # Process price stream
        price_df = self.process_price_stream()
        
        # Aggregate price data
        aggregated_price = self.transformations.aggregate_price_by_window(
            price_df,
            window_duration="1 minute",
            slide_duration="30 seconds"
        )
        
        # Write aggregated price data
        minio_config = self.config.get('minio', {})
        bucket = minio_config.get('bucket_lakehouse', 'stock-data-lakehouse')
        
        price_query = self.write_to_minio(
            aggregated_price,
            bucket=bucket,
            path="prices/aggregated",
            checkpoint_location=f"./checkpoints/prices_aggregated"
        )
        queries.append(price_query)
        
        # Process volume stream
        volume_df = self.process_volume_stream()
        
        # Aggregate volume data
        aggregated_volume = self.transformations.aggregate_volume_by_window(
            volume_df,
            window_duration="1 minute",
            slide_duration="30 seconds"
        )
        
        # Write aggregated volume data
        volume_query = self.write_to_minio(
            aggregated_volume,
            bucket=bucket,
            path="volumes/aggregated",
            checkpoint_location=f"./checkpoints/volumes_aggregated"
        )
        queries.append(volume_query)
        
        logger.info(f"Started {len(queries)} aggregated stream queries")
        
        return queries
    
    def run_all_streams(self):
        """Run all streaming pipelines."""
        logger.info("Starting all streaming pipelines...")
        
        queries = []
        
        # Raw data streams
        queries.append(self.run_price_stream())
        queries.append(self.run_volume_stream())
        
        # Aggregated streams
        aggregated_queries = self.run_aggregated_streams()
        queries.extend(aggregated_queries)
        
        logger.info(f"Started {len(queries)} streaming queries")
        
        # Wait for all queries
        for query in queries:
            query.awaitTermination()
    
    def stop(self):
        """Stop all streaming queries and Spark session."""
        logger.info("Stopping streaming application...")
        if self.spark:
            self.spark.stop()


def main():
    """Entry point for the streaming application."""
    app = None
    try:
        app = StockStreamingApp()
        app.run_all_streams()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in streaming application: {e}", exc_info=True)
        raise
    finally:
        if app:
            app.stop()


if __name__ == '__main__':
    main()
