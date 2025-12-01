#!/usr/bin/env python3
"""
Data Transformation Functions for Spark Streaming

Contains transformation logic for stock data processing.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, avg, sum as spark_sum, max as spark_max, min as spark_min,
    count, from_json, to_timestamp, current_timestamp, lit
)
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class StockDataTransformations:
    """Transformation functions for stock data streaming."""
    
    @staticmethod
    def parse_kafka_json(df: DataFrame, schema: StructType, value_column: str = "value") -> DataFrame:
        """
        Parse JSON string from Kafka value column.
        
        Args:
            df: Input DataFrame with Kafka data
            schema: Schema for the JSON data
            value_column: Name of the column containing JSON string
            
        Returns:
            DataFrame with parsed JSON columns
        """
        return df.select(
            col("key").cast("string").alias("kafka_key"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col(value_column).cast("string"), schema).alias("data")
        ).select("kafka_key", "kafka_partition", "kafka_offset", "kafka_timestamp", "data.*")
    
    @staticmethod
    def add_processing_timestamp(df: DataFrame) -> DataFrame:
        """
        Add processing timestamp column.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with processing_timestamp column
        """
        return df.withColumn("processing_timestamp", current_timestamp())
    
    @staticmethod
    def convert_timestamp_to_datetime(df: DataFrame, timestamp_col: str = "timestamp") -> DataFrame:
        """
        Convert ISO timestamp string to TimestampType.
        
        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            
        Returns:
            DataFrame with converted timestamp
        """
        return df.withColumn(
            "event_timestamp",
            to_timestamp(col(timestamp_col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
        )
    
    @staticmethod
    def aggregate_price_by_window(
        df: DataFrame,
        window_duration: str = "1 minute",
        slide_duration: str = "30 seconds"
    ) -> DataFrame:
        """
        Aggregate stock price data by time window.
        
        Args:
            df: Input DataFrame with stock price data
            window_duration: Window duration (e.g., "1 minute", "5 minutes")
            slide_duration: Slide duration for sliding windows
            
        Returns:
            Aggregated DataFrame with window-based metrics
        """
        return df.groupBy(
            col("symbol"),
            window(col("event_timestamp"), window_duration, slide_duration)
        ).agg(
            avg("price").alias("avg_price"),
            spark_max("price").alias("max_price"),
            spark_min("price").alias("min_price"),
            spark_max("high").alias("max_high"),
            spark_min("low").alias("min_low"),
            spark_sum("volume").alias("total_volume"),
            count("*").alias("record_count")
        ).select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_price"),
            col("max_price"),
            col("min_price"),
            col("max_high"),
            col("min_low"),
            col("total_volume"),
            col("record_count")
        )
    
    @staticmethod
    def aggregate_volume_by_window(
        df: DataFrame,
        window_duration: str = "1 minute",
        slide_duration: str = "30 seconds"
    ) -> DataFrame:
        """
        Aggregate stock volume data by time window.
        
        Args:
            df: Input DataFrame with stock volume data
            window_duration: Window duration
            slide_duration: Slide duration for sliding windows
            
        Returns:
            Aggregated DataFrame with volume metrics
        """
        return df.groupBy(
            col("symbol"),
            window(col("event_timestamp"), window_duration, slide_duration)
        ).agg(
            spark_sum("volume").alias("total_volume"),
            avg("volume").alias("avg_volume"),
            spark_max("volume").alias("max_volume"),
            avg("price").alias("avg_price"),
            count("*").alias("record_count")
        ).select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_volume"),
            col("avg_volume"),
            col("max_volume"),
            col("avg_price"),
            col("record_count")
        )
    
    @staticmethod
    def calculate_price_change(df: DataFrame) -> DataFrame:
        """
        Calculate price change metrics.
        
        Args:
            df: Input DataFrame with price data
            
        Returns:
            DataFrame with price change calculations
        """
        return df.withColumn(
            "price_change",
            col("price") - col("previous_close")
        ).withColumn(
            "price_change_pct",
            (col("price_change") / col("previous_close")) * 100
        )
    
    @staticmethod
    def filter_valid_records(df: DataFrame) -> DataFrame:
        """
        Filter out invalid or null records.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(
            col("symbol").isNotNull() &
            col("price").isNotNull() &
            (col("price") > 0)
        )
    
    @staticmethod
    def add_partition_columns(df: DataFrame) -> DataFrame:
        """
        Add partition columns for efficient storage (year, month, day, hour).
        
        Args:
            df: Input DataFrame with event_timestamp
            
        Returns:
            DataFrame with partition columns
        """
        return df.withColumn("year", col("event_timestamp").year()) \
                 .withColumn("month", col("event_timestamp").month()) \
                 .withColumn("day", col("event_timestamp").dayofmonth()) \
                 .withColumn("hour", col("event_timestamp").hour())
