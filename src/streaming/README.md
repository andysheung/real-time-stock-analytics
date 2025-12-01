# Spark Structured Streaming Module

This module contains Spark Structured Streaming applications for processing real-time stock data from Kafka.

## Overview

The streaming application consumes stock data from Kafka topics (`stock-prices` and `stock-volumes`), processes it in real-time with windowing and aggregations, and writes the results to MinIO/S3 storage.

## Architecture

```
Kafka Topics → Spark Streaming → Transformations → MinIO/S3
```

### Data Flow

1. **Raw Data Streams**: 
   - Consume from `stock-prices` and `stock-volumes` topics
   - Parse JSON messages
   - Write raw data to `stock-data-raw` bucket

2. **Aggregated Data Streams**:
   - Apply windowing operations (1-minute windows, 30-second slides)
   - Calculate aggregations (avg, max, min, sum)
   - Write aggregated data to `stock-data-lakehouse` bucket

## Components

### `streaming_app.py`
Main streaming application that orchestrates all streaming pipelines.

### `transformations.py`
Data transformation functions including:
- JSON parsing from Kafka
- Timestamp conversions
- Window-based aggregations
- Price change calculations
- Data filtering and validation

### `spark_session.py` (in `src/utils/`)
Spark session factory for creating configured Spark sessions with:
- Kafka integration
- MinIO/S3 configuration
- Optimized Spark settings

## Configuration

Configuration is managed through `config/spark_config.yaml`:

- **Kafka**: Bootstrap servers, topics, offset settings
- **Streaming**: Checkpoint location, trigger interval, output mode
- **MinIO/S3**: Endpoint, credentials, bucket names
- **Spark Session**: SQL settings, shuffle partitions

## Usage

### Prerequisites

1. Ensure Kafka and MinIO are running:
   ```bash
   docker-compose up -d
   ```

2. Start the Kafka producer to generate data:
   ```bash
   python src/ingestion/kafka_producer.py
   ```

### Running the Streaming Application

Run all streaming pipelines:
```bash
python src/streaming/streaming_app.py
```

### Programmatic Usage

```python
from src.streaming.streaming_app import StockStreamingApp

# Create app instance
app = StockStreamingApp()

# Run specific streams
price_query = app.run_price_stream()
volume_query = app.run_volume_stream()

# Or run all streams
app.run_all_streams()
```

## Output Structure

### Raw Data
- **Location**: `s3a://stock-data-raw/prices/raw/` and `s3a://stock-data-raw/volumes/raw/`
- **Format**: Parquet
- **Partitioning**: `year/month/day/hour/symbol`

### Aggregated Data
- **Location**: `s3a://stock-data-lakehouse/prices/aggregated/` and `s3a://stock-data-lakehouse/volumes/aggregated/`
- **Format**: Parquet
- **Partitioning**: `year/month/day/hour/symbol`
- **Window**: 1-minute tumbling windows with 30-second slides

## Checkpointing

Checkpoints are stored in `./checkpoints/` directory:
- `checkpoints/prices/` - Price stream checkpoints
- `checkpoints/volumes/` - Volume stream checkpoints
- `checkpoints/prices_aggregated/` - Aggregated price checkpoints
- `checkpoints/volumes_aggregated/` - Aggregated volume checkpoints

Checkpoints enable fault tolerance - if the application restarts, it will resume from the last processed offset.

## Monitoring

The application logs important events:
- Stream initialization
- Query start/stop
- Errors and exceptions
- Processing statistics

Check logs for:
- `INFO` - Normal operations
- `WARN` - Warnings (e.g., data loss)
- `ERROR` - Errors requiring attention

## Troubleshooting

### Kafka Connection Issues
- Verify Kafka is running: `docker ps | grep kafka`
- Check bootstrap servers in config match Docker setup
- Ensure topics exist: `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`

### MinIO Connection Issues
- Verify MinIO is running: `docker ps | grep minio`
- Check endpoint URL in config
- Verify buckets exist: Access MinIO console at http://localhost:9001

### Spark Dependency Issues
- Ensure PySpark is installed: `pip install pyspark>=3.5.0`
- Spark will download required JARs on first run (Kafka connector, S3 connector)

### Checkpoint Recovery
- If checkpoint is corrupted, delete the checkpoint directory and restart
- Application will start from `startingOffsets` configured in config

## Next Steps

- Phase 3: Add HDFS write capability
- Phase 4: Integrate with Airflow for orchestration
- Phase 5: Migrate to Apache Iceberg tables
