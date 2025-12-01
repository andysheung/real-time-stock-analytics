# Utilities Module

This module contains shared utilities for the Real-Time Stock Analytics Platform.

## HDFS Client (`hdfs_client.py`)

Provides a Python interface for HDFS operations.

### Features

- Directory operations (mkdir, ls, rm)
- File operations (put, get)
- Permission management (chmod)
- Health status checking
- Directory setup automation

### Usage

```python
from src.utils.hdfs_client import HDFSClient

# Initialize client
client = HDFSClient()

# Setup standard directories
client.setup_directories()

# List files
files = client.ls("/stock-data/raw/prices")

# Check if path exists
exists = client.exists("/stock-data/raw/prices")

# Get health status
health = client.get_health_status()
```

## HDFS Health Check (`hdfs_health_check.py`)

Comprehensive health check utility for HDFS cluster.

### Usage

```bash
python src/utils/hdfs_health_check.py
```

This will check:
- HDFS cluster health
- Required directories existence
- Directory accessibility
- Overall system status

## Spark Session Factory (`spark_session.py`)

Factory for creating configured Spark sessions with:
- Kafka integration
- MinIO/S3 configuration
- HDFS support
- Optimized Spark settings

### Usage

```python
from src.utils.spark_session import SparkSessionFactory

# Create Spark session
spark = SparkSessionFactory.create_session()

# Get data schemas
price_schema = SparkSessionFactory.get_stock_price_schema()
volume_schema = SparkSessionFactory.get_stock_volume_schema()
```
