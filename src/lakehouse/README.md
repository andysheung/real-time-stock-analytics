# Data Lakehouse Module (Apache Iceberg)

This module contains Apache Iceberg table management, data migration, and optimization utilities.

## Overview

Apache Iceberg provides advanced table management features for data lakehouses:
- **Schema Evolution**: Add, remove, or modify columns without breaking queries
- **Time Travel**: Query historical versions of data
- **Partitioning**: Efficient data organization and query performance
- **ACID Transactions**: Reliable data updates and consistency

## Components

### `iceberg_tables.py`
Manages Iceberg table creation and schema definitions.

**Tables**:
- `stock_prices` - Raw stock price data
- `stock_volumes` - Raw stock volume data
- `daily_aggregates` - Daily aggregated stock data

**Usage**:
```bash
# Create all tables
python src/lakehouse/iceberg_tables.py --table all

# Create specific table
python src/lakehouse/iceberg_tables.py --table prices
```

### `data_migration.py`
Migrates data from raw storage (MinIO/HDFS) to Iceberg tables.

**Usage**:
```bash
# Migrate all data types
python src/lakehouse/data_migration.py --type all --start-date 2025-11-30

# Migrate specific data type
python src/lakehouse/data_migration.py --type prices --start-date 2025-11-30 --end-date 2025-11-30
```

### `table_optimization.py`
Optimizes Iceberg tables through compaction and expiration.

**Operations**:
- **Compaction**: Rewrites small files into larger files for better performance
- **Snapshot Expiration**: Removes old snapshots to save storage
- **Orphan File Removal**: Cleans up files no longer referenced

**Usage**:
```bash
# Optimize all tables
python src/lakehouse/table_optimization.py

# Optimize specific table
python src/lakehouse/table_optimization.py --table stock_analytics.stock_analytics.stock_prices --operation all
```

### `time_travel_queries.py`
Demonstrates time travel queries on Iceberg tables.

**Features**:
- List all snapshots
- Query data at specific snapshot or timestamp
- Compare data between snapshots
- View table history
- Rollback to previous snapshot

**Usage**:
```bash
# List snapshots
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation list-snapshots

# Query at snapshot
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation query-snapshot --snapshot-id 123456

# Query at timestamp
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation query-timestamp --timestamp "2025-11-30 10:00:00"

# Compare snapshots
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation compare --snapshot-id 123456 --snapshot-id-2 123457
```

## Configuration

Iceberg configuration is in `config/iceberg_config.yaml`:
- Catalog settings (Hadoop catalog with S3/MinIO)
- Table definitions and locations
- Partition specifications
- Optimization settings

## Table Schemas

### Stock Prices Table
- Partitioned by: `symbol`, `days(date)`
- Columns: symbol, timestamp, date, price, open, high, low, volume, etc.

### Stock Volumes Table
- Partitioned by: `symbol`, `days(date)`
- Columns: symbol, timestamp, date, volume, price

### Daily Aggregates Table
- Partitioned by: `symbol`, `days(date)`
- Columns: symbol, date, avg_price, max_price, min_price, total_volume, etc.

## Time Travel Examples

### Query Historical Data
```python
from pyspark.sql import SparkSession
from lakehouse.iceberg_tables import IcebergTableManager

manager = IcebergTableManager()
spark = manager.spark

# Query at specific snapshot
df = spark.sql("SELECT * FROM stock_analytics.stock_analytics.stock_prices VERSION AS OF 123456")

# Query at specific timestamp
df = spark.sql("SELECT * FROM stock_analytics.stock_analytics.stock_prices TIMESTAMP AS OF '2025-11-30 10:00:00'")
```

### Compare Versions
```python
# Compare two snapshots
df = spark.sql("""
    SELECT 
        s1.symbol,
        s1.price as price_v1,
        s2.price as price_v2
    FROM stock_analytics.stock_analytics.stock_prices VERSION AS OF 123456 s1
    JOIN stock_analytics.stock_analytics.stock_prices VERSION AS OF 123457 s2
    ON s1.symbol = s2.symbol AND s1.timestamp = s2.timestamp
""")
```

## Schema Evolution

Iceberg supports schema evolution without breaking existing queries:

```python
# Add a new column
spark.sql("ALTER TABLE stock_analytics.stock_analytics.stock_prices ADD COLUMN new_field STRING")

# Rename a column
spark.sql("ALTER TABLE stock_analytics.stock_analytics.stock_prices RENAME COLUMN old_field TO new_field")

# Change column type
spark.sql("ALTER TABLE stock_analytics.stock_analytics.stock_prices ALTER COLUMN price TYPE DOUBLE")
```

## Best Practices

1. **Regular Compaction**: Run compaction regularly to maintain query performance
2. **Snapshot Expiration**: Configure automatic snapshot expiration to manage storage
3. **Partitioning**: Use appropriate partitioning strategy for your query patterns
4. **Time Travel**: Use time travel for auditing and debugging
5. **Schema Evolution**: Plan schema changes carefully to maintain compatibility

## Integration with Airflow

Iceberg operations can be integrated into Airflow DAGs:

```python
from airflow.operators.bash import BashOperator

migrate_task = BashOperator(
    task_id='migrate_to_iceberg',
    bash_command='python /opt/airflow/lakehouse/data_migration.py --type all --start-date {{ ds }}',
    dag=dag
)

optimize_task = BashOperator(
    task_id='optimize_iceberg_tables',
    bash_command='python /opt/airflow/lakehouse/table_optimization.py',
    dag=dag
)
```
