# Batch Processing Module

This module contains Spark batch processing jobs for stock data analytics.

## Jobs

### `daily_aggregation.py`
**Purpose**: Aggregates raw stock data into daily summaries

**Features**:
- Reads raw price and volume data from MinIO/HDFS
- Aggregates by symbol and date
- Calculates daily statistics (avg, max, min, sum)
- Writes aggregated data to lakehouse storage

**Usage**:
```bash
python src/batch/daily_aggregation.py --date 2025-11-30
```

**Output**:
- Aggregated price data: `s3a://stock-data-lakehouse/prices/daily/year=YYYY/month=MM/day=DD/`
- Aggregated volume data: `s3a://stock-data-lakehouse/volumes/daily/year=YYYY/month=MM/day=DD/`

### `data_quality_check.py`
**Purpose**: Validates data quality using custom checks

**Features**:
- Checks record counts
- Validates null values (< 5% threshold)
- Validates data ranges (positive prices, non-negative volumes)
- Checks for duplicates (< 10% threshold)
- Generates quality reports

**Usage**:
```bash
python src/batch/data_quality_check.py --date 2025-11-30
```

**Checks Performed**:
- Record count validation
- Null value checks
- Data range validation
- Duplicate detection
- Schema validation

## Integration with Airflow

These batch jobs are orchestrated by Airflow DAGs:
- Daily aggregation runs at 2 AM
- Data quality checks run hourly

See `src/dags/` for DAG definitions.

## Configuration

Jobs use configuration from:
- `config/spark_config.yaml` - Spark and storage settings
- `config/airflow_config.yaml` - Airflow and scheduling settings

## Error Handling

- Jobs log errors and exit with non-zero status on failure
- Airflow handles retries based on DAG configuration
- Failed jobs can be retried manually via Airflow UI

## Data Paths

### Input (Raw Data)
- Prices: `s3a://stock-data-raw/prices/raw/year=YYYY/month=MM/day=DD/`
- Volumes: `s3a://stock-data-raw/volumes/raw/year=YYYY/month=MM/day=DD/`

### Output (Aggregated Data)
- Prices: `s3a://stock-data-lakehouse/prices/daily/year=YYYY/month=MM/day=DD/`
- Volumes: `s3a://stock-data-lakehouse/volumes/daily/year=YYYY/month=MM/day=DD/`
