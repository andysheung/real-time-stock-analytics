# API Documentation

This document describes the APIs and interfaces available in the Real-Time Stock Analytics Platform.

## Overview

The platform provides several interfaces:
- Kafka topics for data ingestion
- Spark APIs for data processing
- Trino SQL for querying
- Airflow REST API for orchestration
- Grafana API for dashboards

## Kafka Topics API

### Topics

#### `stock-prices`
**Description**: Real-time stock price data

**Message Format**:
```json
{
  "symbol": "AAPL",
  "timestamp": "2025-11-30T10:00:00Z",
  "price": 150.25,
  "open": 149.50,
  "high": 151.00,
  "low": 149.00,
  "volume": 1000000,
  "previous_close": 149.75,
  "price_change": 0.50,
  "price_change_pct": 0.33,
  "market_cap": 2500000000000,
  "currency": "USD"
}
```

#### `stock-volumes`
**Description**: Real-time stock volume data

**Message Format**:
```json
{
  "symbol": "AAPL",
  "timestamp": "2025-11-30T10:00:00Z",
  "volume": 1000000,
  "price": 150.25
}
```

### Producer API

```python
from src.ingestion.kafka_producer import StockDataProducer

producer = StockDataProducer(
    bootstrap_servers='localhost:9092',
    stock_symbols=['AAPL', 'GOOGL'],
    fetch_interval=5
)
producer.run()
```

### Consumer API

```python
from src.ingestion.kafka_consumer import StockDataConsumer

consumer = StockDataConsumer(
    bootstrap_servers='localhost:9092',
    topics=['stock-prices']
)
consumer.consume()
```

## Spark Streaming API

### Streaming Application

```python
from src.streaming.streaming_app import StockStreamingApp

app = StockStreamingApp()
app.run_all_streams()
```

### Transformations

```python
from src.streaming.transformations import StockDataTransformations

transformations = StockDataTransformations()
aggregated_df = transformations.aggregate_price_by_window(
    df,
    window_duration="1 minute",
    slide_duration="30 seconds"
)
```

## Trino SQL API

### Query Interface

Access via:
- Web UI: http://localhost:8081
- CLI: `trino --server http://localhost:8081`
- JDBC/ODBC drivers
- Python client: `trino` package

### Example Queries

See `src/queries/stock_analytics_queries.sql` for comprehensive examples.

## Airflow REST API

### Endpoints

**Base URL**: http://localhost:8080/api/v1

**Authentication**: Basic Auth (username/password)

**Key Endpoints**:
- `GET /dags` - List all DAGs
- `GET /dags/{dag_id}` - Get DAG details
- `POST /dags/{dag_id}/dagRuns` - Trigger DAG run
- `GET /dags/{dag_id}/dagRuns` - List DAG runs
- `GET /dags/{dag_id}/taskInstances` - Get task instances

### Example Usage

```bash
# Trigger DAG
curl -X POST \
  http://localhost:8080/api/v1/dags/stock_analytics_batch_processing/dagRuns \
  -u airflow:airflow \
  -H "Content-Type: application/json" \
  -d '{"conf": {}}'
```

## Grafana API

### Endpoints

**Base URL**: http://localhost:3000/api

**Authentication**: API key or Basic Auth

**Key Endpoints**:
- `GET /dashboards` - List dashboards
- `GET /dashboards/{uid}` - Get dashboard
- `POST /dashboards/db` - Create dashboard
- `GET /datasources` - List datasources

## Batch Processing API

### Daily Aggregation Job

```bash
python src/batch/daily_aggregation.py --date 2025-11-30
```

### Data Quality Check

```bash
python src/batch/data_quality_check.py --date 2025-11-30
```

## Iceberg Table API

### Create Tables

```bash
python src/lakehouse/iceberg_tables.py --table all
```

### Migrate Data

```bash
python src/lakehouse/data_migration.py --type all --start-date 2025-11-30
```

### Optimize Tables

```bash
python src/lakehouse/table_optimization.py
```

## Monitoring API

### Prometheus Metrics

**Endpoint**: http://localhost:9090/api/v1/query

**Example Query**:
```bash
curl 'http://localhost:9090/api/v1/query?query=up{job="kafka"}'
```

### Grafana Dashboards

Access via Grafana Web UI: http://localhost:3000

## Error Handling

All APIs follow standard error handling:
- HTTP status codes for REST APIs
- Exception handling in Python APIs
- Logging for debugging

## Rate Limiting

- Kafka: Configurable per producer/consumer
- Trino: Query timeout and memory limits
- Airflow: Task concurrency limits
- Grafana: No default limits (configure as needed)

## Authentication

### Development
- Basic authentication
- Default credentials in config files

### Production
- OAuth/LDAP for web UIs
- SASL for Kafka
- SSL/TLS for all connections
- Secrets management for credentials

## Versioning

- APIs are versioned where applicable
- Breaking changes documented
- Deprecation notices provided

## Support

For API issues:
- Check documentation
- Review logs
- Contact support team
