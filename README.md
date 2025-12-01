# Real-Time Stock Analytics Platform

A comprehensive data engineering platform for real-time stock market data ingestion, processing, and analytics.

## Architecture Overview

This platform implements a modern data lakehouse architecture with the following layers:

1. **Data Ingestion**: Yahoo Finance API → Kafka
2. **Stream Processing**: Apache Spark Structured Streaming
3. **Raw Storage**: MinIO (local) / S3 (production)
4. **Distributed Storage**: HDFS
5. **Batch Processing**: Apache Spark + Airflow
6. **Data Lakehouse**: Apache Iceberg
7. **Query & Analytics**: Trino + PostgreSQL
8. **Visualization**: Grafana + Prometheus

## Documentation

- [Setup Guide](docs/SETUP_GUIDE.md) - **Rebuild from scratch!** Complete step-by-step guide to recreate the entire project
- [Testing and Usage Guide](docs/TESTING_AND_USAGE.md) - **Start here!** Comprehensive guide for testing and using the platform
- [Production Deployment Guide](docs/PRODUCTION_DEPLOYMENT.md)
- [Security Guide](docs/SECURITY.md)
- [Performance Tuning Guide](docs/PERFORMANCE_TUNING.md)
- [Operational Runbooks](docs/RUNBOOKS.md)
- [API Documentation](docs/API.md)

## Quick Start

> **📖 For detailed testing and usage instructions, see [Testing and Usage Guide](docs/TESTING_AND_USAGE.md)**

### Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Git
- At least 8GB RAM available

### Initial Setup

1. **Create and activate virtual environment:**
   ```bash
   # Create virtual environment
   python3 -m venv venv
   
   # Activate virtual environment
   # On macOS/Linux:
   source venv/bin/activate
   # On Windows:
   venv\Scripts\activate
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   
   **Note:** Always ensure your virtual environment is activated before installing packages or running Python scripts.

3. **Start all infrastructure services:**
   ```bash
   docker-compose up -d
   ```
   
   Wait for services to be healthy (2-3 minutes). Check with:
   ```bash
   docker-compose ps
   ```

3. **Verify services are running:**
   ```bash
   # Check Kafka
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   
   # Run health check
   ./scripts/health_check.sh
   ```

4. **Activate virtual environment (if not already activated):**
   ```bash
   source venv/bin/activate  # On macOS/Linux
   # venv\Scripts\activate  # On Windows
   ```

5. **Start data ingestion:**
   ```bash
   # Terminal 1: Start Kafka producer
   python src/ingestion/kafka_producer.py
   ```

6. **Start stream processing:**
   ```bash
   # Terminal 2: Start Spark streaming
   python src/streaming/streaming_app.py
   ```

7. **Access Web UIs:**
   - **Grafana**: http://localhost:3000 (admin/admin)
   - **Airflow**: http://localhost:8080 (airflow/airflow)
   - **Trino**: http://localhost:8081 (admin)
   - **Prometheus**: http://localhost:9090
   - **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

### Quick Test

Run the complete integration test:
```bash
# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# venv\Scripts\activate  # On Windows

# Start services
docker-compose up -d

# Wait for services (2-3 minutes)
sleep 180

# Run health check
./scripts/health_check.sh

# Start data flow
python src/ingestion/kafka_producer.py &
python src/streaming/streaming_app.py &

# Wait for data processing (1-2 minutes)
sleep 120

# Query data in Trino Web UI: http://localhost:8081
# Or check Grafana dashboards: http://localhost:3000
```

### Access Points

- **Kafka**: `localhost:9092`
- **MinIO Console**: http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)
- **MinIO API**: http://localhost:9000
- **HDFS NameNode Web UI**: http://localhost:9870
- **HDFS NameNode RPC**: `localhost:9000`
- **Airflow Web UI**: http://localhost:8080 (username: `airflow`, password: `airflow`)
- **PostgreSQL**: `localhost:5432`
- **Trino Web UI**: http://localhost:8081 (username: `admin`, no password)
- **Grafana**: http://localhost:3000 (username: `admin`, password: `admin`)
- **Prometheus**: http://localhost:9090

## Project Structure

```
.
├── docker-compose.yml          # Infrastructure orchestration
├── requirements.txt            # Python dependencies
├── src/
│   ├── ingestion/             # Data ingestion scripts
│   ├── streaming/             # Spark streaming jobs
│   ├── batch/                 # Batch processing jobs
│   ├── dags/                  # Airflow DAGs
│   └── utils/                 # Shared utilities
├── config/                    # Configuration files
├── tests/                     # Unit and integration tests
└── docs/                      # Documentation

```

## Development Roadmap

- [x] Phase 1: Foundation & Infrastructure (Kafka, MinIO, Data Ingestion)
- [x] Phase 2: Stream Processing Layer (Spark Structured Streaming)
- [x] Phase 3: Distributed Storage (HDFS)
- [x] Phase 4: Batch Processing & Orchestration (Airflow)
- [x] Phase 5: Data Lakehouse (Apache Iceberg)
- [x] Phase 6: Query Engine (Trino)
- [x] Phase 7: Monitoring & Visualization (Grafana, Prometheus)
- [x] Phase 8: Production Readiness

See [PLAN.md](PLAN.md) for detailed development plan and status.

## HDFS Usage

### Check HDFS Health

```bash
python src/utils/hdfs_health_check.py
```

### Use HDFS Client

```python
from src.utils.hdfs_client import HDFSClient

client = HDFSClient()
client.setup_directories()
client.ls("/stock-data")
```

### Write to HDFS from Spark Streaming

The streaming application supports writing to HDFS. Update your code to enable HDFS writes:

```python
from src.streaming.streaming_app import StockStreamingApp

app = StockStreamingApp()
# Enable HDFS writes
app.run_price_stream(write_to_hdfs=True)
app.run_volume_stream(write_to_hdfs=True)
```

## Batch Processing with Airflow

### Access Airflow UI

1. Start all services:
   ```bash
   docker-compose up -d
   ```

2. Wait for Airflow to initialize (may take a few minutes)

3. Access Airflow Web UI:
   - URL: http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

### Run Batch Jobs Manually

```bash
# Daily aggregation
python src/batch/daily_aggregation.py --date 2025-11-30

# Data quality check
python src/batch/data_quality_check.py --date 2025-11-30
```

### Scheduled Workflows

Airflow DAGs are automatically scheduled:
- **Daily Aggregation**: Runs at 2 AM daily
- **Hourly Data Quality**: Runs every hour

View and manage DAGs in the Airflow UI.

## Data Lakehouse with Apache Iceberg

### Create Iceberg Tables

```bash
# Create all Iceberg tables
python src/lakehouse/iceberg_tables.py --table all
```

### Migrate Data to Iceberg

```bash
# Migrate all data types
python src/lakehouse/data_migration.py --type all --start-date 2025-11-30

# Migrate specific data type
python src/lakehouse/data_migration.py --type prices --start-date 2025-11-30
```

### Optimize Tables

```bash
# Optimize all tables
python src/lakehouse/table_optimization.py

# Optimize specific table
python src/lakehouse/table_optimization.py --table stock_analytics.stock_analytics.stock_prices
```

### Time Travel Queries

```bash
# List snapshots
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation list-snapshots

# Query at specific snapshot
python src/lakehouse/time_travel_queries.py --table stock_analytics.stock_analytics.stock_prices --operation query-snapshot --snapshot-id 123456
```

See `src/lakehouse/README.md` for detailed documentation.

## Query Engine with Trino

### Access Trino Web UI

1. Start all services:
   ```bash
   docker-compose up -d
   ```

2. Wait for Trino to initialize (may take a minute)

3. Access Trino Web UI:
   - URL: http://localhost:8081
   - Default user: `admin` (no password)

### Run SQL Queries

```sql
-- Connect to Iceberg catalog
USE iceberg.stock_analytics;

-- Query latest prices
SELECT * FROM stock_prices 
WHERE symbol = 'AAPL' 
    AND date = CURRENT_DATE
ORDER BY timestamp DESC
LIMIT 10;

-- Get daily aggregates
SELECT * FROM daily_aggregates
WHERE symbol = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY date DESC;
```

### Query Examples

See `src/queries/stock_analytics_queries.sql` for comprehensive query examples including:
- Basic queries
- Aggregations
- Time series analysis
- Comparative analysis
- Volume analysis
- Time travel queries

See `src/queries/README.md` for detailed query documentation.

## Monitoring & Visualization

### Access Grafana

1. Start all services:
   ```bash
   docker-compose up -d
   ```

2. Access Grafana Web UI:
   - URL: http://localhost:3000
   - Username: `admin`
   - Password: `admin`

3. Access Prometheus:
   - URL: http://localhost:9090

### Available Dashboards

- **Infrastructure Health**: Service status, Kafka topics, storage usage
- **Data Pipeline Metrics**: Message rates, consumer lag, error rates
- **Stock Analytics**: Price trends, volume, market summary

Dashboards are automatically provisioned in Grafana.

### Alerting

Prometheus alerting rules are configured for:
- Infrastructure failures (Kafka, MinIO, HDFS, Trino)
- Data pipeline issues (high lag, low throughput, errors)
- Storage warnings (high disk usage, low DataNodes)

See `src/monitoring/README.md` for detailed monitoring documentation.
