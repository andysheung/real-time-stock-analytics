# Complete Setup Guide - Rebuild from Scratch

This guide will walk you through rebuilding the entire Real-Time Stock Analytics Platform from zero. Follow each section step-by-step to recreate the complete project.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Configuration Files](#configuration-files)
4. [Docker Compose Setup](#docker-compose-setup)
5. [Python Source Code](#python-source-code)
6. [Scripts](#scripts)
7. [Documentation](#documentation)
8. [CI/CD Setup](#cicd-setup)
9. [Initialization](#initialization)
10. [Verification](#verification)

---

## Prerequisites

### System Requirements

- **OS**: macOS, Linux, or Windows (with WSL2)
- **RAM**: Minimum 8GB (16GB recommended)
- **Disk Space**: At least 20GB free
- **CPU**: Multi-core processor recommended

### Software Installation

1. **Install Docker and Docker Compose**
   ```bash
   # macOS (using Homebrew)
   brew install docker docker-compose
   
   # Linux (Ubuntu/Debian)
   sudo apt-get update
   sudo apt-get install docker.io docker-compose
   
   # Verify installation
   docker --version
   docker-compose --version
   ```

2. **Install Python 3.9+**
   ```bash
   # macOS
   brew install python@3.11
   
   # Linux
   sudo apt-get install python3.11 python3-pip
   
   # Verify installation
   python3 --version
   pip3 --version
   ```
   
   **Note:** Python's `venv` module is included by default in Python 3.3+. Verify it's available:
   ```bash
   python3 -m venv --help
   ```

3. **Install Git** (if not already installed)
   ```bash
   git --version
   ```

---

## Project Structure

Create the following directory structure:

```bash
# Create project root directory
mkdir real-time-stock-analytics
cd real-time-stock-analytics

# Create main directories
mkdir -p src/ingestion
mkdir -p src/streaming
mkdir -p src/batch
mkdir -p src/dags
mkdir -p src/lakehouse
mkdir -p src/queries
mkdir -p src/utils
mkdir -p src/monitoring
mkdir -p config/trino/catalog
mkdir -p config/prometheus
mkdir -p config/grafana/provisioning/datasources
mkdir -p config/grafana/provisioning/dashboards
mkdir -p config/grafana/dashboards
mkdir -p docs
mkdir -p scripts
mkdir -p logs
mkdir -p plugins
mkdir -p .github/workflows
mkdir -p checkpoints
```

---

## Configuration Files

### 1. Root Files

#### `.gitignore`

Create `.gitignore`:

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
ENV/
*.egg-info/
dist/
build/

# Spark
checkpoints/
spark-warehouse/
metastore_db/
derby.log

# Airflow
logs/
plugins/
airflow_data/
great_expectations/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Environment
.env
.env.local

# Data
*.parquet
*.csv
data/
```

#### `requirements.txt`

Create `requirements.txt`:

```txt
# Data Ingestion
yfinance>=0.2.28
kafka-python>=2.0.2

# Spark
pyspark>=3.5.0

# Storage
minio>=7.2.0
hdfs>=2.7.0

# Workflow Orchestration
apache-airflow>=2.7.0
apache-airflow-providers-postgres>=5.7.0
apache-airflow-providers-docker>=3.7.0

# Data Lakehouse
pyiceberg>=0.5.0

# Query Engine
trino>=0.327.0

# Monitoring
prometheus-client>=0.19.0

# Utilities
pyyaml>=6.0.1
python-dotenv>=1.0.0
requests>=2.31.0
```

#### `README.md`

Create `README.md` (see existing file for full content, or create a basic one):

```markdown
# Real-Time Stock Analytics Platform

A comprehensive data platform for ingesting, processing, and analyzing real-time stock market data.

## Quick Start

See [Testing and Usage Guide](docs/TESTING_AND_USAGE.md) for detailed instructions.

## Documentation

- [Setup Guide](docs/SETUP_GUIDE.md) - Rebuild from scratch
- [Testing and Usage Guide](docs/TESTING_AND_USAGE.md)
- [Production Deployment Guide](docs/PRODUCTION_DEPLOYMENT.md)
- [Security Guide](docs/SECURITY.md)
- [Performance Tuning Guide](docs/PERFORMANCE_TUNING.md)
- [Operational Runbooks](docs/RUNBOOKS.md)
- [API Documentation](docs/API.md)
```

### 2. Configuration Files

#### `config/kafka_config.yaml`

```yaml
# Kafka Configuration
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    stock_prices: "stock-prices"
    stock_volumes: "stock-volumes"
  consumer:
    group_id: "stock-analytics-group"
    auto_offset_reset: "earliest"
    enable_auto_commit: true
  producer:
    acks: "all"
    retries: 3
    batch_size: 16384
    linger_ms: 10
```

#### `config/spark_config.yaml`

```yaml
# Spark Configuration
spark:
  app_name: "StockAnalyticsStreaming"
  master: "local[*]"
  kafka:
    bootstrap_servers: "localhost:9092"
    topics:
      stock_prices: "stock-prices"
      stock_volumes: "stock-volumes"
    starting_offsets: "latest"
    fail_on_data_loss: false
  streaming:
    checkpoint_location: "./checkpoints"
    trigger_interval: "10 seconds"
    output_mode: "append"
    format: "parquet"
  minio:
    endpoint: "http://localhost:9000"
    access_key: "minioadmin"
    secret_key: "minioadmin"
    bucket_raw: "stock-data-raw"
    bucket_lakehouse: "stock-data-lakehouse"
    path_style_access: true
  hdfs:
    namenode_host: "localhost"
    namenode_port: 9000
    namenode_web_ui: "http://localhost:9870"
    paths:
      base: "/stock-data"
      raw:
        prices: "/stock-data/raw/prices"
        volumes: "/stock-data/raw/volumes"
      lakehouse:
        prices: "/stock-data/lakehouse/prices"
        volumes: "/stock-data/lakehouse/volumes"
    replication_factor: 2
  session:
    sql:
      adaptive:
        enabled: true
        coalescePartitions:
          enabled: true
      shuffle:
        partitions: 200
    streaming:
      checkpointLocation: "./checkpoints"
      stopGracefullyOnShutdown: true
```

#### `config/hdfs_config.yaml`

```yaml
# HDFS Configuration
hdfs:
  namenode_host: "localhost"
  namenode_port: 9000
  namenode_web_ui: "http://localhost:9870"
  paths:
    base: "/stock-data"
    raw:
      prices: "/stock-data/raw/prices"
      volumes: "/stock-data/raw/volumes"
    lakehouse:
      prices: "/stock-data/lakehouse/prices"
      volumes: "/stock-data/lakehouse/volumes"
  replication_factor: 2
  block_size: 134217728  # 128MB
  client:
    timeout: 30000  # milliseconds
    retry_count: 3
```

#### `config/airflow_config.yaml`

```yaml
# Airflow Configuration
airflow:
  database:
    host: "postgres"
    port: 5432
    user: "airflow"
    password: "airflow"
    db_name: "airflow"
  executor: "LocalExecutor"
  dags:
    default_args:
      owner: "data-engineering"
      depends_on_past: false
      email_on_failure: false
      email_on_retry: false
      retries: 2
      retry_delay_minutes: 5
    daily_schedule: "0 2 * * *"  # 2 AM daily
    hourly_schedule: "0 * * * *"  # Every hour
  spark:
    app_name: "StockAnalyticsBatch"
    master: "local[*]"
    config_path: "/opt/airflow/config/spark_config.yaml"
  data_quality:
    expectations_store_path: "./great_expectations/expectations"
    checkpoint_store_path: "./great_expectations/checkpoints"
    validation_results_path: "./great_expectations/validation_results"
```

#### `config/iceberg_config.yaml`

```yaml
# Apache Iceberg Configuration
iceberg:
  catalog:
    type: "jdbc"  # Options: jdbc, hive, rest
    name: "stock_analytics"
    uri: "thrift://localhost:9083"  # Hive Metastore (if using Hive catalog)
    jdbc:
      uri: "jdbc:postgresql://postgres:5432/iceberg"
      warehouse: "s3a://stock-data-lakehouse/iceberg-warehouse"
      user: "airflow"
      password: "airflow"
  tables:
    prices:
      name: "stock_prices"
      database: "stock_analytics"
      location: "s3a://stock-data-lakehouse/iceberg/prices"
      partition_spec:
        - field: "symbol"
          transform: "identity"
        - field: "date"
          transform: "day"
    volumes:
      name: "stock_volumes"
      database: "stock_analytics"
      location: "s3a://stock-data-lakehouse/iceberg/volumes"
      partition_spec:
        - field: "symbol"
          transform: "identity"
        - field: "date"
          transform: "day"
    daily_aggregates:
      name: "daily_aggregates"
      database: "stock_analytics"
      location: "s3a://stock-data-lakehouse/iceberg/daily_aggregates"
      partition_spec:
        - field: "symbol"
          transform: "identity"
        - field: "date"
          transform: "day"
  optimization:
    compaction:
      enabled: true
      schedule: "daily"
      target_file_size_mb: 128
    expiration:
      enabled: true
      snapshot_retention_days: 7
      orphan_file_retention_hours: 24
```

#### `config/trino/config.properties`

```properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery.uri=http://trino-coordinator:8080

query.max-memory=5GB
query.max-memory-per-node=2GB
query.max-total-memory=10GB
```

#### `config/trino/jvm.config`

```config
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
```

#### `config/trino/node.properties`

```properties
node.environment=production
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.data-dir=/var/trino/data
```

#### `config/trino/catalog/iceberg.properties`

```properties
connector.name=iceberg
catalog.type=hadoop
catalog.warehouse=s3a://stock-data-lakehouse/iceberg-warehouse
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
```

#### `config/trino/catalog/hive.properties`

```properties
connector.name=hive
hive.metastore.uri=thrift://localhost:9083
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
```

#### `config/trino/catalog/tpch.properties`

```properties
connector.name=tpch
tpch.splits-per-node=4
```

#### `config/prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: 'stock-analytics'
    environment: 'development'

alerting:
  alertmanagers:
    - static_configs:
        - targets: []

rule_files:
  - '/etc/prometheus/alerts.yml'

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9092']

  - job_name: 'minio'
    static_configs:
      - targets: ['minio:9000']

  - job_name: 'hdfs'
    static_configs:
      - targets: ['namenode:9870']

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres:5432']

  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']

  - job_name: 'trino'
    static_configs:
      - targets: ['trino-coordinator:8080']

  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
```

#### `config/prometheus/alerts.yml`

```yaml
groups:
  - name: infrastructure_health
    interval: 30s
    rules:
      - alert: KafkaDown
        expr: up{job="kafka"} == 0
        for: 1m
        annotations:
          summary: "Kafka broker is down"
          
      - alert: MinIODown
        expr: up{job="minio"} == 0
        for: 1m
        annotations:
          summary: "MinIO is down"
          
      - alert: HDFSDown
        expr: up{job="hdfs"} == 0
        for: 1m
        annotations:
          summary: "HDFS NameNode is down"

  - name: data_pipeline
    interval: 30s
    rules:
      - alert: HighKafkaLag
        expr: kafka_consumer_lag_sum > 10000
        for: 5m
        annotations:
          summary: "High Kafka consumer lag detected"
          
      - alert: HighErrorRate
        expr: rate(kafka_errors_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High error rate in data pipeline"
```

#### `config/grafana/provisioning/datasources/prometheus.yml`

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

#### `config/grafana/provisioning/dashboards/dashboards.yml`

```yaml
apiVersion: 1

providers:
  - name: 'Default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
```

#### `config/grafana/dashboards/infrastructure-health.json`

Create a basic dashboard JSON (see existing file for full content, or create minimal version):

```json
{
  "dashboard": {
    "title": "Infrastructure Health",
    "panels": []
  }
}
```

---

## Docker Compose Setup

Create `docker-compose.yml` with all services. See the existing file for complete content, or create with these key services:

- Zookeeper
- Kafka
- MinIO
- HDFS (NameNode, DataNodes)
- PostgreSQL
- Airflow (Scheduler, Webserver)
- Trino (Coordinator, Worker)
- Prometheus
- Grafana

**Note**: The complete `docker-compose.yml` is extensive. Copy it from the existing project or build it incrementally following the phases in PLAN.md.

---

## Python Source Code

### Module Initialization Files

Create `__init__.py` files in each module:

```bash
touch src/__init__.py
touch src/ingestion/__init__.py
touch src/streaming/__init__.py
touch src/batch/__init__.py
touch src/dags/__init__.py
touch src/lakehouse/__init__.py
touch src/queries/__init__.py
touch src/utils/__init__.py
touch src/monitoring/__init__.py
```

### Core Python Files

**Important**: The Python source files are extensive. You have two options:

1. **Copy from existing project** (recommended): If you have access to the existing project, copy all Python files from `src/` directory.

2. **Create from scratch**: Follow the file structure below and implement each file. Refer to the existing project files for implementation details.

**File Structure** (all files should be created in `src/` directory):

#### `src/ingestion/kafka_producer.py`
- Fetches stock data from Yahoo Finance
- Publishes to Kafka topics

#### `src/ingestion/kafka_consumer.py`
- Consumes messages from Kafka
- Displays data

#### `src/utils/spark_session.py`
- Creates configured Spark sessions
- Handles Spark configuration

#### `src/utils/hdfs_client.py`
- HDFS client utilities
- Directory operations

#### `src/utils/hdfs_health_check.py`
- HDFS health check script

#### `src/streaming/transformations.py`
- Spark SQL transformation functions

#### `src/streaming/streaming_app.py`
- Main Spark Structured Streaming application

#### `src/streaming/run_streaming.py`
- Convenience script to run streaming

#### `src/batch/daily_aggregation.py`
- Daily aggregation batch job

#### `src/batch/data_quality_check.py`
- Data quality checks

#### `src/dags/stock_analytics_dag.py`
- Airflow DAG for daily processing

#### `src/dags/hourly_data_quality_dag.py`
- Airflow DAG for hourly quality checks

#### `src/lakehouse/iceberg_tables.py`
- Iceberg table definitions and creation

#### `src/lakehouse/data_migration.py`
- Data migration scripts

#### `src/lakehouse/table_optimization.py`
- Table optimization scripts

#### `src/lakehouse/time_travel_queries.py`
- Time travel query examples

---

## Scripts

### `scripts/deploy.sh`

```bash
#!/bin/bash
set -e

echo "Starting deployment..."

# Pre-deployment checks
echo "Running pre-deployment checks..."
docker --version
docker-compose --version

# Stop existing services
echo "Stopping existing services..."
docker-compose down

# Start services
echo "Starting services..."
docker-compose up -d

# Wait for services
echo "Waiting for services to be healthy..."
sleep 60

# Health checks
echo "Running health checks..."
./scripts/health_check.sh

echo "Deployment completed!"
```

### `scripts/health_check.sh`

```bash
#!/bin/bash

echo "Running health checks..."

# Check Kafka
echo "Checking Kafka..."
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 || echo "Kafka check failed"

# Check MinIO
echo "Checking MinIO..."
curl -f http://localhost:9000/minio/health/live || echo "MinIO check failed"

# Check HDFS
echo "Checking HDFS..."
curl -f http://localhost:9870 || echo "HDFS check failed"

# Check Airflow
echo "Checking Airflow..."
curl -f http://localhost:8080/health || echo "Airflow check failed"

# Check Trino
echo "Checking Trino..."
curl -f http://localhost:8081/v1/info || echo "Trino check failed"

echo "Health checks completed!"
```

### `scripts/backup.sh`

```bash
#!/bin/bash
set -e

BACKUP_DIR="./backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "Starting backup to $BACKUP_DIR..."

# Backup MinIO data
echo "Backing up MinIO..."
docker exec minio mc mirror myminio/ "$BACKUP_DIR/minio/" || echo "MinIO backup failed"

# Backup HDFS data
echo "Backing up HDFS..."
docker exec namenode hdfs dfs -get /stock-data "$BACKUP_DIR/hdfs/" || echo "HDFS backup failed"

# Backup PostgreSQL
echo "Backing up PostgreSQL..."
docker exec postgres pg_dump -U airflow airflow > "$BACKUP_DIR/postgres.sql" || echo "PostgreSQL backup failed"

# Backup configs
echo "Backing up configurations..."
cp -r config "$BACKUP_DIR/" || echo "Config backup failed"

echo "Backup completed: $BACKUP_DIR"
```

**Make scripts executable:**
```bash
chmod +x scripts/*.sh
```

---

## Documentation

Create documentation files in `docs/`:

- `SETUP_GUIDE.md` (this file)
- `TESTING_AND_USAGE.md`
- `PRODUCTION_DEPLOYMENT.md`
- `SECURITY.md`
- `PERFORMANCE_TUNING.md`
- `RUNBOOKS.md`
- `API.md`

---

## CI/CD Setup

### `.github/workflows/ci.yml`

```yaml
name: CI Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install flake8 yamllint
      - name: Lint Python
        run: |
          flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics
      - name: Lint YAML
        run: |
          yamllint config/ docker-compose.yml

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
      - name: Run basic tests
        run: |
          python -m pytest tests/ || echo "No tests yet"

  docker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Validate docker-compose
        run: |
          docker-compose config
```

---

## Initialization

### Step 1: Create and Activate Virtual Environment

**⚠️ Important:** Using a virtual environment is **required** for this project to ensure dependency isolation and avoid conflicts with system Python packages.

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Verify activation (you should see (venv) in your prompt)
# The virtual environment name should appear in your terminal prompt
```

**Note:** You must activate the virtual environment every time you open a new terminal session. To deactivate later, simply run:
```bash
deactivate
```

### Step 2: Install Dependencies

```bash
# Ensure virtual environment is activated (check for (venv) in prompt)
# Install Python packages
pip install --upgrade pip  # Upgrade pip to latest version
pip install -r requirements.txt

# Verify installation
pip list
```

**Note:** All Python scripts in this project should be run with the virtual environment activated. If you see import errors or missing modules, ensure your virtual environment is active.

### Step 3: Start Infrastructure

```bash
# Start all Docker services
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps

# Check logs if needed
docker-compose logs -f
```

### Step 4: Initialize Services

#### Initialize MinIO Buckets

```bash
# Wait for MinIO to be ready
sleep 30

# Create buckets
docker exec minio mc mb myminio/stock-data-raw
docker exec minio mc mb myminio/stock-data-lakehouse
docker exec minio mc mb myminio/iceberg-warehouse

# Set public read policy (optional, for testing)
docker exec minio mc anonymous set download myminio/stock-data-raw
```

#### Initialize HDFS Directories

```bash
# Wait for HDFS to be ready
sleep 60

# Create directories
docker exec namenode hdfs dfs -mkdir -p /stock-data/raw/prices
docker exec namenode hdfs dfs -mkdir -p /stock-data/raw/volumes
docker exec namenode hdfs dfs -mkdir -p /stock-data/lakehouse/prices
docker exec namenode hdfs dfs -mkdir -p /stock-data/lakehouse/volumes

# Set permissions
docker exec namenode hdfs dfs -chmod -R 755 /stock-data
```

#### Initialize Airflow

```bash
# Wait for Airflow to be ready
sleep 90

# Initialize Airflow database (if needed)
docker exec airflow-webserver airflow db init

# Create admin user (if needed)
docker exec airflow-webserver airflow users create \
  --username airflow \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password airflow

# Copy DAGs to Airflow
docker cp src/dags/. airflow-webserver:/opt/airflow/dags/
docker cp src/dags/. airflow-scheduler:/opt/airflow/dags/
```

#### Initialize Iceberg Tables

```bash
# Wait for services
sleep 120

# Ensure virtual environment is activated
# Create Iceberg tables
python src/lakehouse/iceberg_tables.py --table all
```

### Step 5: Verify Setup

```bash
# Run health checks
./scripts/health_check.sh

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check MinIO buckets
docker exec minio mc ls myminio/

# Check HDFS
docker exec namenode hdfs dfs -ls -R /stock-data

# Check Airflow
curl http://localhost:8080/health

# Check Trino
curl http://localhost:8081/v1/info
```

---

## Verification

### Test Data Ingestion

```bash
# Ensure virtual environment is activated
# Terminal 1: Start producer
python src/ingestion/kafka_producer.py

# Terminal 2: Verify messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --from-beginning \
  --max-messages 5
```

### Test Stream Processing

```bash
# Ensure virtual environment is activated
# Start streaming
python src/streaming/streaming_app.py

# Wait 1-2 minutes, then check MinIO
docker exec minio mc ls myminio/stock-data-raw/prices/raw/
```

### Test Batch Processing

```bash
# Ensure virtual environment is activated
# Run batch job
python src/batch/daily_aggregation.py --date $(date +%Y-%m-%d)
```

### Test Queries

```bash
# Access Trino Web UI: http://localhost:8081
# Run a test query:
SELECT COUNT(*) FROM iceberg.stock_analytics.stock_prices;
```

### Test Monitoring

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Airflow**: http://localhost:8080 (airflow/airflow)

---

## Quick Setup Script

Create a setup script `setup.sh`:

```bash
#!/bin/bash
set -e

echo "=== Real-Time Stock Analytics Platform Setup ==="

# 1. Create directories
echo "Creating directory structure..."
mkdir -p src/{ingestion,streaming,batch,dags,lakehouse,queries,utils,monitoring}
mkdir -p config/{trino/catalog,prometheus,grafana/{provisioning/{datasources,dashboards},dashboards}}
mkdir -p {docs,scripts,logs,plugins,checkpoints,.github/workflows}

# 2. Create and activate virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# 5. Wait for services
echo "Waiting for services to initialize..."
sleep 180

# 6. Initialize services
echo "Initializing services..."
./scripts/health_check.sh

# 7. Setup MinIO
echo "Setting up MinIO buckets..."
docker exec minio mc mb myminio/stock-data-raw || true
docker exec minio mc mb myminio/stock-data-lakehouse || true

# 8. Setup HDFS
echo "Setting up HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /stock-data/raw/prices || true
docker exec namenode hdfs dfs -mkdir -p /stock-data/raw/volumes || true

echo "=== Setup Complete ==="
echo "Access points:"
echo "  - Grafana: http://localhost:3000"
echo "  - Airflow: http://localhost:8080"
echo "  - Trino: http://localhost:8081"
echo "  - Prometheus: http://localhost:9090"
```

Make it executable:
```bash
chmod +x setup.sh
```

---

## Troubleshooting Setup

### Common Issues

1. **Port conflicts**
   ```bash
   # Check what's using ports
   lsof -i :9092  # Kafka
   lsof -i :9000  # MinIO
   lsof -i :8080  # Airflow/Trino
   ```

2. **Docker resources**
   ```bash
   # Increase Docker resources
   # Docker Desktop → Settings → Resources
   # Minimum: 8GB RAM, 4 CPUs
   ```

3. **Permission issues**
   ```bash
   # Fix script permissions
   chmod +x scripts/*.sh
   
   # Fix Docker permissions (Linux)
   sudo usermod -aG docker $USER
   ```

4. **Service startup failures**
   ```bash
   # Check logs
   docker-compose logs [service-name]
   
   # Restart service
   docker-compose restart [service-name]
   ```

---

## Next Steps

After setup:

1. **Read Testing Guide**: See `docs/TESTING_AND_USAGE.md`
2. **Start Data Flow**: Run producer and streaming
3. **Explore Dashboards**: Check Grafana visualizations
4. **Run Queries**: Use Trino for analytics
5. **Review Documentation**: Read other docs in `docs/`

---

## Summary Checklist

- [ ] Prerequisites installed (Docker, Python, Git)
- [ ] Project structure created
- [ ] Configuration files created
- [ ] Docker Compose file created
- [ ] Python source code files created
- [ ] Scripts created and made executable
- [ ] Documentation files created
- [ ] CI/CD workflow created
- [ ] Virtual environment created and activated
- [ ] Dependencies installed in virtual environment
- [ ] Docker services started
- [ ] Services initialized (MinIO, HDFS, Airflow)
- [ ] Health checks passed
- [ ] Data ingestion tested
- [ ] Stream processing tested
- [ ] Monitoring verified

---

## Support

If you encounter issues:

1. Check service logs: `docker-compose logs [service]`
2. Run health checks: `./scripts/health_check.sh`
3. Review troubleshooting section above
4. Check documentation: `docs/` directory
5. Review runbooks: `docs/RUNBOOKS.md`

---

**Last Updated**: 2025-11-30

---

## Quick Reference: File Checklist

Use this checklist to ensure all files are created:

### Configuration Files (config/)
- [ ] `kafka_config.yaml`
- [ ] `spark_config.yaml`
- [ ] `hdfs_config.yaml`
- [ ] `airflow_config.yaml`
- [ ] `iceberg_config.yaml`
- [ ] `trino/config.properties`
- [ ] `trino/jvm.config`
- [ ] `trino/node.properties`
- [ ] `trino/catalog/iceberg.properties`
- [ ] `trino/catalog/hive.properties`
- [ ] `trino/catalog/tpch.properties`
- [ ] `prometheus/prometheus.yml`
- [ ] `prometheus/alerts.yml`
- [ ] `grafana/provisioning/datasources/prometheus.yml`
- [ ] `grafana/provisioning/dashboards/dashboards.yml`
- [ ] `grafana/dashboards/infrastructure-health.json`
- [ ] `grafana/dashboards/data-pipeline-metrics.json`
- [ ] `grafana/dashboards/stock-analytics.json`

### Root Files
- [ ] `.gitignore`
- [ ] `requirements.txt`
- [ ] `README.md`
- [ ] `docker-compose.yml`
- [ ] `PLAN.md` (optional, for reference)

### Python Source Files (src/)
- [ ] `ingestion/kafka_producer.py`
- [ ] `ingestion/kafka_consumer.py`
- [ ] `streaming/streaming_app.py`
- [ ] `streaming/transformations.py`
- [ ] `streaming/run_streaming.py`
- [ ] `batch/daily_aggregation.py`
- [ ] `batch/data_quality_check.py`
- [ ] `dags/stock_analytics_dag.py`
- [ ] `dags/hourly_data_quality_dag.py`
- [ ] `lakehouse/iceberg_tables.py`
- [ ] `lakehouse/data_migration.py`
- [ ] `lakehouse/table_optimization.py`
- [ ] `lakehouse/time_travel_queries.py`
- [ ] `queries/stock_analytics_queries.sql`
- [ ] `utils/spark_session.py`
- [ ] `utils/hdfs_client.py`
- [ ] `utils/hdfs_health_check.py`
- [ ] All `__init__.py` files

### Scripts (scripts/)
- [ ] `deploy.sh`
- [ ] `health_check.sh`
- [ ] `backup.sh`

### Documentation (docs/)
- [ ] `SETUP_GUIDE.md` (this file)
- [ ] `TESTING_AND_USAGE.md`
- [ ] `PRODUCTION_DEPLOYMENT.md`
- [ ] `SECURITY.md`
- [ ] `PERFORMANCE_TUNING.md`
- [ ] `RUNBOOKS.md`
- [ ] `API.md`

### CI/CD
- [ ] `.github/workflows/ci.yml`

---

## Alternative: Automated Setup

If you have access to the existing project repository, you can use Git to clone and set up:

```bash
# Clone the repository
git clone <repository-url>
cd real-time-stock-analytics

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Start services
docker-compose up -d

# Wait and initialize
sleep 180
./scripts/health_check.sh
```

This is the fastest way to get started if you have repository access.
