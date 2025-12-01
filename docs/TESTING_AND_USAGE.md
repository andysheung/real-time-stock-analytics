# Testing and Usage Guide

This guide provides step-by-step instructions for testing and using the Real-Time Stock Analytics Platform.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Testing Individual Components](#testing-individual-components)
3. [End-to-End Testing](#end-to-end-testing)
4. [Using the Platform](#using-the-platform)
5. [Example Workflows](#example-workflows)
6. [Troubleshooting](#troubleshooting)

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ installed
- At least 8GB RAM available
- Internet connection for data fetching

### Initial Setup

1. **Clone and navigate to the project:**
   ```bash
   cd real-time-stock-analytics
   ```

2. **Create and activate virtual environment:**
   ```bash
   # Create virtual environment
   python3 -m venv venv
   
   # Activate virtual environment
   # On macOS/Linux:
   source venv/bin/activate
   # On Windows:
   venv\Scripts\activate
   
   # Verify activation (you should see (venv) in your prompt)
   ```

3. **Install Python dependencies:**
   ```bash
   # Ensure virtual environment is activated
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
   
   **Note:** Always ensure your virtual environment is activated before running any Python scripts.

4. **Start all infrastructure services:**
   ```bash
   docker-compose up -d
   ```

5. **Wait for services to be healthy (2-3 minutes):**
   ```bash
   docker-compose ps
   # Wait until all services show "healthy" status
   ```

6. **Verify services are running:**
   ```bash
   # Check Kafka
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   
   # Check MinIO (open in browser)
   # http://localhost:9001 (login: minioadmin/minioadmin)
   
   # Check HDFS
   docker exec namenode hdfs dfsadmin -report
   # HDFS NameNode Web UI: http://localhost:9870
   # Note: HDFS RPC port is 9002 on host (to avoid conflict with MinIO on 9000)
   
   # Check Airflow (open in browser)
   # http://localhost:8080 (login: admin/admin or airflow/airflow)
   
   # Check Trino (open in browser)
   # http://localhost:8081
   
   # Check Prometheus
   # http://localhost:9090
   
   # Check Grafana
   # http://localhost:3000 (login: admin/admin)
   ```

## Testing Individual Components

> **⚠️ Important:** Before running any Python scripts, ensure your virtual environment is activated:
> ```bash
> source venv/bin/activate  # On macOS/Linux
> # venv\Scripts\activate  # On Windows
> ```

### 1. Test Data Ingestion (Kafka Producer)

**Start the producer:**
```bash
# Ensure virtual environment is activated
python3 src/ingestion/kafka_producer.py
```

**Expected output:**
- Messages being published every 5 seconds
- Log messages showing successful publishes
- No errors

**Verify data in Kafka:**
```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages from stock-prices topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --from-beginning \
  --max-messages 5
```

**Test with different symbols:**
```python
from src.ingestion.kafka_producer import StockDataProducer

producer = StockDataProducer(
    bootstrap_servers='localhost:9092',
    stock_symbols=['TSLA', 'NVDA'],  # Different symbols
    fetch_interval=10  # 10 seconds
)
producer.run()
```

### 2. Test Kafka Consumer

**In a new terminal, start the consumer:**
```bash
python3 src/ingestion/kafka_consumer.py
```

**Expected output:**
- Messages being consumed and displayed
- JSON data parsed correctly
- No errors

**Verify consumer group:**
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group stock-analytics-group \
  --describe
```

### 3. Test Spark Structured Streaming

**Start the streaming application:**
```bash
python3 src/streaming/streaming_app.py
```

**Or use the convenience script:**
```bash
# Run all streams
python3 src/streaming/run_streaming.py --mode all

# Run only price stream
python3 src/streaming/run_streaming.py --mode prices

# Run only volume stream
python3 src/streaming/run_streaming.py --mode volumes

# Run only aggregated streams
python3 src/streaming/run_streaming.py --mode aggregated
```

**Expected behavior:**
- Spark session initializes
- Kafka connection established
- Data being processed and written to MinIO
- Checkpoint directory created
- No errors in logs

**Verify data in MinIO:**
1. Open MinIO Console: http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Navigate to `stock-data-raw` bucket
4. Check for `prices/raw/` and `volumes/raw/` directories
5. Verify parquet files are being created

**Verify checkpoint directory:**
```bash
ls -la checkpoints/
# Should see directories: prices/, volumes/, prices_aggregated/, volumes_aggregated/
```

### 4. Test HDFS

**Check HDFS health:**
```bash
python3 src/utils/hdfs_health_check.py
```

**Expected output:**
- HDFS cluster healthy
- All directories exist
- Directories accessible

**Use HDFS client:**
```python
from src.utils.hdfs_client import HDFSClient

client = HDFSClient()
client.setup_directories()
files = client.ls("/stock-data")
print(files)
```

**Write to HDFS from Spark Streaming:**
```python
from src.streaming.streaming_app import StockStreamingApp

app = StockStreamingApp()
# Enable HDFS writes
app.run_price_stream(write_to_hdfs=True)
```

**Verify data in HDFS:**
```bash
docker exec namenode hdfs dfs -ls -R /stock-data
# Note: To access HDFS from host, use port 9002 (not 9000) to avoid conflict with MinIO
# Example: hdfs://localhost:9002/stock-data/raw/prices
```

### 5. Test Batch Processing Jobs

**Test daily aggregation job:**
```bash
# Process yesterday's data
# On Linux:
python3 src/batch/daily_aggregation.py --date $(date -d "yesterday" +%Y-%m-%d)
# On macOS:
python3 src/batch/daily_aggregation.py --date $(date -v-1d +%Y-%m-%d)

# Process specific date
python3 src/batch/daily_aggregation.py --date 2025-11-30
```

**Expected output:**
- Data read from MinIO/HDFS
- Aggregations calculated
- Data written to lakehouse storage
- Success message

**Verify aggregated data:**
```bash
# Check MinIO
# Navigate to stock-data-lakehouse bucket → prices/daily/

# Or use Python
from pyspark.sql import SparkSession
from src.utils.spark_session import SparkSessionFactory

spark = SparkSessionFactory.create_session()
df = spark.read.parquet("s3a://stock-data-lakehouse/prices/daily/")
df.show()
```

**Test data quality check:**
```bash
# On Linux:
python3 src/batch/data_quality_check.py --date $(date -d "yesterday" +%Y-%m-%d)
# On macOS:
python3 src/batch/data_quality_check.py --date $(date -v-1d +%Y-%m-%d)
```

**Expected output:**
- Quality checks performed
- Results displayed
- Pass/fail status

### 6. Test Airflow DAGs

**Access Airflow UI:**
1. Open http://localhost:8080
2. Login: `admin` / `admin` (or `airflow` / `airflow` if default user exists)
3. Find DAGs: `stock_analytics_batch_processing` and `hourly_data_quality_check`

**Manually trigger a DAG:**
1. Click on DAG name
2. Click "Trigger DAG" button
3. Monitor execution in Graph View
4. Check task logs

**Verify DAG execution:**
```bash
# Check Airflow logs
docker logs airflow-scheduler

# Check task status via API
curl -u airflow:airflow \
  http://localhost:8080/api/v1/dags/stock_analytics_batch_processing/dagRuns
```

### 7. Test Apache Iceberg

**Create Iceberg tables:**
```bash
python3 src/lakehouse/iceberg_tables.py --table all
```

**Expected output:**
- Tables created successfully
- No errors

**Migrate data to Iceberg:**
```bash
# Migrate all data types
python3 src/lakehouse/data_migration.py --type all --start-date $(date -d "yesterday" +%Y-%m-%d)

# Migrate specific type
python3 src/lakehouse/data_migration.py --type prices --start-date 2025-11-30
```

**Query Iceberg tables:**
```python
from pyspark.sql import SparkSession
from src.lakehouse.iceberg_tables import IcebergTableManager

manager = IcebergTableManager()
spark = manager.spark

# Query data (after USE stock_analytics.stock_analytics; or use full path)
df = spark.sql("SELECT * FROM stock_analytics.stock_analytics.stock_prices LIMIT 10")
# Or if using Iceberg catalog in Trino:
# df = spark.sql("SELECT * FROM iceberg.stock_analytics.stock_prices LIMIT 10")
df.show()
```

**Test time travel:**
```bash
# List snapshots
# Note: Table name format depends on catalog type
# For Spark/Iceberg: stock_analytics.stock_analytics.stock_prices
# For Trino: iceberg.stock_analytics.stock_prices
python3 src/lakehouse/time_travel_queries.py \
  --table stock_analytics.stock_analytics.stock_prices \
  --operation list-snapshots
```

**Optimize tables:**
```bash
# Optimize all tables
python3 src/lakehouse/table_optimization.py

# Optimize specific table
python3 src/lakehouse/table_optimization.py \
  --table stock_analytics.stock_analytics.stock_prices \
  --operation compact
```

### 8. Test Trino Queries

**Access Trino Web UI:**
1. Open http://localhost:8081
2. Login: Any username (e.g., `admin` or `user`) - no password required by default

**Run a query:**
```sql
-- Use Iceberg catalog
USE iceberg.stock_analytics;

-- Query latest prices
-- Note: After USE statement, you can reference tables directly
SELECT 
    symbol,
    price,
    timestamp
FROM stock_prices
WHERE symbol = 'AAPL'
    AND date = CURRENT_DATE
ORDER BY timestamp DESC
LIMIT 10;

-- Or use full catalog path:
-- SELECT * FROM iceberg.stock_analytics.stock_prices LIMIT 10;
```

**Run example queries:**
```bash
# Copy queries from stock_analytics_queries.sql
# Paste into Trino Web UI and execute
```

**Query via CLI (if Trino CLI installed):**
```bash
trino --server http://localhost:8081 --user admin \
  --execute "SELECT COUNT(*) FROM iceberg.stock_analytics.stock_prices"
```

**Query via Python:**
```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8081,
    user='admin',
    catalog='iceberg',
    schema='stock_analytics'
)

cur = conn.cursor()
cur.execute("SELECT * FROM stock_prices LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(row)
```

### 9. Test Monitoring

**Access Prometheus:**
1. Open http://localhost:9090
2. Check targets: Status → Targets
3. Verify all targets are "UP"

**Query metrics:**
```promql
# Service uptime
up{job="kafka"}

# Kafka messages rate
rate(kafka_messages_in_total[5m])

# Check all services
up
```

**Access Grafana:**
1. Open http://localhost:3000
2. Login: `admin` / `admin`
3. Navigate to Dashboards
4. View:
   - Infrastructure Health
   - Data Pipeline Metrics
   - Stock Analytics

**Verify dashboards:**
- All panels should load
- Data should be visible (if services are running)
- No errors in dashboard

## End-to-End Testing

### Complete Data Flow Test

**Step 1: Start Infrastructure**
```bash
docker-compose up -d
# Wait 2-3 minutes for all services
```

**Step 2: Start Data Ingestion**
```bash
# Terminal 1: Start Kafka producer
python3 src/ingestion/kafka_producer.py
```

**Step 3: Start Stream Processing**
```bash
# Terminal 2: Start Spark streaming
python3 src/streaming/streaming_app.py
```

**Step 4: Verify Data Flow**
```bash
# Check Kafka messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock-prices \
  --max-messages 5

# Check MinIO data (via web UI or CLI)
docker exec minio mc ls myminio/stock-data-raw/prices/raw/

# Check HDFS data
docker exec namenode hdfs dfs -ls /stock-data/raw/prices/
```

**Step 5: Run Batch Processing**
```bash
# Wait for some data to accumulate (5-10 minutes)
# Then run batch job
python3 src/batch/daily_aggregation.py --date $(date +%Y-%m-%d)
# Or process yesterday's data:
# Linux: python3 src/batch/daily_aggregation.py --date $(date -d "yesterday" +%Y-%m-%d)
# macOS: python3 src/batch/daily_aggregation.py --date $(date -v-1d +%Y-%m-%d)
```

**Step 6: Query Data**
```sql
-- In Trino Web UI
USE iceberg.stock_analytics;

SELECT 
    symbol,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price
FROM stock_prices
WHERE date = CURRENT_DATE
GROUP BY symbol;
```

**Step 7: Verify Monitoring**
- Check Grafana dashboards show data
- Verify Prometheus metrics
- Check alerts are configured

### Data Quality Test

**Run data quality checks:**
```bash
# Check today's data
python3 src/batch/data_quality_check.py --date $(date +%Y-%m-%d)
# Or check yesterday's data:
# Linux: python3 src/batch/data_quality_check.py --date $(date -d "yesterday" +%Y-%m-%d)
# macOS: python3 src/batch/data_quality_check.py --date $(date -v-1d +%Y-%m-%d)
```

**Expected results:**
- Record count > 0
- Null values < 5%
- No negative prices
- Duplicates < 10%

**Verify in Airflow:**
- Trigger `hourly_data_quality_check` DAG
- Check task logs
- Verify quality reports

## Using the Platform

### Daily Operations

**1. Start the Platform:**
```bash
# Start all services
docker-compose up -d

# Start data ingestion
python3 src/ingestion/kafka_producer.py &

# Start streaming
python3 src/streaming/streaming_app.py &
```

**2. Monitor the Platform:**
- Check Grafana dashboards: http://localhost:3000
- Check Prometheus alerts: http://localhost:9090/alerts
- Check Airflow DAGs: http://localhost:8080

**3. Run Batch Jobs:**
- Daily aggregation runs automatically at 2 AM (via Airflow)
- Or run manually: `python3 src/batch/daily_aggregation.py --date YYYY-MM-DD`

**4. Query Data:**
- Use Trino Web UI: http://localhost:8081
- Run analytical queries
- Export results if needed

### Example Use Cases

#### Use Case 1: Real-Time Price Monitoring

```python
# Start producer
python3 src/ingestion/kafka_producer.py

# In another terminal, monitor prices
from src.ingestion.kafka_consumer import StockDataConsumer

consumer = StockDataConsumer(
    bootstrap_servers='localhost:9092',
    topics=['stock-prices']
)

# Filter for specific symbol
for message in consumer.consume():
    data = message.value
    if data['symbol'] == 'AAPL':
        print(f"AAPL: ${data['price']:.2f} ({data.get('price_change_pct', 0):.2f}%)")
```

#### Use Case 2: Daily Price Analysis

```sql
-- In Trino
USE iceberg.stock_analytics;

-- Get daily summary
SELECT 
    symbol,
    date,
    avg_price,
    max_price,
    min_price,
    total_volume,
    price_range_pct
FROM daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY date DESC, symbol;
```

#### Use Case 3: Volume Analysis

```sql
-- Top symbols by volume
SELECT 
    symbol,
    SUM(total_volume) as total_volume_7d
FROM daily_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY symbol
ORDER BY total_volume_7d DESC
LIMIT 10;
```

#### Use Case 4: Price Trend Analysis

```sql
-- Moving average
SELECT 
    date,
    symbol,
    avg_price,
    AVG(avg_price) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM daily_aggregates
WHERE symbol = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY date;
```

#### Use Case 5: Historical Data Query (Time Travel)

```python
from src.lakehouse.time_travel_queries import TimeTravelQueries

queries = TimeTravelQueries()

# List snapshots
snapshots = queries.list_snapshots("stock_analytics.stock_analytics.stock_prices")

# Query at specific timestamp
df = queries.query_at_timestamp(
    "stock_analytics.stock_analytics.stock_prices",
    "2025-11-30 10:00:00"
)
```

### Integration Testing

**Test Complete Pipeline:**

```bash
#!/bin/bash
# Complete integration test script

echo "Starting integration test..."

# 1. Start services
docker-compose up -d
sleep 60

# 2. Verify services
./scripts/health_check.sh

# 3. Start producer (background)
python3 src/ingestion/kafka_producer.py &
PRODUCER_PID=$!

# 4. Wait for data
sleep 30

# 5. Start streaming
python3 src/streaming/streaming_app.py &
STREAMING_PID=$!

# 6. Wait for processing
sleep 60

# 7. Verify data in storage
echo "Checking MinIO..."
docker exec minio mc ls myminio/stock-data-raw/prices/raw/

echo "Checking HDFS..."
docker exec namenode hdfs dfs -ls /stock-data/raw/prices/

# 8. Run batch job
python3 src/batch/daily_aggregation.py --date $(date +%Y-%m-%d)

# 9. Query data
echo "Testing Trino query..."
docker exec trino-coordinator trino \
  --server http://localhost:8081 \
  --user admin \
  --execute "USE iceberg.stock_analytics; SELECT COUNT(*) FROM stock_prices"

# 10. Cleanup
kill $PRODUCER_PID
kill $STREAMING_PID

echo "Integration test completed!"
```

## Example Workflows

### Workflow 1: Daily Stock Analysis

1. **Morning**: Check overnight batch job results
   ```bash
   # Check Airflow DAG status
   # View Grafana dashboard for overnight metrics
   ```

2. **During Day**: Monitor real-time data
   - View Grafana Stock Analytics dashboard
   - Check for alerts in Prometheus
   - Monitor Kafka consumer lag

3. **End of Day**: Run analysis queries
   ```sql
   -- Daily summary
   SELECT * FROM daily_aggregates WHERE date = CURRENT_DATE;
   ```

### Workflow 2: Data Quality Monitoring

1. **Hourly**: Automatic quality checks run (via Airflow)
2. **Review**: Check quality reports in Airflow logs
3. **Action**: If issues detected, investigate and fix

### Workflow 3: Historical Analysis

1. **Query Historical Data**: Use Trino time travel
2. **Compare Periods**: Use SQL to compare different time periods
3. **Export Results**: Download query results for reporting

## Troubleshooting

### Common Issues

**Issue**: Services not starting
```bash
# Check Docker logs
docker-compose logs

# Check specific service
docker logs kafka
docker logs minio

# Restart services
docker-compose restart
```

**Issue**: No data in Kafka
```bash
# Check producer is running
ps aux | grep kafka_producer

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check producer logs
# Look for errors in producer output
```

**Issue**: Spark streaming not processing
```bash
# Check checkpoint directory
ls -la checkpoints/

# Check Spark logs
# Look for errors in streaming output

# Restart streaming (will resume from checkpoint)
python3 src/streaming/streaming_app.py
```

**Issue**: Trino queries failing
```bash
# Check Trino logs
docker logs trino-coordinator

# Verify Iceberg tables exist
docker exec trino-coordinator trino \
  --server http://localhost:8081 \
  --execute "SHOW TABLES FROM iceberg.stock_analytics"

# Check MinIO connectivity
docker exec trino-coordinator ping minio
```

**Issue**: Airflow DAGs not running
```bash
# Check DAG is unpaused
# In Airflow UI, verify DAG is not paused

# Check scheduler logs
docker logs airflow-scheduler

# Manually trigger DAG
# Use Airflow UI or API
```

### Health Checks

**Run comprehensive health check:**
```bash
./scripts/health_check.sh
```

**Check individual services:**
```bash
# Kafka
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# MinIO
curl http://localhost:9000/minio/health/live

# HDFS NameNode Web UI
curl http://localhost:9870
# Note: HDFS RPC port is 9002 on host (MinIO uses 9000)

# Airflow
curl http://localhost:8080/health

# Trino
curl http://localhost:8081/v1/info

# Prometheus
curl http://localhost:9090/-/healthy

# Grafana
curl http://localhost:3000/api/health
```

## Performance Testing

### Load Test Kafka Producer

```python
# Create test script: test_load.py
from src.ingestion.kafka_producer import StockDataProducer
import time

producer = StockDataProducer(
    bootstrap_servers='localhost:9092',
    stock_symbols=['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA'],
    fetch_interval=1  # 1 second for load testing
)

# Run for 5 minutes
start_time = time.time()
while time.time() - start_time < 300:  # 5 minutes
    producer.run()
```

### Test Query Performance

```sql
-- In Trino, test query performance
EXPLAIN ANALYZE
SELECT 
    symbol,
    AVG(price) as avg_price
FROM stock_prices
WHERE date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY symbol;
```

### Monitor Resource Usage

```bash
# Check Docker resource usage
docker stats

# Check disk usage
df -h

# Check memory
free -h
```

## Best Practices

1. **Start Small**: Begin with one symbol, then scale up
2. **Monitor First**: Always check Grafana before scaling
3. **Test Incrementally**: Test each component before testing end-to-end
4. **Use Health Checks**: Run health checks regularly
5. **Check Logs**: Monitor logs for errors and warnings
6. **Backup Data**: Run backups before major changes
7. **Document Issues**: Keep track of issues and solutions

## Next Steps

After testing:
1. Review performance metrics
2. Optimize based on findings
3. Scale up gradually
4. Set up production monitoring
5. Configure alerts
6. Document your specific use cases

## Support

- **Documentation**: See `docs/` directory
- **Runbooks**: See `docs/RUNBOOKS.md`
- **Troubleshooting**: See Troubleshooting section above
- **Health Checks**: Use `./scripts/health_check.sh`
