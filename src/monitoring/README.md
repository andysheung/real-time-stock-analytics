# Monitoring & Visualization

This directory contains monitoring and visualization configuration for the Real-Time Stock Analytics Platform.

## Components

### Prometheus
- **URL**: http://localhost:9090
- **Purpose**: Metrics collection and storage
- **Configuration**: `config/prometheus/prometheus.yml`

### Grafana
- **URL**: http://localhost:3000
- **Username**: `admin`
- **Password**: `admin`
- **Purpose**: Visualization and dashboards

## Dashboards

### Infrastructure Health Dashboard
- Service status (Kafka, MinIO, HDFS, Trino)
- Kafka topics information
- MinIO storage usage
- HDFS DataNode status

### Data Pipeline Metrics Dashboard
- Kafka message rate
- Consumer lag
- Data ingestion throughput
- Error rates
- Spark streaming processing rate

### Stock Analytics Dashboard
- Stock prices over time
- Trading volume
- Price changes
- Top symbols by volume
- Market summary

## Accessing Dashboards

1. Start all services:
   ```bash
   docker-compose up -d
   ```

2. Access Grafana:
   - URL: http://localhost:3000
   - Login with admin/admin

3. Navigate to Dashboards:
   - Dashboards are automatically provisioned
   - Find them in the Dashboards menu

## Alerting

Prometheus alerting rules are configured in `config/prometheus/alerts.yml`:

### Infrastructure Alerts
- Kafka broker down
- MinIO down
- HDFS NameNode down
- Airflow scheduler down
- Trino coordinator down

### Data Pipeline Alerts
- High Kafka consumer lag
- Low data ingestion rate
- High error rate

### Storage Alerts
- High MinIO disk usage
- Low HDFS DataNodes

## Adding Custom Metrics

To add custom metrics:

1. Export metrics from your application using Prometheus client libraries
2. Add scrape configuration to `config/prometheus/prometheus.yml`
3. Create Grafana panels to visualize the metrics

## Example: Custom Stock Metrics

```python
from prometheus_client import Counter, Gauge, Histogram

# Define metrics
stock_price_gauge = Gauge('stock_price', 'Current stock price', ['symbol'])
stock_volume_counter = Counter('stock_volume_total', 'Total stock volume', ['symbol'])

# Update metrics
stock_price_gauge.labels(symbol='AAPL').set(150.25)
stock_volume_counter.labels(symbol='AAPL').inc(1000)
```

## Troubleshooting

### Prometheus Not Scraping
- Check service endpoints are accessible
- Verify scrape configuration in `prometheus.yml`
- Check Prometheus logs: `docker logs prometheus`

### Grafana Not Showing Data
- Verify Prometheus datasource is configured
- Check dashboard queries match available metrics
- Verify time range is correct

### Missing Metrics
- Ensure services expose metrics endpoints
- Check Prometheus targets page: http://localhost:9090/targets
- Verify scrape intervals are appropriate
