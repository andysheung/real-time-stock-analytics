# Airflow DAGs

This directory contains Airflow DAGs for orchestrating batch processing workflows.

## DAGs

### `stock_analytics_dag.py`
**Schedule**: Daily at 2 AM  
**Description**: Main batch processing workflow for stock analytics

**Tasks**:
1. `data_quality_check` - Validates data quality for raw data
2. `daily_price_aggregation` - Aggregates daily price and volume data
3. `data_quality_report` - Generates data quality report

**Dependencies**: 
```
data_quality_check → daily_price_aggregation → data_quality_report
```

### `hourly_data_quality_dag.py`
**Schedule**: Every hour  
**Description**: Hourly data quality checks on recent data

**Tasks**:
1. `hourly_data_quality_check` - Runs data quality validation

## Usage

### Accessing Airflow UI

1. Start all services:
   ```bash
   docker-compose up -d
   ```

2. Access Airflow Web UI:
   - URL: http://localhost:8080
   - Default credentials: `airflow` / `airflow`

### Running DAGs Manually

1. Navigate to Airflow UI
2. Find the DAG you want to run
3. Click "Trigger DAG" button
4. Monitor execution in the UI

### Viewing Logs

Logs are available in:
- Airflow UI: Click on a task → View Log
- Local filesystem: `./logs/` directory

## Configuration

DAGs use configuration from:
- `config/airflow_config.yaml` - Airflow settings
- `config/spark_config.yaml` - Spark job settings

## Task Retries

- Daily DAG: 2 retries with 5-minute delay
- Hourly DAG: 1 retry with 5-minute delay

## Monitoring

Monitor DAG execution:
- Airflow UI: http://localhost:8080
- Check task status, logs, and execution history
- Set up alerts for failed tasks (configured in Airflow UI)
