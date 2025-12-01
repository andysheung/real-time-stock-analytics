"""
Stock Analytics Batch Processing DAG

Orchestrates daily aggregation and data quality checks for stock data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'stock_analytics_batch_processing',
    default_args=default_args,
    description='Daily batch processing for stock analytics',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=days_ago(1),
    catchup=False,
    tags=['stock-analytics', 'batch-processing'],
)

# Task 1: Data Quality Check
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/batch/data_quality_check.py --date {{ ds }}',
    dag=dag,
)

# Task 2: Daily Price Aggregation
daily_price_aggregation = BashOperator(
    task_id='daily_price_aggregation',
    bash_command='python /opt/airflow/batch/daily_aggregation.py --date {{ ds }}',
    dag=dag,
)

# Task 3: Data Quality Report (runs after aggregation)
data_quality_report = BashOperator(
    task_id='data_quality_report',
    bash_command='echo "Data quality check completed for {{ ds }}"',
    dag=dag,
)

# Define task dependencies
data_quality_check >> daily_price_aggregation >> data_quality_report
