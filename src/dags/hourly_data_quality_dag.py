"""
Hourly Data Quality Check DAG

Runs data quality checks every hour on recent data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments for DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'hourly_data_quality_check',
    default_args=default_args,
    description='Hourly data quality checks for stock data',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['stock-analytics', 'data-quality', 'hourly'],
)

# Calculate date (yesterday for hourly checks on recent data)
hourly_data_quality = BashOperator(
    task_id='hourly_data_quality_check',
    bash_command='python /opt/airflow/batch/data_quality_check.py --date {{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}',
    dag=dag,
)

hourly_data_quality
