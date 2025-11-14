from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def wait_for_kafka():
    """
    Wait for Kafka messages to be consumed
    """
    print("⏳ Waiting 30 seconds for Kafka consumption...")
    time.sleep(30)

# Create the DAG
dag = DAG(
    'tfl_realtime_pipeline',
    default_args=default_args,
    description='TfL Real-time Data Pipeline: Ingest → Kafka → Transform → S3',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    catchup=False,
    tags=['tfl', 'kafka', 'spark', 's3'],
)

# Task 1: Ingest data from TfL API and send to Kafka
ingest_task = BashOperator(
    task_id='ingest_tfl_data',
    bash_command='python /opt/airflow/scripts/tfl_ingest_minimal.py',
    dag=dag,
)

# Task 2: Consume from Kafka and write to bronze layer
consume_task = BashOperator(
    task_id='consume_kafka_to_bronze',
    bash_command='python /opt/airflow/scripts/tfl_consumer.py',
    dag=dag,
)

# Task 3: Wait for consumption to complete
wait_task = PythonOperator(
    task_id='wait_for_consumption',
    python_callable=wait_for_kafka,
    dag=dag,
)

# Task 4: Transform data using PySpark (bronze → gold)
transform_task = BashOperator(
    task_id='transform_with_spark',
    bash_command='python /opt/airflow/scripts/tfl_transform_minimal.py',
    dag=dag,
)

# Task 5: Run analysis
analyze_task = BashOperator(
    task_id='analyze_data',
    bash_command='python /opt/airflow/scripts/tfl_analysis_minimal.py',
    dag=dag,
)

# Define task dependencies
ingest_task >> consume_task >> wait_task >> transform_task >> analyze_task