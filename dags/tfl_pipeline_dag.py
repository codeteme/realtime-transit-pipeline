from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import boto3
import os

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define your AWS bucket (✅ edit to match your own)
S3_BUCKET = "temesgen-tfl-pipeline"
LOCAL_TRANSFORM_PATH = "/tmp/tfl_data/transformed.json"
S3_OBJECT_KEY = "transformed/latest_transformed.json"

# ------------------------------------------------------------
# DEFINE YOUR PYTHON FUNCTIONS
# ------------------------------------------------------------

def run_ingest():
    """Call the minimal ingestion script."""
    subprocess.run(["python", "/opt/airflow/scripts/tfl_ingest_minimal.py"], check=True)

def run_transform():
    """Run the PySpark transformation script."""
    subprocess.run(["spark-submit", "/opt/airflow/scripts/tfl_transform_minimal.py"], check=True)

def run_analysis():
    """Run the simple aggregation/analysis step."""
    subprocess.run(["spark-submit", "/opt/airflow/scripts/tfl_analysis_minimal.py"], check=True)


def upload_to_s3():
    """Upload the transformed file to S3."""
    if not os.path.exists(LOCAL_TRANSFORM_PATH):
        raise FileNotFoundError(f"{LOCAL_TRANSFORM_PATH} not found")

    s3 = boto3.client("s3")
    s3.upload_file(LOCAL_TRANSFORM_PATH, S3_BUCKET, S3_OBJECT_KEY)
    print(f"✅ Uploaded to s3://{S3_BUCKET}/{S3_OBJECT_KEY}")


# ------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------
with DAG(
    dag_id="tfl_pipeline_dag",
    description="TfL Bus Arrivals Pipeline (ingest -> transform -> analyze -> upload to S3)",
    start_date=datetime(2025, 11, 10),
    schedule_interval="*/2 * * * *",  # every 2 minutes
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["tfl", "bus", "pyspark", "aws", "data-engineering"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_bus_data",
        python_callable=run_ingest,
    )

    transform = PythonOperator(
        task_id="transform_bus_data",
        python_callable=run_transform,
    )

    analyze = PythonOperator(
        task_id="analyze_bus_data",
        python_callable=run_analysis,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    # Define pipeline order
    ingest >> transform >> analyze >> upload