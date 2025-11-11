from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

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

# ------------------------------------------------------------
# DEFINE YOUR PYTHON FUNCTIONS
# ------------------------------------------------------------

def run_ingest():
    """Call the minimal ingestion script."""
    subprocess.run(["python", "tfl_ingest_minimal.py"], check=True)

def run_transform():
    """Run the PySpark transformation script."""
    subprocess.run(["spark-submit", "tfl_transform_minimal.py"], check=True)

def run_analysis():
    """Run the simple aggregation/analysis step."""
    subprocess.run(["spark-submit", "tfl_analysis_minimal.py"], check=True)

# ------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------
with DAG(
    dag_id="tfl_pipeline_dag",
    description="TfL Bus Arrivals Pipeline (instant API -> transform -> analysis)",
    start_date=datetime(2025, 11, 10),
    schedule_interval="*/2 * * * *",  # every 2 minutes (you can change)
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["tfl", "bus", "pyspark", "data-engineering"]
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

    ingest >> transform >> analyze