from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import boto3
import os
import logging
import glob

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

S3_BUCKET = "temesgen-tfl-pipeline"
LOCAL_TRANSFORM_PATH = "/tmp/tfl_data/latest_transformed.json"
S3_OBJECT_KEY_JSON = "transformed/latest_transformed.json"
S3_OBJECT_KEY_PARQUET_PREFIX = "transformed/parquet/"

# ------------------------------------------------------------
# PYTHON FUNCTIONS
# ------------------------------------------------------------

def run_ingest():
    """Run the ingestion script and return success status."""
    try:
        subprocess.run(["python", "/opt/airflow/scripts/tfl_ingest_minimal.py"], check=True)
        logging.info("Ingestion script completed successfully.")
        return "Ingestion completed successfully"
    except subprocess.CalledProcessError as e:
        logging.error(f"Ingestion script failed: {e}")
        raise RuntimeError(f"Ingestion script failed: {e}") from e
    except Exception as e:
        logging.error(f"Unexpected error during ingestion: {e}")
        raise

def run_transform():
    """Run the PySpark transformation script."""
    try:
        subprocess.run(["spark-submit", "/opt/airflow/scripts/tfl_transform_minimal.py"], check=True)
        logging.info("Transformation script completed successfully.")
        return "Transformation completed successfully"
    except subprocess.CalledProcessError as e:
        logging.error(f"Transformation script failed: {e}")
        raise RuntimeError(f"Transformation script failed: {e}") from e
    except Exception as e:
        logging.error(f"Unexpected error during transformation: {e}")
        raise

def run_analysis():
    """Run the aggregation/analysis step."""
    try:
        subprocess.run(["spark-submit", "/opt/airflow/scripts/tfl_analysis_minimal.py"], check=True)
        logging.info("Analysis script completed successfully.")
        return "Analysis completed successfully"
    except subprocess.CalledProcessError as e:
        logging.error(f"Analysis script failed: {e}")
        raise RuntimeError(f"Analysis script failed: {e}") from e
    except Exception as e:
        logging.error(f"Unexpected error during analysis: {e}")
        raise

def upload_to_s3():
    """Upload the transformed file to S3 with error handling and logging."""
    from datetime import datetime
    s3 = boto3.client("s3")

    # Look for JSON files created by transform
    latest_files = glob.glob("/tmp/tfl_data/transformed_*.json")
    
    if not latest_files:
        # Fallback: check for parquet files in /opt/airflow/data/gold
        parquet_dir = "/opt/airflow/data/gold"
        parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
        
        if not parquet_files:
            error_msg = "No transformed JSON or Parquet files found to upload."
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Upload parquet files
        uploaded_count = 0
        for parquet_file in parquet_files:
            file_name = os.path.basename(parquet_file)
            s3_key = f"{S3_OBJECT_KEY_PARQUET_PREFIX}{file_name}"
            
            try:
                s3.upload_file(parquet_file, S3_BUCKET, s3_key)
                logging.info(f"Uploaded {file_name} to s3://{S3_BUCKET}/{s3_key}")
                uploaded_count += 1
            except Exception as e:
                logging.error(f"Failed to upload {file_name}: {e}")
                raise RuntimeError(f"Failed to upload {file_name}: {e}") from e
        
        return f"Uploaded {uploaded_count} Parquet files to S3"

    # Upload the most recent JSON file
    latest_file = max(latest_files, key=os.path.getmtime)
    file_name = os.path.basename(latest_file)
    s3_key = f"transformed/{file_name}"  # Fixed: Use file_name in key
    
    try:
        s3.upload_file(latest_file, S3_BUCKET, s3_key)
        logging.info(f"✅ Uploaded {latest_file} to s3://{S3_BUCKET}/{s3_key}")
        return f"Uploaded {file_name} to S3 successfully"
    except Exception as e:
        logging.error(f"Failed to upload JSON file: {e}")
        raise RuntimeError(f"Failed to upload JSON file: {e}") from e


# ------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------
with DAG(
    dag_id="tfl_pipeline_dag",
    description="TfL Bus Arrivals Pipeline (ingest -> transform -> analyze -> upload to S3)",
    start_date=datetime(2023, 1, 1),
    schedule_interval="*/2 * * * *",  # every 2 minutes
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["tfl", "bus", "pyspark", "aws", "data-engineering"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_bus_data",
        python_callable=run_ingest,
        do_xcom_push=True,
    )

    transform = PythonOperator(
        task_id="transform_bus_data",
        python_callable=run_transform,
        do_xcom_push=True,
    )

    analyze = PythonOperator(
        task_id="analyze_bus_data",
        python_callable=run_analysis,
        do_xcom_push=True,
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        do_xcom_push=True,
    )

    ingest >> transform >> analyze >> upload