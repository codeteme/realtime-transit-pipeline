FROM apache/airflow:2.10.2-python3.12

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && apt-get clean

# Switch back to airflow user for Python package installation
USER airflow
RUN pip install --no-cache-dir pyspark boto3 awscli

# Copy your scripts
COPY scripts/ /opt/airflow/scripts/
ENV PATH="/opt/airflow/scripts:${PATH}"