# TfL Real-Time Streaming Pipeline

This project ingests live arrival data from Transport for London (TfL), lands the raw feed in Kafka, persists it to a bronze data lake, transforms it with PySpark, and analyzes / exports curated results for downstream use. It ships with an Airflow + Kafka + Postgres stack (via Docker Compose), standalone scripts for each stage, and helper tooling to validate or reset the environment quickly.

---

## System Overview

```
TfL API ➜ (scripts/tfl_ingest_minimal.py) ➜ Kafka topic `tfl_raw`
        ➜ (scripts/tfl_consumer.py) ➜ Bronze JSON (`/opt/airflow/data/bronze`)
        ➜ (scripts/tfl_transform_minimal.py) ➜ Gold Parquet + JSON (`/opt/airflow/data/gold`, `/tmp/tfl_data`)
        ➜ (scripts/tfl_analysis_minimal.py)
        ➜ Orchestrated by Airflow DAG `tfl_realtime_pipeline`
```

Key services (all defined in `docker-compose.yml`):
- **Airflow (webserver + scheduler)** – orchestrates ingestion → Kafka consume → wait → Spark transform → analysis.
- **Postgres** – Airflow metadata database.
- **Kafka + Zookeeper + Kafka UI** – streaming backbone and web UI on `http://localhost:8090`.
- **PySpark** – bundled into the Airflow image for transformations.

---

## Repository Structure

| Path | Description |
| --- | --- |
| `docker-compose.yml` | Spins up Postgres, Kafka/Zookeeper, Kafka UI, and custom Airflow image. |
| `Dockerfile` | Extends `apache/airflow:2.10.2-python3.12`, installs Java + all Python deps (Kafka, PySpark, boto3, etc.). |
| `dags/tfl_pipeline_dag.py` | Airflow DAG (`tfl_realtime_pipeline`) that chains ingest → consume → wait → transform → analyze. |
| `scripts/tfl_ingest_minimal.py` | Fetches TfL arrivals and sends a single batch to Kafka topic `tfl_raw`. |
| `scripts/tfl_consumer.py` | Reads Kafka messages, writes each record to bronze JSON files. |
| `scripts/tfl_transform_minimal.py` | PySpark job that cleans/selects fields and writes Parquet to gold. |
| `scripts/tfl_analysis_minimal.py` | Simple aggregation (avg wait times) over gold data. |
| `data/bronze`, `data/gold` | Host-mounted landing zones mirroring `/opt/airflow/data/...` inside containers. |
| `.env` | Sample configuration for Docker Compose (AWS creds, Airflow admin user, UID/GID). |
| `requirements.txt` | Python dependency lock for the Airflow image. |

---

## Prerequisites

- Docker + Docker Compose v2
- Python 3.10+ (for running scripts locally, outside containers)
- Internet access for TfL API calls
- Optional: AWS account/credentials if you plan to push data to S3 (`s3:CreateBucket`, `s3:ListBucket`, `s3:PutObject`)

---

## Quick Start (Docker Compose)

1. **Build the custom Airflow image**
   ```bash
   docker compose build
   ```

2. **Fix Airflow UID issue (macOS/Linux)**
   
   Create or update your `.env` file to prevent user identification errors:
   ```bash
   echo "AIRFLOW_UID=50000" >> .env
   ```

3. **Initialize Airflow DB/users + create data directories**
   ```bash
   docker compose run --rm airflow-init
   ```
   
   **If user creation fails** (UID error on macOS), create the admin user manually:
   ```bash
   docker compose up -d
   docker compose exec airflow-scheduler airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin
   ```

4. **Launch the stack**
   ```bash
   docker compose up -d
   ```
   Services:
   - Airflow UI: `http://localhost:8080` (credentials: `admin` / `admin`)
   - Kafka UI: `http://localhost:8090`

5. **Validate before triggering**
   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/scripts/validate_pipeline.py
   ```

6. **Smoke test the whole pipeline**
   ```bash
   ./test_pipeline_simple.sh
   ```
   This script ingests once, checks Kafka, consumes to bronze, transforms to gold, and runs analysis.

7. **Trigger the scheduled DAG**
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger tfl_realtime_pipeline
   ```
   The DAG runs every 15 minutes (`*/15 * * * *`). Monitor in the Airflow UI or via `docker compose logs airflow-scheduler`.

8. **Shut down**
   ```bash
   docker compose down
   ```
   Add `-v` if you want to wipe the Postgres volume and start fresh.

---

## Running Individual Stages Manually

Inside the scheduler container:
```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/tfl_ingest_minimal.py
docker compose exec airflow-scheduler python /opt/airflow/scripts/tfl_consumer.py
docker compose exec airflow-scheduler python /opt/airflow/scripts/tfl_transform_minimal.py
docker compose exec airflow-scheduler python /opt/airflow/scripts/tfl_analysis_minimal.py
```

Outside Docker (useful for development):
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python scripts/tfl_ingest_minimal.py   # requires local Kafka at kafka:9092 or adjust env
```

---

## Data Layout

| Layer | Location (host) | Description |
| --- | --- | --- |
| Bronze | `data/bronze/` | Raw TfL JSON snapshots (one file per consumed Kafka message). |
| Gold | `data/gold/` | Parquet output from Spark with curated columns (`arrival_id`, `line_name`, `station_name`, etc.). |
| Temp JSON | `/tmp/tfl_data/` inside container | Coalesced JSON export used by `scripts/tfl_upload_s3.py`. |

These directories are bind-mounted into the Airflow containers (`/opt/airflow/data/...`), so host tooling (Spark, pandas, etc.) can inspect files directly.

---

## Validation & Troubleshooting

1. **Health Checks**
   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/scripts/check_pipeline.py
   ```
   Prints directory listings, schema info, Kafka connectivity, PySpark status, and AWS configuration sanity.

2. **Kafka visibility**
   - Use Kafka UI (`http://localhost:8090`) to inspect topics, offsets, and sample messages.
   - CLI example:  
     `docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic tfl_raw --from-beginning --max-messages 5`

3. **Airflow Login Issues**
   If you cannot log in to the Airflow UI (`http://localhost:8080`):
   - Default credentials: `admin` / `admin`
   - If user wasn't created, run:
     ```bash
     docker compose exec airflow-scheduler airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
     ```

4. **Full clean restart**
   ```bash
   docker compose down -v
   rm -rf data/bronze data/gold logs /tmp/tfl_data
   docker compose build
   echo "AIRFLOW_UID=50000" > .env
   docker compose run --rm airflow-init
   docker compose up -d
   ```

5. **Common issues**
   - *Ingest task hangs*: usually waiting on TfL API or failing to reach Kafka (`kafka:9092`). Confirm network access from the Airflow container.
   - *Consumer finds zero files*: ensure ingest actually published new messages and the consumer group is pointing to `tfl_raw`.
   - *PySpark errors*: verify the Airflow image was rebuilt (so Java + PySpark exist) and the container has enough memory.
   - *UID not found error*: Add `AIRFLOW_UID=50000` to your `.env` file and restart services.