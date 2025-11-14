# import requests
# import json
# import time
# from datetime import datetime, timedelta
# from pathlib import Path
# import sys

# # -----------------------------
# # CONFIG
# # -----------------------------
# BASE_URL = "https://api.tfl.gov.uk/StopPoint/{stop_id}/Arrivals"
# STOP_ID = "490008660N"   # example stop (replace with yours)
# OUT_DIR = Path("data/bronze")
# POLL_INTERVAL = 30        # seconds, matches TfL refresh frequency
# RUN_DURATION = 120        # seconds, total duration to run the loop

# # -----------------------------
# # MAIN LOOP
# # -----------------------------
# def fetch_and_save():
#     OUT_DIR.mkdir(parents=True, exist_ok=True)
#     url = BASE_URL.format(stop_id=STOP_ID)
#     print(f"Fetching from {url} ...")

#     try:
#         response = requests.get(url, timeout=10)
#         response.raise_for_status()
#         data = response.json()
#     except Exception as e:
#         print(f"[{datetime.now()}] Error fetching data: {e}")
#         return False

#     ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
#     out_path = OUT_DIR / f"bus_arrivals_{ts}.json"

#     with open(out_path, "w") as f:
#         json.dump(data, f)

#     print(f"[{datetime.now()}] Saved {len(data)} records to {out_path}")
#     return True

# # -----------------------------
# # LOOP (or single-run)
# # -----------------------------
# if __name__ == "__main__":
#     start_time = datetime.utcnow()
#     end_time = start_time + timedelta(seconds=RUN_DURATION)
#     success_count = 0

#     while datetime.utcnow() < end_time:
#         if fetch_and_save():
#             success_count += 1
#         time.sleep(POLL_INTERVAL)

#     print(f"[{datetime.now()}] Finished running for {RUN_DURATION} seconds.")
#     print(f"Total successful fetches: {success_count}")

#     if success_count > 0:
#         sys.exit(0)
#     else:
#         sys.exit(1)

from kafka import KafkaProducer
import json, requests

def ingest():
    url = "https://api.tfl.gov.uk/Line/centrall/Arrivals"
    data = requests.get(url).json()

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    producer.send("tfl_raw", data)
    producer.flush()

    print("📨 Sent TfL message to Kafka topic 'tfl_raw'")

if __name__ == "__main__":
    ingest()