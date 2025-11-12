import requests
import json
import time
from datetime import datetime
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
BASE_URL = "https://api.tfl.gov.uk/StopPoint/{stop_id}/Arrivals"
STOP_ID = "490008660N"   # example stop (replace with yours)
OUT_DIR = Path("data/bronze")
POLL_INTERVAL = 30        # seconds, matches TfL refresh frequency

# -----------------------------
# MAIN LOOP
# -----------------------------
def fetch_and_save():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    url = BASE_URL.format(stop_id=STOP_ID)
    print(f"Fetching from {url} ...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[{datetime.now()}] Error fetching data: {e}")
        return

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_path = OUT_DIR / f"bus_arrivals_{ts}.json"

    with open(out_path, "w") as f:
        json.dump(data, f)

    print(f"[{datetime.now()}] Saved {len(data)} records to {out_path}")

# -----------------------------
# LOOP (or single-run)
# -----------------------------
if __name__ == "__main__":
    while True:
        fetch_and_save()
        time.sleep(POLL_INTERVAL)