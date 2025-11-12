from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pathlib import Path
import os
import sys
import traceback
import glob
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = "/opt/airflow/data"
BRONZE_DIR = f"{DATA_DIR}/bronze"
GOLD_DIR = f"{DATA_DIR}/gold"

# -----------------------------
# MAIN FUNCTION
# -----------------------------
def main():
    spark = None
    try:
        # Generate timestamp for this run
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        JSON_OUTPUT_DIR = f"/tmp/tfl_data/transformed_{timestamp}"
        JSON_FINAL = f"/tmp/tfl_data/transformed_{timestamp}.json"
        
        print(f"🚀 Starting transformation at {timestamp}")
        
        # -----------------------------
        # SPARK SESSION
        # -----------------------------
        spark = (
            SparkSession.builder
            .appName("tfl_transform_minimal")
            .getOrCreate()
        )
        
        # -----------------------------
        # READ RAW JSON
        # -----------------------------
        print(f"📖 Reading data from {BRONZE_DIR}")
        df = spark.read.json(str(Path(BRONZE_DIR) / "*.json"))
        
        row_count = df.count()
        print(f"📊 Loaded {row_count} rows")
        
        if row_count == 0:
            print("⚠️ Warning: No data found in bronze layer")
            return
        
        # -----------------------------
        # SELECT & CLEAN
        # -----------------------------
        print("🧹 Cleaning and transforming data")
        clean_df = (
            df.select(
                col("lineName").alias("route"),
                col("stationName").alias("stop_name"),
                col("expectedArrival").alias("expected_ts"),
                col("timeToStation").alias("seconds_to_arrival")
            )
            .withColumn("expected_time", to_timestamp(col("expected_ts")))
        )
        
        # -----------------------------
        # WRITE TO PARQUET (for data lake)
        # -----------------------------
        print(f"💾 Writing Parquet to {GOLD_DIR}")
        (
            clean_df.write
            .mode("overwrite")
            .parquet(GOLD_DIR)
        )
        print(f"✅ Parquet written to {GOLD_DIR}")
        
        # -----------------------------
        # WRITE TO JSON (for S3 upload)
        # -----------------------------
        print(f"📝 Writing JSON to {JSON_OUTPUT_DIR}")
        
        # Create directory if it doesn't exist
        os.makedirs("/tmp/tfl_data", exist_ok=True)
        
        # Write to temporary directory (Spark creates part files)
        (
            clean_df.coalesce(1)
            .write
            .mode("overwrite")
            .json(JSON_OUTPUT_DIR)
        )
        
        # Find the part file and rename it
        json_files = glob.glob(f"{JSON_OUTPUT_DIR}/part-*.json")
        
        if json_files:
            # Move the part file to final location
            os.rename(json_files[0], JSON_FINAL)
            # Clean up the temporary directory
            import shutil
            shutil.rmtree(JSON_OUTPUT_DIR, ignore_errors=True)
            print(f"✅ JSON written to {JSON_FINAL}")
        else:
            print(f"⚠️ No JSON part files found in {JSON_OUTPUT_DIR}")
            raise FileNotFoundError("JSON output generation failed")
        
        print("🎉 Transformation completed successfully!")
        
    except Exception as e:
        print("❌ Transformation failed:")
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            try:
                spark.stop()
                print("🛑 Spark session stopped")
            except Exception:
                pass

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == '__main__':
    main()