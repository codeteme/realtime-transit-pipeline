from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
BRONZE_DIR = "data/bronze"
GOLD_DIR = "data/gold"

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
df = spark.read.json(str(Path(BRONZE_DIR) / "*.json"))

# -----------------------------
# SELECT & CLEAN
# -----------------------------
# Keep only the most useful columns
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
# WRITE TO PARQUET
# -----------------------------
(
    clean_df.write
    .mode("overwrite")
    .parquet(GOLD_DIR)
)

print(f"✅ Cleaned data written to {GOLD_DIR}")
spark.stop()