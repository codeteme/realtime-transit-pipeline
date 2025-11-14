#!/usr/bin/env python3
import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_DIR = "/opt/airflow/data/bronze"
GOLD_DIR = "/opt/airflow/data/gold"

print("🔧 Starting transform...")

spark = SparkSession.builder.appName("TfL").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("multiLine", "true").json(f"{BRONZE_DIR}/tfl_raw_*.json")

df_clean = df.select(
    col("id").alias("arrival_id"),
    col("lineName").alias("line_name"),
    col("stationName").alias("station"),
    col("timeToStation").alias("seconds")
).filter(col("seconds").isNotNull())

print(f"✅ Got {df_clean.count()} records")

# NO PARTITIONING - just write
df_clean.write.mode("overwrite").parquet(GOLD_DIR)
print("✅ Wrote to gold")
spark.stop()
