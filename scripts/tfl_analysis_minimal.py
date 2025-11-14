#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Analysis").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/opt/airflow/data/gold")
print(f"\n📊 Total records: {df.count()}")
df.show(10)
spark.stop()
