from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("tfl_analysis").getOrCreate()

df = spark.read.parquet("data/gold")

result = (
    df.groupBy("route")
      .agg(avg(col("seconds_to_arrival")).alias("avg_wait_sec"))
      .orderBy(col("avg_wait_sec"))
)

result.show(10, truncate=False)
spark.stop()