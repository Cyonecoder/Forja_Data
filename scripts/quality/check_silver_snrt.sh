#!/usr/bin/env bash
set -euo pipefail

docker exec -i forja_spark_master python /opt/spark_jobs/check_silver.py || true

docker exec -i forja_spark_master python - <<'PYEOF'
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("silver_quality_check").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = "s3a://forja-datalake/silver/snrt/watchings_enriched/"
df = spark.read.parquet(path)

df = df.withColumn("report_date", F.to_date("watched_at"))

print("=== ROW COUNT ===")
print(df.count())

print("=== DATE RANGE ===")
df.select(F.min("report_date").alias("min_date"), F.max("report_date").alias("max_date")).show(truncate=False)

print("=== DISTINCT DAYS ===")
df.select("report_date").distinct().count()

print("=== NULL COUNTS ===")
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in ["w_id","user_id","content_id","content_title","content_type","watch_duration","watched_at","country","os"]
]).show(truncate=False)

print("=== DUPLICATE W_ID ===")
dup = df.groupBy("w_id").count().filter("count > 1").count()
print(dup)

print("=== TOP 20 DAYS BY ROWS ===")
df.groupBy("report_date").count().orderBy(F.col("report_date").desc()).show(20, truncate=False)

spark.stop()
PYEOF
