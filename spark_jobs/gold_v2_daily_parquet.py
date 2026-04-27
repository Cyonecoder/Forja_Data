import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"[GOLD V2 PARQUET] Traitement du {TARGET_DATE}")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://forja_minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SILVER_PATH = "s3a://forja-datalake/silver/snrt/watchings_enriched/"
GOLD_BASE   = "s3a://forja-datalake/gold_v2/snrt"

spark = SparkSession.builder \
    .appName(f"gold_v2_daily_parquet_{TARGET_DATE}") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    df_raw = spark.read.parquet(SILVER_PATH)
    df = df_raw.withColumn("watched_date", F.to_date(F.col("watched_at"))) \
               .filter(F.col("watched_date") == F.lit(TARGET_DATE))

    count_silver = df.count()
    print(f"[GOLD V2 PARQUET] Lignes Silver pour {TARGET_DATE} : {count_silver}")
    if count_silver == 0:
        print("[GOLD V2 PARQUET] Aucune donnée, on arrête proprement.")
        spark.stop()
        sys.exit(0)

    MAX_DURATION = 600.0
    df_content = df \
        .withColumn("watch_dur_clean", F.least(F.col("watch_duration").cast("double"), F.lit(MAX_DURATION))) \
        .groupBy("watched_date", "content_id", "content_title", "content_type", "cat_name") \
        .agg(
            F.count("w_id").alias("total_views"),
            F.countDistinct("user_id").alias("total_users"),
            (F.sum("watch_dur_clean") / F.count("w_id") / 60.0).alias("avg_duration_min"),
            (F.sum("watch_dur_clean") / 60.0).alias("watch_time_total"),
            F.first(F.col("country"), ignorenulls=True).alias("top_country"),
            F.first(F.col("os"),      ignorenulls=True).alias("top_os")
        ) \
        .withColumn("completion_rate", F.lit(0.0)) \
        .withColumn("report_date", F.col("watched_date")) \
        .select(
            "report_date",
            "content_id",
            "content_title",
            "content_type",
            "cat_name",
            "total_views",
            "total_users",
            "avg_duration_min",
            "watch_time_total",
            "completion_rate",
            "top_country",
            "top_os"
        )

    rows_content = df_content.count()
    print(f"[GOLD V2 PARQUET] Lignes contenu agrégées : {rows_content}")

    out_path_content = f"{GOLD_BASE}/daily_content/report_date={TARGET_DATE}"
    df_content.write.mode("overwrite").parquet(out_path_content)
    print(f"[GOLD V2 PARQUET] ✅ Écrit : {out_path_content}")

except Exception as e:
    print(f"[GOLD V2 PARQUET] ❌ Erreur : {e}")
    spark.stop()
    sys.exit(1)

spark.stop()
