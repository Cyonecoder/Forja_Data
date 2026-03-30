import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, first, substring

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD-SNRT] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
SILVER_PATH = "s3a://forja-datalake/silver/snrt/watchings_enriched"
PG_URL = "jdbc:postgresql://forja_postgres:5432/snrt_stats"
PG_PROPS = {"user": "snrt_readonly", "password": "6AOd3Dm2", "driver": "org.postgresql.Driver"}

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Gold-SNRT")
        .master("local[2]")
        .config("spark.driver.memory", "1500m")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())

def main():
    log.info("🚀 FORJA Gold SNRT")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)
    log.info(f"✅ Silver chargé: {df.count()} lignes")

    df_gold = (df
        .withColumn("report_month", substring(col("watched_at"), 1, 7))
        .groupBy("content_title", "content_type", "report_month")
        .agg(
            count("w_id").alias("total_views"),
            spark_round(avg(col("watch_duration").cast("float")), 2).alias("avg_duration_min"),
            count("user_id").alias("total_users"),
            first("country").alias("top_country"),
            first("os").alias("top_device")
        )
    )

    log.info(f"📊 Gold SNRT: {df_gold.count()} lignes")
    df_gold.show(5, truncate=False)
    df_gold.write.jdbc(url=PG_URL, table="gold_snrt_content_performance", mode="append", properties=PG_PROPS)
    log.info("✅ Gold SNRT écrit dans PostgreSQL !")
    spark.stop()

if __name__ == "__main__":
    main()
