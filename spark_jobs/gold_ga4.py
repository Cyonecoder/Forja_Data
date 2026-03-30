import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, avg, round as spark_round, concat_ws, col

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD-GA4] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
SILVER_PATH = "s3a://forja-datalake/silver/ga4/"
PG_URL = "jdbc:postgresql://postgres:5432/snrt_stats"
PG_PROPS = {"user": "snrt_readonly", "password": "6AOd3Dm2", "driver": "org.postgresql.Driver"}

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Gold-GA4")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())

def main():
    log.info("🚀 FORJA Gold GA4")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(SILVER_PATH)
    log.info(f"✅ Silver chargé: {df.count()} lignes")
    log.info(f"Colonnes disponibles: {df.columns}")

    report_date = concat_ws("-", col("year").cast("string"), col("month").cast("string"), col("day").cast("string"))

    df_gold = (df
        .withColumn("report_date", report_date)
        .groupBy("report_date", "report_type", "device_category_fr")
        .agg(
            spark_round(spark_sum("activeUsers"), 0).alias("active_users"),
            spark_round(spark_sum("sessions"), 0).alias("sessions"),
            spark_round(spark_sum("newUsers"), 0).alias("new_users"),
            spark_round(avg("bounce_rate_pct"), 2).alias("bounce_rate_pct"),
            spark_round(avg("averageSessionDuration"), 2).alias("avg_session_min"),
            spark_round(spark_sum("eventCount"), 0).alias("event_count")
        )
        .withColumnRenamed("device_category_fr", "device_category")
    )
    log.info(f"📊 Gold GA4: {df_gold.count()} lignes")
    df_gold.write.jdbc(url=PG_URL, table="gold_ga4_daily_stats", mode="append", properties=PG_PROPS)
    log.info("✅ Gold GA4 écrit dans PostgreSQL !")
    spark.stop()

if __name__ == "__main__":
    main()
