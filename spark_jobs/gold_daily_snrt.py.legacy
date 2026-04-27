import sys, os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, sum, first, lit, current_timestamp, to_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s - %(message)s")
logger = logging.getLogger("GOLD-DAILY-SNRT")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
JDBC_URL = "jdbc:postgresql://172.21.0.11:5432/snrt_stats?options=-c%20search_path%3Dpublic"
JDBC_PROPS = {"user": "snrt_readonly", "password": "6AOd3Dm2", "driver": "org.postgresql.Driver"}

def build_spark():
    return (SparkSession.builder.appName("FORJA-Gold-Daily-SNRT")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())

def main():
    logger.info("START FORJA Gold Daily SNRT")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Lecture watchings_enriched...")
    df = spark.read.parquet("s3a://forja-datalake/silver/snrt/watchings_enriched/")
    df = (df.withColumn("watch_date", to_date(col("watched_at")))
            .withColumn("duration_sec", col("watch_duration").cast("double"))
            .filter(col("watch_date").isNotNull())
            .select("watch_date","duration_sec","user_id","content_title","content_type","country","os"))
    logger.info("watchings_enriched charge OK")

    logger.info("Calcul gold_daily_content_perf...")
    df_content = df.groupBy(
        col("watch_date").alias("report_date"),
        col("content_title"),
        col("content_type")
    ).agg(
        count("*").cast("long").alias("total_views"),
        countDistinct("user_id").cast("long").alias("total_users"),
        avg(col("duration_sec") / 60.0).alias("avg_duration_min"),
        (sum(col("duration_sec")) / 60.0).alias("watch_time_total"),
        lit(0.0).cast("double").alias("completion_rate"),
        first("country", ignorenulls=True).alias("top_country"),
        lit(None).cast("string").alias("top_city"),
        first("os", ignorenulls=True).alias("top_os"),
        lit(None).cast("string").alias("top_browser")
    ).withColumn("updated_at", current_timestamp()).coalesce(1)

    logger.info("Ecriture gold_daily_content_perf (append)...")
    df_content.write.jdbc(url=JDBC_URL, table="gold_daily_content_performance", mode="append", properties=JDBC_PROPS)
    logger.info("gold_daily_content_perf OK")

    logger.info("Calcul gold_daily_users...")
    df_users = df.groupBy(col("watch_date").alias("report_date")).agg(
        countDistinct("user_id").cast("long").alias("new_users"),
        countDistinct("user_id").cast("long").alias("total_users_cumul"),
        lit(None).cast("long").alias("users_email_only"),
        lit(None).cast("long").alias("users_phone_only"),
        lit(None).cast("long").alias("users_email_phone")
    ).withColumn("updated_at", current_timestamp()).coalesce(1)

    logger.info("Ecriture gold_daily_users (append)...")
    df_users.write.jdbc(url=JDBC_URL, table="gold_daily_users", mode="append", properties=JDBC_PROPS)
    logger.info("gold_daily_users OK")
    spark.stop()
    logger.info("Gold Daily SNRT termine avec succes")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("ERREUR FATALE: " + str(e), exc_info=True)
        sys.exit(1)
