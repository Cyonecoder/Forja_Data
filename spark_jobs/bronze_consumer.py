import os, time, logging, json
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, year, month, dayofmonth, to_timestamp, to_date
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BRONZE-CONSUMER] %(levelname)s — %(message)s")
log = logging.getLogger("BRONZE-CONSUMER")

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_GA4      = "ga4.events"
BRONZE_PATH    = "s3a://forja-datalake/bronze/ga4/"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
BUCKET         = "forja-datalake"
OFFSET_KEY     = "offsets/bronze_ga4_offset.txt"
INTERVAL       = 60
BATCH_SIZE     = 50000

# Schema réel basé sur les messages Kafka observés
GA4_SCHEMA = StructType([
    StructField("report_type",              StringType(),  True),
    StructField("timestamp",                StringType(),  True),
    # page_views
    StructField("pagePath",                 StringType(),  True),
    StructField("pageTitle",                StringType(),  True),
    StructField("screenPageViews",          LongType(),    True),
    StructField("activeUsers",              LongType(),    True),
    StructField("averageSessionDuration",   DoubleType(),  True),
    # user_traffic
    StructField("sessionSource",            StringType(),  True),
    StructField("sessionMedium",            StringType(),  True),
    StructField("sessions",                 LongType(),    True),
    StructField("newUsers",                 LongType(),    True),
    StructField("bounceRate",               DoubleType(),  True),
    # device_info
    StructField("deviceCategory",           StringType(),  True),
    StructField("operatingSystem",          StringType(),  True),
    StructField("browser",                  StringType(),  True),
    # events
    StructField("eventName",                StringType(),  True),
    StructField("eventCount",               LongType(),    True),
    StructField("eventCountPerUser",        DoubleType(),  True),
    # content_engagement
    StructField("userEngagementDuration",   DoubleType(),  True),
    StructField("engagedSessions",          LongType(),    True),
    StructField("scrolledUsers",            LongType(),    True),
])

def get_s3():
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def load_offset(s3):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=OFFSET_KEY)
        val = int(obj["Body"].read().decode().strip())
        log.info(f"📌 Offset chargé depuis MinIO: {val}")
        return val
    except:
        log.info("📌 Pas d'offset → démarrage depuis 0")
        return 0

def save_offset(s3, offset):
    s3.put_object(Bucket=BUCKET, Key=OFFSET_KEY, Body=str(offset).encode())

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Bronze-GA4-Consumer")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory",                         "512m")
        .config("spark.driver.memory",                           "512m")
        .config("spark.executor.cores",                          "1")
        .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",                MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                   "2")
        .getOrCreate())

def run_batch(spark, s3, starting_offset):
    df_raw = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_GA4)
        .option("startingOffsets", json.dumps({"ga4.events": {"0": starting_offset}}))
        .option("endingOffsets",   json.dumps({"ga4.events": {"0": starting_offset + BATCH_SIZE}}))
        .option("failOnDataLoss",  "false")
        .load())

    count = df_raw.count()
    if count == 0:
        log.info("⏳ Aucun nouveau message Kafka")
        return starting_offset

    max_offset = df_raw.selectExpr("max(offset)").collect()[0][0]
    log.info(f"📥 {count} messages (offset {starting_offset} → {max_offset})")

    df_parsed = (df_raw
        .selectExpr("CAST(value AS STRING) as json_str", "offset as kafka_offset")
        .withColumn("data", from_json(col("json_str"), GA4_SCHEMA))
        .withColumn("ingested_at", current_timestamp())
        .select(col("kafka_offset"), col("ingested_at"), col("data.*"))
    )

    # Extraire la date depuis le champ timestamp ISO (ex: 2026-03-21T14:19:45.356602)
    df_dated = (df_parsed
        .withColumn("event_ts",  to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        .withColumn("ga4_date",  to_date(col("event_ts")))
        .withColumn("year",      year(col("ga4_date")))
        .withColumn("month",     month(col("ga4_date")))
        .withColumn("day",       dayofmonth(col("ga4_date")))
        .filter(col("ga4_date").isNotNull())
        .filter(col("report_type").isNotNull())
        .drop("timestamp")
    )

    row_count = df_dated.count()
    if row_count == 0:
        log.warning("⚠️ 0 lignes après parsing — vérifier le schema")
        return max_offset + 1

    (df_dated.write
        .mode("append")
        .partitionBy("year", "month", "day", "report_type")
        .parquet(BRONZE_PATH))

    log.info(f"✅ {row_count} lignes → MinIO Bronze | offset suivant: {max_offset + 1}")
    return max_offset + 1

def main():
    log.info("🚀 FORJA Bronze Consumer GA4 — démarrage")
    spark  = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    s3     = get_s3()
    offset = load_offset(s3)

    while True:
        try:
            new_offset = run_batch(spark, s3, offset)
            if new_offset != offset:
                save_offset(s3, new_offset)
                offset = new_offset
        except Exception as e:
            log.error(f"❌ Erreur: {e}", exc_info=True)
        log.info(f"⏳ Prochain batch dans {INTERVAL}s...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
