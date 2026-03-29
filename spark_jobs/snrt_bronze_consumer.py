import os, time, logging, json
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-BRONZE] %(levelname)s — %(message)s")
log = logging.getLogger("SNRT-BRONZE")

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",          "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",         "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD",     "minioadmin123")
BUCKET         = "forja-datalake"
BRONZE_BASE    = "s3a://forja-datalake/bronze/snrt"
INTERVAL       = 120
BATCH_SIZE     = 50000

TOPICS = {
    "snrt-watchings":          "watchings",
    "snrt-users":              "users",
    "snrt-profiles":           "profiles",
    "snrt-contents":           "contents",
    "snrt-subscriptions":      "subscriptions",
    "snrt-user-subscriptions": "user_subscriptions",
    "snrt-programs":           "programs",
    "snrt-categories":         "categories",
    "snrt-user-fav":           "user_fav",
    "snrt-user-like":          "user_like",
}

def get_s3():
    return boto3.client("s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def load_offset(s3, topic):
    key = f"offsets/snrt_{topic.replace('-','_')}.txt"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        val = int(obj["Body"].read().decode().strip())
        return val
    except:
        return 0

def save_offset(s3, topic, offset):
    key = f"offsets/snrt_{topic.replace('-','_')}.txt"
    s3.put_object(Bucket=BUCKET, Key=key, Body=str(offset).encode())

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Bronze-SNRT-Consumer")
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

def run_batch(spark, s3, topic, table_name, starting_offset):
    df = (spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", json.dumps({topic: {"0": starting_offset}}))
        .option("endingOffsets",   json.dumps({topic: {"0": starting_offset + BATCH_SIZE}}))
        .option("failOnDataLoss",  "false")
        .load())

    count = df.count()
    if count == 0:
        return starting_offset

    max_offset = df.selectExpr("max(offset)").collect()[0][0]

    df_bronze = (df
        .selectExpr(
            "CAST(value AS STRING) as raw_json",
            "CAST(key AS STRING)   as kafka_key",
            "timestamp             as kafka_timestamp",
            "partition             as kafka_partition",
            "offset                as kafka_offset"
        )
        .withColumn("ingested_at", current_timestamp())
        .withColumn("table_name",  lit(table_name))
        .withColumn("year",        year(col("ingested_at")))
        .withColumn("month",       month(col("ingested_at")))
        .withColumn("day",         dayofmonth(col("ingested_at")))
    )

    output_path = f"{BRONZE_BASE}/{table_name}"
    (df_bronze.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(output_path))

    log.info(f"✅ [{table_name}] {count} lignes → {output_path} | offset: {max_offset + 1}")
    return max_offset + 1

def main():
    log.info("🚀 FORJA Bronze Consumer SNRT — démarrage")
    spark   = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    s3      = get_s3()
    offsets = {topic: load_offset(s3, topic) for topic in TOPICS}
    log.info(f"📌 Offsets initiaux: {offsets}")

    while True:
        for topic, table_name in TOPICS.items():
            try:
                new_off = run_batch(spark, s3, topic, table_name, offsets[topic])
                if new_off != offsets[topic]:
                    save_offset(s3, topic, new_off)
                    offsets[topic] = new_off
            except Exception as e:
                log.error(f"❌ [{table_name}]: {e}", exc_info=True)
        log.info(f"⏳ Prochain batch dans {INTERVAL}s...")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()
