import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-SILVER-SAMPLE] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BASE = "s3a://forja-datalake/bronze/snrt"

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Silver-SNRT-Sample")
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
    log.info("🚀 FORJA Silver SNRT — Sample")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    watchings = (spark.read.parquet(f"{BASE}/watchings").limit(500)
        .withColumn("w_id",         get_json_object(col("raw_json"), "$.id"))
        .withColumn("user_id",      get_json_object(col("raw_json"), "$.user_id"))
        .withColumn("content_id",   get_json_object(col("raw_json"), "$.content_id"))
        .withColumn("watch_duration", get_json_object(col("raw_json"), "$.duration"))
        .withColumn("watched_at",   get_json_object(col("raw_json"), "$.created_at"))
        .withColumn("device_type",  get_json_object(col("raw_json"), "$.os"))
        .withColumn("country",      get_json_object(col("raw_json"), "$.country"))
        .select("w_id","user_id","content_id","watch_duration","watched_at","device_type","country")
    )
    log.info(f"✅ watchings parsés: {watchings.count()} lignes")

    contents = (spark.read.parquet(f"{BASE}/contents").limit(500)
        .withColumn("c_id",           get_json_object(col("raw_json"), "$.id"))
        .withColumn("content_title",  get_json_object(col("raw_json"), "$.name_short.fr"))
        .withColumn("content_type",   get_json_object(col("raw_json"), "$.type"))
        .select("c_id","content_title","content_type")
    )
    log.info(f"✅ contents parsés: {contents.count()} lignes")

    users = (spark.read.parquet(f"{BASE}/users").limit(500)
        .withColumn("u_id",   get_json_object(col("raw_json"), "$.id"))
        .withColumn("email",  get_json_object(col("raw_json"), "$.email"))
        .withColumn("role",   get_json_object(col("raw_json"), "$.role"))
        .select("u_id","email","role")
    )
    log.info(f"✅ users parsés: {users.count()} lignes")

    enriched = (watchings
        .join(contents, watchings["content_id"] == contents["c_id"], "left")
        .join(users,    watchings["user_id"]    == users["u_id"],    "left")
        .select("w_id","user_id","content_id","content_title","content_type",
                "watch_duration","watched_at","device_type","country","email","role")
    )

    log.info(f"✅ Enriched: {enriched.count()} lignes")
    enriched.show(5, truncate=False)

    OUTPUT = "s3a://forja-datalake/silver/snrt/watchings_enriched"
    enriched.write.mode("overwrite").parquet(OUTPUT)
    log.info("✅ Silver SNRT sample écrit dans MinIO !")
    spark.stop()

if __name__ == "__main__":
    main()
