import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, substring, broadcast

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-SILVER-FULL] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
BASE = "s3a://forja-datalake/bronze/snrt"
OUTPUT = "s3a://forja-datalake/silver/snrt/watchings_enriched"

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Silver-SNRT-Full")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "512m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum", "50")
        .getOrCreate())

def main():
    log.info("🚀 FORJA Silver SNRT — Full")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Charger les tables de référence (petites) en broadcast
    log.info("📖 Chargement contents...")
    contents = (spark.read.parquet(f"{BASE}/contents")
        .withColumn("c_id",          get_json_object(col("raw_json"), "$.id"))
        .withColumn("content_title", get_json_object(col("raw_json"), "$.name_short.fr"))
        .withColumn("content_type",  get_json_object(col("raw_json"), "$.type"))
        .withColumn("category_id",   get_json_object(col("raw_json"), "$.category_id"))
        .select("c_id","content_title","content_type","category_id")
        .dropDuplicates(["c_id"])
    )
    log.info(f"✅ contents: {contents.count()} lignes")

    log.info("📖 Chargement categories...")
    categories = (spark.read.parquet(f"{BASE}/categories")
        .withColumn("cat_id",   get_json_object(col("raw_json"), "$.id"))
        .withColumn("cat_name", get_json_object(col("raw_json"), "$.name.fr"))
        .select("cat_id","cat_name")
        .dropDuplicates(["cat_id"])
    )
    log.info(f"✅ categories: {categories.count()} lignes")

    log.info("📖 Chargement users...")
    users = (spark.read.parquet(f"{BASE}/users")
        .withColumn("u_id",  get_json_object(col("raw_json"), "$.id"))
        .withColumn("role",  get_json_object(col("raw_json"), "$.role"))
        .select("u_id","role")
        .dropDuplicates(["u_id"])
    )
    log.info(f"✅ users: {users.count()} lignes")

    # Traiter watchings par année pour éviter OOM
    log.info("📖 Chargement watchings...")
    watchings = (spark.read.parquet(f"{BASE}/watchings")
        .withColumn("w_id",           get_json_object(col("raw_json"), "$.id"))
        .withColumn("user_id",        get_json_object(col("raw_json"), "$.user_id"))
        .withColumn("content_id",     get_json_object(col("raw_json"), "$.content_id"))
        .withColumn("watch_duration", get_json_object(col("raw_json"), "$.duration").cast("float"))
        .withColumn("watched_at",     get_json_object(col("raw_json"), "$.created_at"))
        .withColumn("country",        get_json_object(col("raw_json"), "$.country"))
        .withColumn("os",             get_json_object(col("raw_json"), "$.os"))
        .withColumn("report_year",    col("year"))
        .select("w_id","user_id","content_id","watch_duration","watched_at","country","os","report_year")
    )

    log.info("🔗 Jointures...")
    enriched = (watchings
        .join(broadcast(contents), watchings["content_id"] == contents["c_id"], "left")
        .join(broadcast(categories), contents["category_id"] == categories["cat_id"], "left")
        .join(broadcast(users), watchings["user_id"] == users["u_id"], "left")
        .select(
            "w_id","user_id","content_id","content_title","content_type",
            "cat_name","watch_duration","watched_at","country","os","role","report_year"
        )
    )

    log.info("💾 Écriture Silver partitionné par année...")
    (enriched.write
        .mode("overwrite")
        .partitionBy("report_year")
        .parquet(OUTPUT)
    )
    log.info("✅ Silver SNRT FULL écrit dans MinIO !")
    spark.stop()

if __name__ == "__main__":
    main()
