import os, logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, first, substring

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD-SNRT] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
SILVER_PATH     = "s3a://forja-datalake/silver/snrt/watchings_enriched"
PG_URL          = "jdbc:postgresql://forja_postgres:5432/snrt_stats"
PG_PROPS        = {
    "user":     os.getenv("PG_USER",     "snrt_readonly"),
    "password": os.getenv("PG_PASSWORD", "6AOd3Dm2"),
    "driver":   "org.postgresql.Driver"
}
PG_HOST         = "forja_postgres"
PG_PORT         = 5432
PG_DB           = "snrt_stats"
TARGET_TABLE    = "gold_snrt_content_performance"
TMP_TABLE       = "gold_snrt_content_performance_tmp"

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Gold-SNRT")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "1500m")
        .config("spark.hadoop.fs.s3a.endpoint",                    MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                  os.getenv("MINIO_ROOT_USER",     "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",                  os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access",           "true")
        .config("spark.hadoop.fs.s3a.impl",                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                     "2")
        .getOrCreate())

def upsert_to_postgres(df):
    df.write.jdbc(url=PG_URL, table=TMP_TABLE, mode="overwrite", properties=PG_PROPS)
    log.info(f"✅ Table temporaire {TMP_TABLE} écrite")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=os.getenv("PG_USER","snrt_readonly"), password=os.getenv("PG_PASSWORD","6AOd3Dm2"))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                        content_title    TEXT,
                        content_type     TEXT,
                        report_month     TEXT,
                        total_views      BIGINT,
                        avg_duration_min DOUBLE PRECISION,
                        total_users      BIGINT,
                        top_country      TEXT,
                        top_device       TEXT,
                        updated_at       TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (content_title, content_type, report_month)
                    );
                """)
                cur.execute(f"""
                    INSERT INTO {TARGET_TABLE}
                        (content_title, content_type, report_month,
                         total_views, avg_duration_min, total_users,
                         top_country, top_device, updated_at)
                    SELECT content_title, content_type, report_month,
                           total_views, avg_duration_min, total_users,
                           top_country, top_device, NOW()
                    FROM {TMP_TABLE}
                    ON CONFLICT (content_title, content_type, report_month)
                    DO UPDATE SET
                        total_views      = EXCLUDED.total_views,
                        avg_duration_min = EXCLUDED.avg_duration_min,
                        total_users      = EXCLUDED.total_users,
                        top_country      = EXCLUDED.top_country,
                        top_device       = EXCLUDED.top_device,
                        updated_at       = NOW();
                """)
                log.info(f"✅ UPSERT {TARGET_TABLE} réussi")
    finally:
        conn.close()

def main():
    spark = None
    try:
        log.info("🚀 FORJA Gold SNRT démarrage")
        spark = create_spark()
        spark.sparkContext.setLogLevel("WARN")
        try:
            df = spark.read.parquet(SILVER_PATH)
        except Exception as e:
            log.error(f"❌ Impossible de lire Silver SNRT: {e}")
            raise
        row_count = df.count()
        log.info(f"✅ Silver chargé: {row_count} lignes")
        if row_count == 0:
            log.warning("⚠️  Silver SNRT vide — job annulé sans écriture")
            return
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
        upsert_to_postgres(df_gold)
        log.info("🎯 Gold SNRT terminé avec succès")
    except Exception as e:
        log.error(f"❌ Gold SNRT FAILED: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
