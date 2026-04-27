import os, logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, substring, round as spark_round, coalesce, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD-SNRT-ENG] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
SILVER_BASE      = "s3a://forja-datalake/silver/snrt"
SILVER_WATCHINGS = f"{SILVER_BASE}/watchings_enriched"
SILVER_USER_FAV  = f"{SILVER_BASE}/user_fav"
SILVER_USER_LIKE = f"{SILVER_BASE}/user_like"

PG_URL   = "jdbc:postgresql://forja_postgres:5432/snrt_stats"
PG_PROPS = {"user": os.getenv("PG_USER","snrt_readonly"),
            "password": os.getenv("PG_PASSWORD","6AOd3Dm2"),
            "driver": "org.postgresql.Driver"}
PG_HOST = "forja_postgres"; PG_PORT = 5432; PG_DB = "snrt_stats"
TARGET_TABLE = "gold_snrt_engagement"; TMP_TABLE = "gold_snrt_engagement_tmp"

def create_spark():
    return (SparkSession.builder.appName("FORJA-Gold-SNRT-Engagement")
        .master(os.getenv("SPARK_MASTER","local[2]"))
        .config("spark.driver.memory","1500m")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER","minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD","minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions","2").getOrCreate())

def safe_read(spark, path, name):
    try:
        df = spark.read.parquet(path)
        log.info(f"✅ Silver {name}: {df.count()} lignes")
        return df
    except Exception as e:
        log.warning(f"⚠️  Silver {name} inaccessible ({e}) — ignoré")
        return None

def upsert_to_postgres(df):
    df.write.jdbc(url=PG_URL, table=TMP_TABLE, mode="overwrite", properties=PG_PROPS)
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=os.getenv("PG_USER","snrt_readonly"), password=os.getenv("PG_PASSWORD","6AOd3Dm2"))
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                        content_title TEXT, content_type TEXT, report_month TEXT,
                        total_views BIGINT, total_favs BIGINT, total_likes BIGINT,
                        engagement_rate DOUBLE PRECISION,
                        updated_at TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (content_title, content_type, report_month));
                """)
                cur.execute(f"""
                    INSERT INTO {TARGET_TABLE}
                        (content_title,content_type,report_month,total_views,total_favs,total_likes,engagement_rate,updated_at)
                    SELECT content_title,content_type,report_month,total_views,total_favs,total_likes,engagement_rate,NOW()
                    FROM {TMP_TABLE}
                    ON CONFLICT (content_title,content_type,report_month) DO UPDATE SET
                        total_views=EXCLUDED.total_views, total_favs=EXCLUDED.total_favs,
                        total_likes=EXCLUDED.total_likes, engagement_rate=EXCLUDED.engagement_rate,
                        updated_at=NOW();
                """)
                log.info(f"✅ UPSERT {TARGET_TABLE} réussi")
    finally:
        conn.close()

def main():
    spark = None
    try:
        log.info("🚀 FORJA Gold SNRT Engagement")
        spark = create_spark()
        spark.sparkContext.setLogLevel("WARN")
        df_watch = safe_read(spark, SILVER_WATCHINGS, "watchings_enriched")
        if df_watch is None or df_watch.count() == 0:
            log.warning("⚠️  Watchings vide — job annulé"); return
        df_base = (df_watch
            .withColumn("report_month", substring(col("watched_at"), 1, 7))
            .groupBy("content_title","content_type","report_month")
            .agg(count("w_id").alias("total_views")))
        df_gold = (df_base
            .withColumn("total_favs",  lit(0).cast("bigint"))
            .withColumn("total_likes", lit(0).cast("bigint"))
            .withColumn("engagement_rate",
                spark_round((col("total_favs")+col("total_likes")).cast("double")/col("total_views").cast("double")*100,2))
            .select("content_title","content_type","report_month",
                    "total_views","total_favs","total_likes","engagement_rate"))
        log.info(f"📊 Gold Engagement: {df_gold.count()} lignes")
        df_gold.show(5, truncate=False)
        upsert_to_postgres(df_gold)
        log.info("🎯 Gold SNRT Engagement terminé")
    except Exception as e:
        log.error(f"❌ Gold Engagement FAILED: {e}", exc_info=True); raise
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    main()
