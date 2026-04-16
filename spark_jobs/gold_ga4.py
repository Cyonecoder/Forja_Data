import os, logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as spark_sum, avg, round as spark_round,
    col, lpad, concat_ws, to_date, date_format
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD-GA4] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
SILVER_PATH    = "s3a://forja-datalake/silver/ga4/"
PG_URL         = "jdbc:postgresql://forja_postgres:5432/snrt_stats"
PG_PROPS       = {
    "user":     os.getenv("PG_USER",     "snrt_readonly"),
    "password": os.getenv("PG_PASSWORD", "6AOd3Dm2"),
    "driver":   "org.postgresql.Driver"
}
PG_HOST       = "forja_postgres"
PG_PORT       = 5432
PG_DB         = "snrt_stats"
TARGET_TABLE  = "gold_ga4_daily_stats"
TMP_TABLE     = "gold_ga4_daily_stats_tmp"

def create_spark():
    return (SparkSession.builder
        .appName("FORJA-Gold-GA4")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory",                            "1500m")
        .config("spark.hadoop.fs.s3a.endpoint",                   MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                 os.getenv("MINIO_ROOT_USER",     "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",                 os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access",          "true")
        .config("spark.hadoop.fs.s3a.impl",                       "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                    "4")
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
                        report_date       TEXT,
                        report_type       TEXT,
                        device_category   TEXT,
                        active_users      DOUBLE PRECISION,
                        sessions          DOUBLE PRECISION,
                        new_users         DOUBLE PRECISION,
                        bounce_rate_pct   DOUBLE PRECISION,
                        avg_session_min   DOUBLE PRECISION,
                        event_count       DOUBLE PRECISION,
                        updated_at        TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (report_date, report_type, device_category)
                    );
                """)
                cur.execute(f"""
                    INSERT INTO {TARGET_TABLE}
                        (report_date, report_type, device_category,
                         active_users, sessions, new_users,
                         bounce_rate_pct, avg_session_min, event_count, updated_at)
                    SELECT report_date, report_type, device_category,
                           active_users, sessions, new_users,
                           bounce_rate_pct, avg_session_min, event_count, NOW()
                    FROM {TMP_TABLE}
                    ON CONFLICT (report_date, report_type, device_category)
                    DO UPDATE SET
                        active_users    = EXCLUDED.active_users,
                        sessions        = EXCLUDED.sessions,
                        new_users       = EXCLUDED.new_users,
                        bounce_rate_pct = EXCLUDED.bounce_rate_pct,
                        avg_session_min = EXCLUDED.avg_session_min,
                        event_count     = EXCLUDED.event_count,
                        updated_at      = NOW();
                """)
                log.info(f"✅ UPSERT {TARGET_TABLE} réussi")
    finally:
        conn.close()

def main():
    spark = None
    try:
        log.info("🚀 FORJA Gold GA4 démarrage")
        spark = create_spark()
        spark.sparkContext.setLogLevel("WARN")
        try:
            df = spark.read.parquet(SILVER_PATH)
        except Exception as e:
            log.error(f"❌ Impossible de lire Silver GA4: {e}")
            raise
        # Extraire year/month/day depuis ga4_date si colonnes absentes
        from pyspark.sql.functions import year as yr, month as mo, dayofmonth as dom, to_date
        if 'year' not in df.columns:
            df = df.withColumn('year',  yr(to_date('ga4_date')))                    .withColumn('month', mo(to_date('ga4_date')))                    .withColumn('day',   dom(to_date('ga4_date')))
        row_count = df.count()
        log.info(f"✅ Silver chargé: {row_count} lignes | Colonnes: {df.columns}")
        if row_count == 0:
            log.warning("⚠️  Silver GA4 vide — job annulé sans écriture")
            return
        report_date = date_format(
            to_date(
                concat_ws("-",
                    col("year").cast("string"),
                    lpad(col("month").cast("string"), 2, "0"),
                    lpad(col("day").cast("string"),   2, "0")
                ), "yyyy-MM-dd"
            ), "yyyy-MM-dd"
        )
        df_gold = (df
            .withColumn("report_date", report_date)
            .groupBy("report_date", "report_type", "device_category_fr")
            .agg(
                spark_round(spark_sum("activeUsers"),        0).alias("active_users"),
                spark_round(spark_sum("sessions"),           0).alias("sessions"),
                spark_round(spark_sum("newUsers"),           0).alias("new_users"),
                spark_round(avg("bounce_rate_pct"),          2).alias("bounce_rate_pct"),
                spark_round(avg("averageSessionDuration"),   2).alias("avg_session_min"),
                spark_round(spark_sum("eventCount"),         0).alias("event_count")
            )
            .withColumnRenamed("device_category_fr", "device_category")
        )
        log.info(f"📊 Gold GA4: {df_gold.count()} lignes")
        df_gold.show(5, truncate=False)
        upsert_to_postgres(df_gold)
        log.info("🎯 Gold GA4 terminé avec succès")
    except Exception as e:
        log.error(f"❌ Gold GA4 FAILED: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
