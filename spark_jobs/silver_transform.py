from pyspark.sql import functions as F
import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, lower, round as spark_round,
    current_timestamp, lit, coalesce, to_date
)
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SILVER-TRANSFORM] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",     "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD","minioadmin123")
BRONZE_PATH    = "s3a://forja-datalake/bronze/ga4/"
SILVER_PATH    = "s3a://forja-datalake/silver/ga4/"

def create_spark():
    spark = (SparkSession.builder
        .appName("FORJA-Silver-GA4-Transform")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.endpoint",                  MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",                MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",                MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access",         "true")
        .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions",                   "4")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_bronze(spark):
    log.info(f"📖 Lecture Bronze: {BRONZE_PATH}")
    from pyspark.sql.types import LongType, DoubleType as DT, StringType
    long_cols = ["userEngagementDuration","engagedSessions","scrolledUsers","eventCount","sessions","newUsers"]
    double_cols = ["screenPageViews","activeUsers","totalUsers","bounceRate","averageSessionDuration"]
    from datetime import date, timedelta
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    y, m, d = yesterday.split("-")
    BRONZE_PATH_J1 = f"{BRONZE_PATH}year={y}/month={int(m)}/day={int(d)}/"
    try:
        df = spark.read.option("mergeSchema","false").parquet(BRONZE_PATH_J1)
        log.info(f"📅 Lecture J-1 uniquement: {yesterday}")
    except Exception:
        df = spark.read.option("mergeSchema","false").parquet(BRONZE_PATH)
        log.info("📅 Fallback: lecture complète Bronze")
    for c in long_cols:
        if c in df.columns: df = df.withColumn(c, df[c].cast(LongType()))
    for c in double_cols:
        if c in df.columns: df = df.withColumn(c, df[c].cast(DT()))
    log.info(f"   → {df.count()} lignes brutes")
    return df

def transform(df):
    log.info("🧹 Nettoyage + enrichissement...")
    optional_cols = [
        "deviceCategory", "eventName", "sessionSource", "pagePath",
        "screenPageViews", "activeUsers", "sessions", "newUsers",
        "userEngagementDuration", "engagedSessions", "scrolledUsers",
        "eventCount", "bounceRate", "sessionMedium", "sessionCampaign",
        "averageSessionDuration", "totalUsers", "avg_session_min",
        "bounce_rate_pct", "is_new_user", "device_category_fr"
    ]
    for _col in optional_cols:
        if _col not in df.columns:
            df = df.withColumn(_col, F.lit(None))
    df_clean = (df
        .filter(col("report_type").isNotNull())
        .withColumn("report_type",    trim(lower(col("report_type"))))
        .withColumn("deviceCategory", trim(lower(col("deviceCategory"))))
        .withColumn("eventName",      trim(lower(col("eventName"))))
        .withColumn("sessionSource",  trim(lower(col("sessionSource"))))
        .withColumn("pagePath", when(
            col("pagePath").isin("(not set)", "", "(none)"), None
        ).otherwise(col("pagePath")))
        .withColumn("screenPageViews",
            spark_round(coalesce(col("screenPageViews").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("activeUsers",
            spark_round(coalesce(col("activeUsers").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("sessions",
            spark_round(coalesce(col("sessions").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("newUsers",
            spark_round(coalesce(col("newUsers").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("bounceRate",
            spark_round(coalesce(col("bounceRate").cast(DoubleType()), lit(0.0)), 4))
        .withColumn("averageSessionDuration",
            spark_round(coalesce(col("averageSessionDuration").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("eventCount",
            spark_round(coalesce(col("eventCount").cast(DoubleType()), lit(0.0)), 2))
        .withColumn("userEngagementDuration",
            spark_round(coalesce(col("userEngagementDuration").cast(DoubleType()), lit(0.0)), 2))
        # Enrichissement
        .withColumn("device_category_fr", when(col("deviceCategory") == "mobile",  "Mobile")
            .when(col("deviceCategory") == "desktop", "Desktop")
            .when(col("deviceCategory") == "tablet",  "Tablette")
            .otherwise("Autre"))
        .withColumn("is_new_user",    when(col("newUsers") > 0, True).otherwise(False))
        .withColumn("bounce_rate_pct", spark_round(col("bounceRate") * 100, 2))
        .withColumn("avg_session_min", spark_round(col("averageSessionDuration") / 60, 2))
        .withColumn("ga4_date",        to_date(col("ga4_date"), "yyyyMMdd"))
        .withColumn("transformed_at",  current_timestamp())
    )

    dedup_keys = ["report_type", "ga4_date", "pagePath", "sessionSource", "deviceCategory", "eventName"]
    df_dedup = df_clean.dropDuplicates(dedup_keys)
    before = df_clean.count()
    after  = df_dedup.count()
    log.info(f"🔍 Dédup: {before} → {after} lignes (supprimées: {before - after})")
    return df_dedup

def write_silver(df):
    log.info(f"💾 Écriture Silver: {SILVER_PATH}")
    # Cast colonnes VOID en StringType pour Parquet
    from pyspark.sql.types import NullType, StringType
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, df[field.name].cast(StringType()))
    log.info("🔧 Colonnes VOID castées en String")
    (df.write
        .mode("overwrite")
        .partitionBy("report_type", "ga4_date")
        .parquet(SILVER_PATH))
    log.info("✅ Silver écrit")

def validate(spark):
    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    log.info(f"📊 Total Silver: {total} lignes")
    log.info("📋 Par report_type:")
    df.groupBy("report_type").count().orderBy("count", ascending=False).show(truncate=False)
    log.info("📅 Dates disponibles:")
    df.select("ga4_date").distinct().orderBy("ga4_date").show(40, truncate=False)
    log.info("📱 Appareils:")
    df.groupBy("device_category_fr").sum("activeUsers").orderBy("sum(activeUsers)", ascending=False).show()

def main():
    log.info("🚀 FORJA Silver Transform (GA4)")
    spark      = create_spark()
    df_bronze  = read_bronze(spark)
    df_silver  = transform(df_bronze)
    write_silver(df_silver)
    validate(spark)
    log.info("🎉 Silver Transform terminé !")
    spark.stop()

if __name__ == "__main__":
    main()
