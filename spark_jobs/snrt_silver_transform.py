import os, logging, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, when, trim, lower,
    round as spark_round, current_timestamp, lit, coalesce,
    to_timestamp, year, month, dayofmonth, regexp_replace
)
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SNRT-SILVER] %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",     "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",    "minioadmin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD","minioadmin123")
BRONZE_BASE    = "s3a://forja-datalake/bronze/snrt"
SILVER_PATH    = "s3a://forja-datalake/silver/snrt"

def create_spark():
    spark = (SparkSession.builder
        .appName("FORJA-Silver-SNRT-Transform")
        .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.executor.memory",                         "512m")
        .config("spark.driver.memory",                           "512m")
        .config("spark.executor.cores",                          "1")
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

# Schemas des tables clés
WATCHINGS_SCHEMA = StructType([
    StructField("id",              IntegerType(), True),
    StructField("user_id",         IntegerType(), True),
    StructField("content_id",      IntegerType(), True),
    StructField("profile_id",      IntegerType(), True),
    StructField("device_id",       StringType(),  True),
    StructField("duration",        StringType(),  True),
    StructField("created_at",      StringType(),  True),
    StructField("updated_at",      StringType(),  True),
    StructField("country",         StringType(),  True),
    StructField("os",              StringType(),  True),
    StructField("os_version",      StringType(),  True),
    StructField("browser",         StringType(),  True),
    StructField("browser_version", StringType(),  True),
    StructField("system_lang",     StringType(),  True),
    StructField("lang",            StringType(),  True),
    StructField("isp",             StringType(),  True),
    StructField("state",           StringType(),  True),
    StructField("city",            StringType(),  True),
])

USERS_SCHEMA = StructType([
    StructField("id",                  IntegerType(), True),
    StructField("email",               StringType(),  True),
    StructField("role",                StringType(),  True),
    StructField("is_active",           BooleanType(), True),
    StructField("subscription_id",     IntegerType(), True),
    StructField("subscription_start",  StringType(),  True),
    StructField("subscription_end",    StringType(),  True),
    StructField("lang",                StringType(),  True),
    StructField("credits",             FloatType(),   True),
    StructField("created_at",          StringType(),  True),
    StructField("updated_at",          StringType(),  True),
])

CONTENTS_SCHEMA = StructType([
    StructField("id",          IntegerType(), True),
    StructField("title",       StringType(),  True),
    StructField("type",        StringType(),  True),
    StructField("duration",    IntegerType(), True),
    StructField("created_at",  StringType(),  True),
    StructField("updated_at",  StringType(),  True),
])

def read_table(spark, table, schema):
    path = f"{BRONZE_BASE}/{table}"
    log.info(f"📖 Lecture {table}...")
    try:
        df_raw = spark.read.parquet(path)
        # Le raw_json contient le JSON de la ligne PostgreSQL
        df = (df_raw
            .select(from_json(col("raw_json"), schema).alias("d"), col("ingested_at"))
            .select("d.*", "ingested_at")
            .dropDuplicates(["id"])
            .filter(col("id").isNotNull())
        )
        count = df.count()
        log.info(f"  ✅ {table}: {count} lignes (après dédup par id)")
        return df
    except Exception as e:
        log.warning(f"  ⚠️ {table} indisponible: {e}")
        return None

def build_watchings_enriched(spark, df_w, df_u, df_c):
    log.info("🔗 Jointure watchings × contents × users...")

    # Nettoyage watchings
    df_w_clean = (df_w
        .withColumn("created_at_ts",  to_timestamp(col("created_at")))
        .withColumn("watch_date",     col("created_at_ts").cast("date"))
        .withColumn("watch_year",     year(col("created_at_ts")))
        .withColumn("watch_month",    month(col("created_at_ts")))
        .withColumn("watch_day",      dayofmonth(col("created_at_ts")))
        .withColumn("duration_sec",
            regexp_replace(col("duration"), "[^0-9]", "").cast(IntegerType()))
        .withColumn("duration_min",
            spark_round(col("duration_sec") / 60, 2))
        .withColumn("country",  trim(col("country")))
        .withColumn("os",       trim(lower(col("os"))))
        .withColumn("browser",  trim(lower(col("browser"))))
        .filter(col("watch_date").isNotNull())
    )

    # Join avec contents
    df_c_slim = df_c.select(
        col("id").alias("c_id"),
        col("title").alias("content_title"),
        col("type").alias("content_type"),
        col("duration").alias("content_duration_sec")
    )
    df_joined = df_w_clean.join(df_c_slim, df_w_clean["content_id"] == df_c_slim["c_id"], "left")

    # Join avec users
    df_u_slim = df_u.select(
        col("id").alias("u_id"),
        col("role").alias("user_role"),
        col("is_active").alias("user_is_active"),
        col("subscription_id").alias("user_subscription_id"),
        col("lang").alias("user_lang"),
        col("created_at").alias("user_created_at")
    )
    df_final = df_joined.join(df_u_slim, df_w_clean["user_id"] == df_u_slim["u_id"], "left")

    # Enrichissement
    df_enriched = (df_final
        .withColumn("is_complete_view", when(
            col("duration_sec").isNotNull() &
            col("content_duration_sec").isNotNull() &
            (col("content_duration_sec") > 0),
            spark_round(col("duration_sec") / col("content_duration_sec") * 100, 1)
        ).otherwise(None))
        .withColumn("device_type", when(col("os").isin("android","ios"), "Mobile")
            .when(col("os").isin("windows","macos","linux","ubuntu"), "Desktop")
            .otherwise("Autre"))
        .withColumn("transformed_at", current_timestamp())
    )

    count = df_enriched.count()
    log.info(f"  ✅ watchings_enriched: {count} lignes")
    return df_enriched

def write_silver_table(df, table_name, partition_cols):
    path = f"{SILVER_PATH}/{table_name}"
    log.info(f"💾 Écriture Silver {table_name} → {path}")
    (df.write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(path))
    log.info(f"  ✅ {table_name} écrit")

def validate(spark):
    log.info("\n📊 VALIDATION SILVER SNRT")
    log.info("="*50)

    df_w = spark.read.parquet(f"{SILVER_PATH}/watchings_enriched")
    total = df_w.count()
    log.info(f"Total watchings enrichis: {total}")

    log.info("🎬 Top 10 contenus les plus vus:")
    df_w.groupBy("content_title", "content_type") \
        .count().orderBy("count", ascending=False).show(10, truncate=40)

    log.info("🌍 Top 10 pays:")
    df_w.filter(col("country").isNotNull()) \
        .groupBy("country").count() \
        .orderBy("count", ascending=False).show(10)

    log.info("📱 Répartition devices:")
    df_w.groupBy("device_type").count() \
        .orderBy("count", ascending=False).show()

    log.info("⏱️  Durée moyenne de visionnage par contenu (top 10):")
    df_w.filter(col("duration_min").isNotNull()) \
        .groupBy("content_title") \
        .avg("duration_min") \
        .withColumnRenamed("avg(duration_min)", "avg_duration_min") \
        .orderBy("avg_duration_min", ascending=False).show(10, truncate=40)

    log.info("📅 Jours avec le plus de watchings:")
    df_w.groupBy("watch_date").count() \
        .orderBy("watch_date").show(40)

def main():
    log.info("🚀 FORJA Silver Transform SNRT")
    spark  = create_spark()

    df_w = read_table(spark, "watchings",  WATCHINGS_SCHEMA)
    df_u = read_table(spark, "users",      USERS_SCHEMA)
    df_c = read_table(spark, "contents",   CONTENTS_SCHEMA)

    if df_w is None:
        log.error("❌ watchings indisponible — abandon")
        spark.stop()
        return

    # Table watchings enrichie (table principale Silver)
    df_enriched = build_watchings_enriched(spark, df_w, df_u, df_c)
    write_silver_table(df_enriched, "watchings_enriched", ["watch_year", "watch_month", "watch_day"])

    # Tables référentiels nettoyées
    if df_u is not None:
        df_u_clean = (df_u
            .withColumn("created_at_ts", to_timestamp(col("created_at")))
            .withColumn("transformed_at", current_timestamp())
        )
        write_silver_table(df_u_clean, "users_clean", ["role"])

    if df_c is not None:
        df_c_clean = (df_c
            .withColumn("transformed_at", current_timestamp())
        )
        write_silver_table(df_c_clean, "contents_clean", ["type"])

    validate(spark)
    log.info("🎉 Silver SNRT terminé !")
    spark.stop()

if __name__ == "__main__":
    main()
