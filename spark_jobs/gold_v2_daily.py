import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Paramètre date (défaut = hier) ──────────────────────────────────────────
TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
print(f"[GOLD V2] Traitement du {TARGET_DATE}")

# ── Paramètres Postgres / MinIO (fixes et simples) ──────────────────────────
PG_HOST = "forja_postgres"
PG_PORT = "5432"
PG_DB   = "snrt_stats"
PG_USER = "snrt_readonly"
PG_PASS = os.getenv("POSTGRES_PASSWORD", "6AOd3Dm2")  # fallback propre

print("[GOLD V2] Postgres JDBC URL :",
      f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://forja_minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")
SILVER_PATH    = "s3a://forja-datalake/silver/snrt/watchings_enriched/"

# ── Session Spark ────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName(f"gold_v2_daily_{TARGET_DATE}") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

JDBC_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROP = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

try:
    # ── Lecture Silver filtrée sur TARGET_DATE ───────────────────────────────
    df_raw = spark.read.parquet(SILVER_PATH)
    df = df_raw.withColumn("watched_date", F.to_date(F.col("watched_at"))) \
               .filter(F.col("watched_date") == F.lit(TARGET_DATE))

    count_silver = df.count()
    print(f"[GOLD V2] Lignes Silver pour {TARGET_DATE} : {count_silver}")

    if count_silver == 0:
        print(f"[GOLD V2] Aucune donnée Silver pour {TARGET_DATE} → on arrête proprement.")
        spark.stop()
        sys.exit(0)

    # ── Lecture contents (hiérarchie) via JDBC ───────────────────────────────
    df_contents = spark.read \
        .format("jdbc") \
        .option("url",      JDBC_URL) \
        .option("dbtable",  "(SELECT id::text, slug, type, program_id FROM contents) AS c") \
        .option("user",     PG_USER) \
        .option("password", PG_PASS) \
        .option("driver",   "org.postgresql.Driver") \
        .load()

    df_parent = df_contents.select(
        F.col("id").alias("parent_id"),
        F.col("slug").alias("season_slug"),
        F.col("program_id").alias("grandparent_id")
    )
    df_grandparent = df_contents.select(
        F.col("id").alias("grandparent_id2"),
        F.col("slug").alias("series_slug")
    )

    df_hier = df_contents \
        .join(df_parent,       df_contents["program_id"] == df_parent["parent_id"],       "left") \
        .join(df_grandparent,  df_parent["grandparent_id"] == df_grandparent["grandparent_id2"], "left") \
        .select(
            df_contents["id"].alias("cid"),
            df_contents["slug"].alias("episode_slug"),
            df_parent["season_slug"],
            df_grandparent["series_slug"]
        )

    # ── Join Silver + hiérarchie ─────────────────────────────────────────────
    df = df.join(df_hier, df["content_id"] == df_hier["cid"], "left")

    # ── Agrégations gold_v2_daily_content ────────────────────────────────────
    MAX_DURATION = 600.0

    df_agg = df \
        .withColumn("watch_dur_clean", F.least(F.col("watch_duration").cast("double"), F.lit(MAX_DURATION))) \
        .groupBy("watched_date", "content_id", "content_title", "content_type",
                 "series_slug", "season_slug", "episode_slug", "cat_name") \
        .agg(
            F.count("w_id").alias("total_views"),
            F.countDistinct("user_id").alias("total_users"),
            (F.sum("watch_dur_clean") / F.count("w_id") / 60.0).alias("avg_duration_min"),
            (F.sum("watch_dur_clean") / 60.0).alias("watch_time_total"),
            F.first(F.col("country"), ignorenulls=True).alias("top_country"),
            F.first(F.col("os"),      ignorenulls=True).alias("top_os")
        ) \
        .withColumn("completion_rate", F.lit(0.0)) \
        .withColumn("report_date", F.col("watched_date"))

    rows_content = df_agg.count()
    print(f"[GOLD V2] Lignes gold_v2_daily_content : {rows_content}")

    # ── Agrégation gold_v2_daily_users ───────────────────────────────────────
    df_users_day = df.groupBy("watched_date").agg(
        F.countDistinct("user_id").alias("active_users"),
        F.first(F.col("country"), ignorenulls=True).alias("top_country"),
        F.first(F.col("os"),      ignorenulls=True).alias("top_os")
    )

    # ── Agrégation gold_v2_daily_actions ─────────────────────────────────────
    df_actions = df.withColumn("action_type", F.lit("watching")) \
        .groupBy("watched_date", "action_type") \
        .agg(
            F.count("w_id").alias("total_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.first("content_title", ignorenulls=True).alias("top_content"),
            F.first("country",       ignorenulls=True).alias("top_country")
        )

    # ── Suppression de la date cible puis INSERT (idempotent) ────────────────
    # On utilise ici une commande SQL simple via JDBC (Spark)
    delete_sql = f"DELETE FROM gold_v2_daily_content WHERE report_date = DATE '{TARGET_DATE}'"
    spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", delete_sql) \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    delete_sql2 = f"DELETE FROM gold_v2_daily_users   WHERE report_date = DATE '{TARGET_DATE}'"
    spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", delete_sql2) \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    delete_sql3 = f"DELETE FROM gold_v2_daily_actions WHERE report_date = DATE '{TARGET_DATE}'"
    spark.read.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("query", delete_sql3) \
        .option("user", PG_USER) \
        .option("password", PG_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # INSERT via Spark JDBC
    df_agg.select(
        F.col("report_date"),
        F.col("content_id"),
        F.col("content_title"),
        F.col("content_type"),
        F.col("series_slug"),
        F.col("season_slug"),
        F.col("episode_slug"),
        F.col("cat_name"),
        F.col("total_views"),
        F.col("total_users"),
        F.col("avg_duration_min"),
        F.col("watch_time_total"),
        F.col("completion_rate"),
        F.col("top_country"),
        F.col("top_os")
    ).write.jdbc(JDBC_URL, "gold_v2_daily_content", mode="append", properties=JDBC_PROP)

    df_users_day.select(
        F.col("watched_date").alias("report_date"),
        F.lit(0).alias("new_users"),
        F.col("active_users"),
        F.lit(0).alias("total_users_cumul"),
        F.col("top_country"),
        F.col("top_os")
    ).write.jdbc(JDBC_URL, "gold_v2_daily_users", mode="append", properties=JDBC_PROP)

    df_actions.select(
        F.col("watched_date").alias("report_date"),
        F.col("action_type"),
        F.col("total_count"),
        F.col("unique_users"),
        F.col("top_content"),
        F.col("top_country")
    ).write.jdbc(JDBC_URL, "gold_v2_daily_actions", mode="append", properties=JDBC_PROP)

    print(f"[GOLD V2] ✅ Done {TARGET_DATE} — {rows_content} lignes contenu écrites")

except Exception as e:
    print(f"[GOLD V2] ❌ Erreur : {e}")
    spark.stop()
    sys.exit(1)

spark.stop()
