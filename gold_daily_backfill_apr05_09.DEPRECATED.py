from pyspark.sql import SparkSession, functions as F
import psycopg2

PG_HOST = "169.255.179.24"
PG_PORT = "5432"
PG_DB = "snrt_stats"
PG_USER = "snrt_readonly"
PG_PASSWORD = "6AOd3Dm2"

START_DATE = "2026-04-05"
END_DATE = "2026-04-09"

spark = (
    SparkSession.builder
    .appName("gold-daily-backfill-apr05-09")
    .config("spark.jars", "/opt/spark_jobs/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark_jobs/jars/hadoop-aws-3.3.4.jar,/opt/spark_jobs/jars/postgresql-42.6.0.jar")
    .getOrCreate()
)

df = spark.read.parquet("s3a://forja-datalake/silver/snrt/watchings_enriched/")

cols = set(df.columns)

title_col = None
if "content_title" in cols:
    title_col = F.when(
        F.col("content_title").cast("string").contains('"fr"'),
        F.regexp_extract(F.col("content_title").cast("string"), '"fr"\\s*:\\s*"([^"]+)"', 1)
    ).otherwise(F.col("content_title").cast("string"))
else:
    title_col = F.col("content_id").cast("string")

content_type_col = F.col("content_type") if "content_type" in cols else F.lit("unknown")
watch_date_col = F.col("watch_date").cast("date")
user_col = F.col("user_id").cast("string") if "user_id" in cols else F.lit(None)
duration_sec_col = F.col("duration_sec").cast("double") if "duration_sec" in cols else F.lit(0.0)
duration_min_col = (
    F.col("duration_min").cast("double")
    if "duration_min" in cols
    else (duration_sec_col / F.lit(60.0))
)
completion_col = F.col("completion_rate").cast("double") if "completion_rate" in cols else F.lit(0.0)
country_col = F.col("country") if "country" in cols else F.lit(None)
city_col = F.col("city") if "city" in cols else F.lit(None)
os_col = F.col("os") if "os" in cols else F.lit(None)
browser_col = F.col("browser") if "browser" in cols else F.lit(None)

base = (
    df.filter((watch_date_col >= F.lit(START_DATE)) & (watch_date_col <= F.lit(END_DATE)))
      .withColumn("content_title_safe", F.coalesce(F.nullif(F.trim(title_col), F.lit("")), F.col("content_id").cast("string")))
      .withColumn("content_type_safe", content_type_col)
      .withColumn("report_date", watch_date_col)
      .withColumn("duration_min_safe", F.coalesce(duration_min_col, F.lit(0.0)))
      .withColumn("watch_time_hours", F.coalesce(duration_sec_col, F.lit(0.0)) / F.lit(3600.0))
      .withColumn("completion_rate_safe", F.coalesce(completion_col, F.lit(0.0)))
      .withColumn("country_safe", country_col)
      .withColumn("city_safe", city_col)
      .withColumn("os_safe", os_col)
      .withColumn("browser_safe", browser_col)
)

agg = (
    base.groupBy("content_title_safe", "content_type_safe", "report_date")
        .agg(
            F.count("*").alias("total_views"),
            F.countDistinct("user_id").alias("total_users"),
            F.round(F.avg("duration_min_safe"), 2).alias("avg_duration_min"),
            F.round(F.sum("watch_time_hours"), 2).alias("watch_time_total"),
            F.round(F.avg("completion_rate_safe"), 2).alias("completion_rate"),
            F.first("country_safe", ignorenulls=True).alias("top_country"),
            F.first("city_safe", ignorenulls=True).alias("top_city"),
            F.first("os_safe", ignorenulls=True).alias("top_os"),
            F.first("browser_safe", ignorenulls=True).alias("top_browser"),
        )
        .select(
            F.col("content_title_safe").alias("content_title"),
            F.col("content_type_safe").alias("content_type"),
            "report_date",
            "total_views",
            "total_users",
            "avg_duration_min",
            "watch_time_total",
            "completion_rate",
            "top_country",
            "top_city",
            "top_os",
            "top_browser",
        )
)

rows = agg.count()
print(f"ROWS_TO_WRITE={rows}")

conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("""
DELETE FROM gold_daily_content_performance
WHERE report_date BETWEEN DATE '2026-04-05' AND DATE '2026-04-09'
""")
cur.close()
conn.close()

(
    agg.write
      .format("jdbc")
      .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
      .option("dbtable", "gold_daily_content_performance")
      .option("user", PG_USER)
      .option("password", PG_PASSWORD)
      .option("driver", "org.postgresql.Driver")
      .mode("append")
      .save()
)

spark.stop()
