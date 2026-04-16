import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("inspect")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT","http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER","minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD","minioadmin123"))
    .config("spark.hadoop.fs.s3a.path.style.access","true")
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("s3a://forja-datalake/silver/snrt/watchings_enriched")
print("=== SCHEMA ===")
df.printSchema()
print("=== SAMPLE 5 lignes ===")
df.show(5, truncate=False)
print("=== COLONNES AVEC NULLS ===")
from pyspark.sql.functions import col, count, when
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
spark.stop()
