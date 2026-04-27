import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("count").master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

BASE = "s3a://forja-datalake/bronze/snrt"
tables = ["watchings","contents","users","categories","profiles","subscriptions","user_fav","user_like"]
for t in tables:
    try:
        n = spark.read.parquet(f"{BASE}/{t}").count()
        print(f"✅ {t}: {n} lignes")
    except Exception as e:
        print(f"❌ {t}: {e}")
spark.stop()
