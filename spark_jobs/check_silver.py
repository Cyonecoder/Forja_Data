import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_USER     = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASS     = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin123')

spark = (SparkSession.builder
    .appName('CheckSilver')
    .master('spark://spark-master:7077')
    .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key', MINIO_USER)
    .config('spark.hadoop.fs.s3a.secret.key', MINIO_PASS)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider',
            'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .getOrCreate())

df = spark.read.parquet('s3a://forja-datalake/silver/snrt/watchings_enriched/')
print('ROWS=', df.count())
df.printSchema()
df.show(5, truncate=False)

spark.stop()
