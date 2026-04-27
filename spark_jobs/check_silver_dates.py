import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as smin, max as smax

spark = (SparkSession.builder.appName('CheckSilverDates')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin123')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    .getOrCreate())
df = spark.read.parquet('s3a://forja-datalake/silver/snrt/watchings_enriched/')
res = df.agg(smin('watch_date').alias('min'), smax('watch_date').alias('max')).collect()[0]
print('SILVER_MIN =', res['min'])
print('SILVER_MAX =', res['max'])
spark.stop()
