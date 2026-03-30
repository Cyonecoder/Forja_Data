from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("check").master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://forja-datalake/bronze/snrt/watchings")
from pyspark.sql.functions import col, concat_ws
df.select("year","month").distinct().orderBy("year","month").show(50)
print(f"Total: {df.count()} lignes")
spark.stop()
