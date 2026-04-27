import os, time, logging, json
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth, lit, max as spark_max
from pyspark.sql.types import StringType
from kafka import KafkaConsumer as KConsumer
from kafka.structs import TopicPartition

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SNRT-BRONZE] %(levelname)s - %(message)s')
log = logging.getLogger('SNRT-BRONZE')

KAFKA_SERVERS  = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_USER     = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASS     = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin123')
BRONZE_BASE    = 's3a://forja-datalake/bronze/snrt'
BUCKET         = 'forja-datalake'
INTERVAL       = 120
BATCH_SIZE     = 50000
NUM_PARTITIONS = 3

TOPICS = {
    'snrt-watchings':          'watchings',
    'snrt-users':              'users',
    'snrt-profiles':           'profiles',
    'snrt-contents':           'contents',
    'snrt-subscriptions':      'subscriptions',
    'snrt-user-subscriptions': 'user_subscriptions',
    'snrt-programs':           'programs',
    'snrt-categories':         'categories',
    'snrt-user-fav':           'user_fav',
    'snrt-user-like':          'user_like',
}

def get_s3():
    return boto3.client('s3', endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER, aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version='s3v4'), region_name='us-east-1')

def get_kafka_bounds(topic):
    c = KConsumer(bootstrap_servers=KAFKA_SERVERS)
    tps = [TopicPartition(topic, p) for p in range(NUM_PARTITIONS)]
    ends = c.end_offsets(tps)
    begins = c.beginning_offsets(tps)
    c.close()
    return {tp.partition: begins[tp] for tp in tps}, {tp.partition: ends[tp] for tp in tps}

def load_offsets(s3, topic):
    offsets = {}
    for pid in range(NUM_PARTITIONS):
        key = 'offsets/snrt_{}_p{}.txt'.format(topic.replace('-','_'), pid)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            offsets[pid] = int(obj['Body'].read().decode().strip())
        except:
            offsets[pid] = 0
    return offsets

def save_offsets(s3, topic, offsets):
    for pid, offset in offsets.items():
        key = 'offsets/snrt_{}_p{}.txt'.format(topic.replace('-','_'), pid)
        s3.put_object(Bucket=BUCKET, Key=key, Body=str(offset).encode())

def build_spark():
    return SparkSession.builder.appName('SNRT-Bronze-Consumer') \
        .config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT) \
        .config('spark.hadoop.fs.s3a.access.key', MINIO_USER) \
        .config('spark.hadoop.fs.s3a.secret.key', MINIO_PASS) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.sql.legacy.parquet.nanosAsLong', 'true') \
        .config('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED') \
        .getOrCreate()

def run_batch(spark, s3, topic, table_name, offsets):
    # Verifier les bornes reelles du topic
    begins, ends = get_kafka_bounds(topic)
    # Skip si topic vide
    if all(ends[p] == 0 for p in range(NUM_PARTITIONS)):
        log.info('  [{}] topic vide - skip'.format(table_name))
        return offsets
    # Construire starting/ending en respectant les bornes reelles
    starting = {}
    ending = {}
    has_data = False
    for pid in range(NUM_PARTITIONS):
        begin = begins[pid]
        end = ends[pid]
        current = max(offsets[pid], begin)  # jamais en dessous du begin
        if current >= end:
            starting[str(pid)] = current
            ending[str(pid)] = current  # rien a lire
        else:
            starting[str(pid)] = current
            ending[str(pid)] = min(current + BATCH_SIZE, end)
            has_data = True
    if not has_data:
        log.info('  [{}] a jour - skip'.format(table_name))
        return offsets
    starting_json = json.dumps({topic: starting})
    ending_json   = json.dumps({topic: ending})
    df = spark.read \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_SERVERS) \
        .option('subscribe', topic) \
        .option('startingOffsets', starting_json) \
        .option('endingOffsets',   ending_json) \
        .option('failOnDataLoss',  'false') \
        .load()
    count = df.count()
    if count == 0:
        log.info('  [{}] 0 lignes'.format(table_name))
        return offsets
    df_out = df.select(
        col('value').cast(StringType()).alias('raw_json'),
        col('key').cast(StringType()).alias('kafka_key'),
        col('timestamp').cast('long').alias('kafka_timestamp'),
        col('partition').cast('long').alias('kafka_partition'),
        col('offset').cast('long').alias('kafka_offset'),
        current_timestamp().alias('ingested_at'),
        lit(table_name).alias('table_name')
    ).withColumn('year',  year(current_timestamp())) \
     .withColumn('month', month(current_timestamp())) \
     .withColumn('day',   dayofmonth(current_timestamp()))
    output_path = '{}/{}'.format(BRONZE_BASE, table_name)
    df_out.write.mode('append').partitionBy('year','month','day').parquet(output_path)
    max_off_rows = df.groupBy('partition').agg(spark_max('offset').alias('m')).collect()
    new_offsets = dict(offsets)
    for row in max_off_rows:
        new_offsets[row['partition']] = row['m'] + 1
    log.info('  OK [{}] {} lignes | offsets: {}'.format(table_name, count, new_offsets))
    return new_offsets

def main():
    log.info('SNRT-Bronze demarre (multi-partition + bornes protegees)')
    spark = build_spark()
    spark.sparkContext.setLogLevel('WARN')
    s3 = get_s3()
    offsets = {topic: load_offsets(s3, topic) for topic in TOPICS}
    log.info('Offsets charges')
    while True:
        log.info('Debut du batch...')
        for topic, table_name in TOPICS.items():
            try:
                new_off = run_batch(spark, s3, topic, table_name, offsets[topic])
                if new_off != offsets[topic]:
                    save_offsets(s3, topic, new_off)
                    offsets[topic] = new_off
            except Exception as e:
                log.error('[{}]: {}'.format(table_name, e))
        log.info('Prochain batch dans {}s...'.format(INTERVAL))
        time.sleep(INTERVAL)

if __name__ == '__main__':
    main()