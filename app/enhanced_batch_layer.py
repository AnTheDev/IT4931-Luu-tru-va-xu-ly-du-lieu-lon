from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, to_date, when, avg, count, sum as spark_sum,
    split, explode, trim, lit, current_timestamp, round as spark_round,
    desc, asc, first, last, collect_list, collect_set, size, coalesce,
    # Window Functions
    row_number, rank, dense_rank, lag, lead, ntile, percent_rank, cume_dist,
    # Advanced Aggregations
    max as spark_max, min as spark_min, stddev, stddev_pop, variance, var_pop,
    percentile_approx, approx_count_distinct, countDistinct, sumDistinct,
    array_distinct, concat_ws, array_join,
    # Broadcast join
    broadcast,
    # String functions
    regexp_replace, lower, upper, length, substring, initcap,
    # Date functions
    datediff, months_between, date_format, quarter, month, dayofweek, dayofyear,
    add_months, date_add, date_sub, year as spark_year, weekofyear,
    # Conditional
    greatest, least, abs as spark_abs, sqrt, log, log10, exp, pow,
    # Array functions
    array_contains, array_intersect, array_union, flatten, slice,
    # Struct functions
    struct, create_map
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, LongType, ArrayType, FloatType, MapType
)
from pyspark import StorageLevel
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ENVIRONMENT CONFIGURATION
MASTER = os.environ.get("MASTER", "local[*]")
CONNECTION_STRING = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
MONGO_ENABLED = os.environ.get("MONGO_ENABLED", "true").lower() == "true"
CSV_PATH = os.environ.get("CSV_PATH", "/data/tmdb.csv")

# MinIO Configuration
MINIO_ENABLED = os.environ.get("MINIO_ENABLED", "false").lower() == "true"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.bigdata.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")
MINIO_CSV_FILE = os.environ.get("MINIO_CSV_FILE", "tmdb.csv")

# Performance tuning
SHUFFLE_PARTITIONS = int(os.environ.get("SHUFFLE_PARTITIONS", "200"))
BROADCAST_THRESHOLD = int(os.environ.get("BROADCAST_THRESHOLD", "10485760"))  # 10MB

# Spark packages
packages = []
if MONGO_ENABLED:
    packages.append('org.mongodb.spark:mongo-spark-connector_2.12:10.4.0')
if MINIO_ENABLED:
    packages.append('org.apache.hadoop:hadoop-aws:3.3.4')
    packages.append('com.amazonaws:aws-java-sdk-bundle:1.12.500')

print("=" * 80)
print("🚀 ENHANCED BATCH LAYER - LAMBDA ARCHITECTURE")
print("=" * 80)
print(f"📂 Data Source: {'MinIO: ' + MINIO_ENDPOINT if MINIO_ENABLED else 'Local: ' + CSV_PATH}")
print(f"📦 MongoDB: {CONNECTION_STRING} ({'enabled' if MONGO_ENABLED else 'disabled'})")
print(f"🔧 Shuffle Partitions: {SHUFFLE_PARTITIONS}")
print(f"🔧 Broadcast Threshold: {BROADCAST_THRESHOLD} bytes")
print("=" * 80)

# SPARK SESSION WITH OPTIMIZATION
spark_builder = SparkSession.builder \
    .appName("EnhancedBatchLayer-LambdaArchitecture") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.write.connection.uri", CONNECTION_STRING) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
    .config("spark.sql.autoBroadcastJoinThreshold", str(BROADCAST_THRESHOLD)) \
    .config("spark.sql.broadcastTimeout", "600") \
    .config("spark.sql.cbo.enabled", "true") \
    .config("spark.sql.cbo.joinReorder.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

if MINIO_ENABLED:
    spark_builder = spark_builder \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")