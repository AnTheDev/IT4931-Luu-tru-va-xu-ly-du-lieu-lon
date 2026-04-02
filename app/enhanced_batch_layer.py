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


# CUSTOM USER DEFINED FUNCTIONS (UDFs)
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import ArrayType

@udf(returnType=StringType())
def categorize_rating(rating):
    """UDF: Phân loại phim theo rating với nhiều mức hơn"""
    if rating is None:
        return "Unknown"
    elif rating >= 9.0:
        return "Masterpiece"
    elif rating >= 8.0:
        return "Excellent"
    elif rating >= 7.0:
        return "Good"
    elif rating >= 6.0:
        return "Above Average"
    elif rating >= 5.0:
        return "Average"
    elif rating >= 4.0:
        return "Below Average"
    elif rating >= 3.0:
        return "Poor"
    else:
        return "Very Poor"

@udf(returnType=StringType())
def categorize_budget(budget):
    """UDF: Phân loại phim theo budget chi tiết hơn"""
    if budget is None or budget == 0:
        return "Unknown"
    elif budget >= 200000000:
        return "Mega Blockbuster"
    elif budget >= 100000000:
        return "Blockbuster"
    elif budget >= 50000000:
        return "Big Budget"
    elif budget >= 20000000:
        return "Medium High"
    elif budget >= 10000000:
        return "Medium Budget"
    elif budget >= 5000000:
        return "Low Medium"
    elif budget >= 1000000:
        return "Low Budget"
    else:
        return "Micro Budget"

@udf(returnType=StringType())
def extract_decade(year):
    """UDF: Lấy thập kỷ từ năm"""
    if year is None:
        return "Unknown"
    decade = (year // 10) * 10
    return f"{decade}s"

@udf(returnType=FloatType())
def calculate_roi(revenue, budget):
    """UDF: Tính ROI (Return on Investment)"""
    if budget is None or budget == 0 or revenue is None:
        return None
    return float((revenue - budget) / budget * 100)

@udf(returnType=StringType())
def calculate_profitability_tier(roi):
    """UDF: Phân loại mức độ lợi nhuận"""
    if roi is None:
        return "Unknown"
    elif roi >= 500:
        return "Extremely Profitable"
    elif roi >= 200:
        return "Highly Profitable"
    elif roi >= 100:
        return "Very Profitable"
    elif roi >= 50:
        return "Profitable"
    elif roi >= 0:
        return "Break Even"
    elif roi >= -50:
        return "Minor Loss"
    else:
        return "Major Loss"

@udf(returnType=StringType())
def categorize_runtime(runtime):
    """UDF: Phân loại theo thời lượng phim"""
    if runtime is None or runtime == 0:
        return "Unknown"
    elif runtime < 60:
        return "Short Film"
    elif runtime < 90:
        return "Short Feature"
    elif runtime < 120:
        return "Standard"
    elif runtime < 150:
        return "Long"
    elif runtime < 180:
        return "Epic"
    else:
        return "Extended"

@udf(returnType=StringType())
def categorize_popularity(popularity):
    """UDF: Phân loại theo độ phổ biến"""
    if popularity is None:
        return "Unknown"
    elif popularity >= 100:
        return "Viral"
    elif popularity >= 50:
        return "Trending"
    elif popularity >= 20:
        return "Popular"
    elif popularity >= 10:
        return "Moderate"
    elif popularity >= 5:
        return "Low"
    else:
        return "Niche"

@udf(returnType=IntegerType())
def count_genres(genres_str):
    """UDF: Đếm số lượng genres"""
    if genres_str is None or genres_str == "":
        return 0
    return len([g.strip() for g in genres_str.split(",") if g.strip()])

@udf(returnType=StringType())
def get_primary_genre(genres_str):
    """UDF: Lấy genre chính (đầu tiên)"""
    if genres_str is None or genres_str == "":
        return "Unknown"
    genres = [g.strip() for g in genres_str.split(",") if g.strip()]
    return genres[0] if genres else "Unknown"

# SCHEMA DEFINITION
TMDB_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("budget", LongType(), True),
    StructField("homepage", StringType(), True),
    StructField("tconst", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("production_companies", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("spoken_languages", StringType(), True),
    StructField("keywords", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True),
    StructField("cast", StringType(), True)
])


# LOOKUP TABLES FOR BROADCAST JOINS
def create_genre_lookup():
    """Create genre lookup table for broadcast join"""
    genre_data = [
        ("Action", "High Energy", 1, "action-adventure"),
        ("Adventure", "Exciting", 2, "action-adventure"),
        ("Animation", "Family Friendly", 3, "family"),
        ("Comedy", "Light Entertainment", 4, "comedy"),
        ("Crime", "Dark", 5, "thriller"),
        ("Documentary", "Educational", 6, "documentary"),
        ("Drama", "Emotional", 7, "drama"),
        ("Family", "All Ages", 8, "family"),
        ("Fantasy", "Imaginative", 9, "fantasy-scifi"),
        ("History", "Historical", 10, "drama"),
        ("Horror", "Thrilling", 11, "horror"),
        ("Music", "Musical", 12, "music"),
        ("Mystery", "Intriguing", 13, "thriller"),
        ("Romance", "Love Stories", 14, "romance"),
        ("Science Fiction", "Futuristic", 15, "fantasy-scifi"),
        ("TV Movie", "Television", 16, "other"),
        ("Thriller", "Suspenseful", 17, "thriller"),
        ("War", "Conflict", 18, "drama"),
        ("Western", "American Frontier", 19, "western")
    ]
    return spark.createDataFrame(
        genre_data, 
        ["genre_name", "genre_description", "genre_priority", "genre_category"]
    )

def create_language_lookup():
    """Create language lookup table for broadcast join"""
    language_data = [
        ("en", "English", "Western", True),
        ("es", "Spanish", "Western", True),
        ("fr", "French", "Western", True),
        ("de", "German", "Western", True),
        ("it", "Italian", "Western", True),
        ("pt", "Portuguese", "Western", True),
        ("ja", "Japanese", "Asian", True),
        ("ko", "Korean", "Asian", True),
        ("zh", "Chinese", "Asian", True),
        ("hi", "Hindi", "Asian", True),
        ("ru", "Russian", "Eastern European", True),
        ("ar", "Arabic", "Middle Eastern", True),
    ]
    return spark.createDataFrame(
        language_data,
        ["lang_code", "lang_name", "lang_region", "is_major"]
    )

def create_decade_lookup():
    """Create decade lookup table for analysis"""
    decade_data = [
        ("1970s", "New Hollywood", "Renaissance"),
        ("1980s", "Blockbuster Era", "Commercial"),
        ("1990s", "Indie Rise", "Diverse"),
        ("2000s", "Digital Revolution", "Technological"),
        ("2010s", "Streaming Age", "Modern"),
        ("2020s", "Post-Pandemic", "Hybrid")
    ]
    return spark.createDataFrame(
        decade_data,
        ["decade", "era_name", "era_characteristic"]
    )