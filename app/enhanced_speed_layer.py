#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    lit,
    year,
    to_date,
    avg,
    count,
    desc,
    round as spark_round,
    window,
    max as spark_max,
    min as spark_min,
    stddev
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType
)
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import udf
from pyspark.sql.functions import to_timestamp
import os
from dotenv import load_dotenv

load_dotenv()

# =====================================
# ENVIRONMENT CONFIGURATION
# =====================================

MASTER = os.environ.get("MASTER", "local[*]")
KAFKA_BROKER1 = os.environ.get("KAFKA_BROKER1", "localhost:9092")
MOVIE_TOPIC = os.environ.get("MOVIE_TOPIC", "movie")

CONNECTION_STRING = os.environ.get(
    "CONNECTION_STRING",
    "mongodb://localhost:27017"
)

MONGO_ENABLED = (
    os.environ.get("MONGO_ENABLED", "true")
    .lower() == "true"
)

print("=" * 60)
print("⚡ SPEED LAYER - ANALYTICS")
print("=" * 60)
print(f"Kafka Broker : {KAFKA_BROKER1}")
print(f"Kafka Topic  : {MOVIE_TOPIC}")
print("=" * 60)

# =====================================
# SPARK SESSION
# =====================================

spark = (
    SparkSession.builder
    .appName("SpeedLayerMicroBatch")
    .master(MASTER)
    .config(
    "spark.jars.packages",
    ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
    ])
)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =====================================
# STREAM SCHEMA
# =====================================

MOVIE_STREAM_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("vote_average", StringType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("popularity", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", StringType(), True),
    StructField("budget", StringType(), True),
    StructField("runtime", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("adult", BooleanType(), True),
    StructField("production_companies", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

#UDF
@udf(returnType=FloatType())
def safe_float_convert(value):
    try:
        return float(value)
    except:
        return None


@udf(returnType=StringType())
def categorize_popularity_stream(popularity):

    if popularity is None:
        return "Unknown"

    try:
        pop = float(popularity)

        if pop >= 100:
            return "Viral"
        elif pop >= 50:
            return "Trending"
        elif pop >= 20:
            return "Popular"
        elif pop >= 10:
            return "Moderate"
        else:
            return "Niche"

    except:
        return "Unknown"


@udf(returnType=StringType())
def categorize_rating_stream(rating):

    if rating is None:
        return "Unknown"

    try:
        r = float(rating)

        if r >= 8:
            return "Excellent"
        elif r >= 7:
            return "Good"
        elif r >= 5:
            return "Average"
        elif r >= 3:
            return "Below Average"
        else:
            return "Poor"

    except:
        return "Unknown"

def save_to_mongodb(df, collection_name):

    if not MONGO_ENABLED:
        return

    try:

        df.write \
            .format("mongodb") \
            .option(
                "connection.uri",
                CONNECTION_STRING
            ) \
            .option(
                "database",
                "BIGDATA"
            ) \
            .option(
                "collection",
                collection_name
            ) \
            .mode("append") \
            .save()

        print(
            f"Saved to MongoDB: {collection_name}"
        )

    except Exception as e:

        print(
            f"MongoDB error: {str(e)}"
        )

def process_micro_batch(df, epoch_id):

    if df.rdd.isEmpty():
        return

    print(
        f"\nProcessing epoch {epoch_id}"
    )

    processed_df = (
        df
        .withColumn(
            "vote_average",
            safe_float_convert(
                col("vote_average")
            )
        )
        .withColumn(
            "popularity",
            safe_float_convert(
                col("popularity")
            )
        )
        .withColumn(
            "release_year",
            year(
                to_date(
                    col("release_date")
                )
            )
        )
        .withColumn(
            "rating_category",
            categorize_rating_stream(
                col("vote_average")
            )
        )
        .withColumn(
            "popularity_category",
            categorize_popularity_stream(
                col("popularity")
            )
        )
        .withColumn(
            "processed_at",
            current_timestamp()
        )
        .withColumn(
            "layer",
            lit("speed")
        )
    )

    processed_df.show(
        truncate=False
    )

    save_to_mongodb(
    processed_df,
    "speed_movies"
)

    compute_realtime_aggregations(
    processed_df,
    epoch_id
)

def compute_realtime_aggregations(df, epoch_id):

    print(
        f"Computing analytics for epoch {epoch_id}"
    )

    # =====================
    # Genre Statistics
    # =====================

    genre_stats = (
        df
        .groupBy("genres")
        .agg(
            count("*").alias("movie_count"),
            spark_round(
                avg("vote_average"),
                2
            ).alias("avg_rating"),
            spark_round(
                avg("popularity"),
                2
            ).alias("avg_popularity")
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
    )

    save_to_mongodb(
        genre_stats,
        "speed_genre_stats"
    )

    # =====================
    # Release Year Statistics
    # =====================

    year_stats = (
        df
        .groupBy("release_year")
        .agg(
            count("*").alias("movie_count"),
            spark_round(
                avg("vote_average"),
                2
            ).alias("avg_rating")
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
    )

    save_to_mongodb(
        year_stats,
        "speed_year_stats"
    )

    # =====================
    # Language Statistics
    # =====================

    language_stats = (
        df
        .groupBy("original_language")
        .agg(
            count("*").alias("movie_count"),
            spark_round(
                avg("vote_average"),
                2
            ).alias("avg_rating")
        )
        .withColumn(
            "updated_at",
            current_timestamp()
        )
    )

    save_to_mongodb(
        language_stats,
        "speed_language_stats"
    )

    # =====================
    # Top Movies
    # =====================

    top_movies = (
        df
        .orderBy(
            desc("popularity")
        )
        .limit(100)
    )

    save_to_mongodb(
        top_movies,
        "speed_top_movies"
    )

    print(
        f"Analytics completed for epoch {epoch_id}"
    )
# =====================================
# MAIN
# =====================================
def create_window_aggregation_query(df):

    genre_windowed = (
        df
        .groupBy(
            window(
                col("event_time"),
                "5 minutes"
            ),
            col("genres")
        )
        .agg(
            count("*").alias("movie_count"),
            spark_round(
                avg("vote_average"),
                2
            ).alias("avg_rating"),
            spark_round(
                avg("popularity"),
                2
            ).alias("avg_popularity")
        )
    )

    return genre_windowed

def create_trending_analysis_query(df):

    trending_df = (
        df
        .groupBy(
            window(
                col("event_time"),
                "10 minutes",
                "2 minutes"
            )
        )
        .agg(
            count("*").alias("total_movies"),

            spark_round(
                avg("popularity"),
                2
            ).alias("avg_popularity"),

            spark_max("popularity")
                .alias("max_popularity"),

            spark_min("popularity")
                .alias("min_popularity"),

            stddev("popularity")
                .alias("popularity_stddev")
        )
    )

    return trending_df


def run_speed_layer():
    print("\n📡 Connecting to Kafka...")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER1)
        .option("subscribe", MOVIE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    print("✅ Kafka connected")

    parsed_df = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) as json",
            "timestamp as kafka_timestamp"
        )
        .select(
            from_json(
                col("json"),
                MOVIE_STREAM_SCHEMA
            ).alias("movie"),
            col("kafka_timestamp")
        )
        .select("movie.*", "kafka_timestamp") \
.withColumn(
    "event_time",
      to_timestamp(col("event_time"))
)
    )

    window_df = create_window_aggregation_query(
    parsed_df
)
    trending_df = create_trending_analysis_query(
    parsed_df
)

    query1 = (
    parsed_df.writeStream
    .foreachBatch(
        process_micro_batch
    )
    .outputMode("append")
    .start()
)
    query2 = (
    window_df.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)
    query3 = (
    trending_df.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

    print("\n🚀 Speed Layer Started")
    print("Waiting for Kafka messages...")

    query1.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()


if __name__ == "__main__":
    try:
        run_speed_layer()
    except KeyboardInterrupt:
        print("\nStopping Speed Layer...")
    finally:
        spark.stop()