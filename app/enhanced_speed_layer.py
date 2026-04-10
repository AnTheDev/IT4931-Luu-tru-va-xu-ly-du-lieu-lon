#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

import os
from dotenv import load_dotenv

load_dotenv()

# =====================================
# ENVIRONMENT CONFIGURATION
# =====================================

MASTER = os.environ.get("MASTER", "local[*]")
KAFKA_BROKER1 = os.environ.get("KAFKA_BROKER1", "localhost:9092")
MOVIE_TOPIC = os.environ.get("MOVIE_TOPIC", "movie")

print("=" * 60)
print("⚡ SPEED LAYER SETUP")
print("=" * 60)
print(f"Kafka Broker : {KAFKA_BROKER1}")
print(f"Kafka Topic  : {MOVIE_TOPIC}")
print("=" * 60)

# =====================================
# SPARK SESSION
# =====================================

spark = (
    SparkSession.builder
    .appName("SpeedLayerSetup")
    .master(MASTER)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
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

# =====================================
# MAIN
# =====================================

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
        .select("movie.*", "kafka_timestamp")
    )

    query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    print("\n🚀 Speed Layer Started")
    print("Waiting for Kafka messages...")

    query.awaitTermination()


if __name__ == "__main__":
    try:
        run_speed_layer()
    except KeyboardInterrupt:
        print("\nStopping Speed Layer...")
    finally:
        spark.stop()