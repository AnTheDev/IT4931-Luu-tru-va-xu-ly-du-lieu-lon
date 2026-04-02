from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, to_date, when, expr
from schema import MOVIE_SCHEMA
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
MOVIE_TOPIC = os.environ["MOVIE_TOPIC"]
MASTER = os.environ["MASTER"]

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
]

spark = SparkSession.builder \
    .appName("MovieConsumer") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", MOVIE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) AS json") \
           .select(from_json(col("json"), MOVIE_SCHEMA).alias("data")) \
           .select("data.*")

final_df = parsed \
    .withColumn("popularity", col("popularity").cast("double")) \
    .withColumn("vote_average", col("vote_average").cast("double")) \
    .withColumn("budget", col("budget").cast("double")) \
    .withColumn("revenue", col("revenue").cast("double")) \
    .withColumn("runtime", col("runtime").cast("double")) \
    .withColumn("genres", expr("transform(genres, g -> g.name)")) \
    .withColumn("production_companies", expr("transform(production_companies, c -> c.name)")) \
    .withColumn("production_countries", expr("transform(production_countries, c -> c.name)")) \
    .withColumn("release_year", year(to_date("release_date", "yyyy-MM-dd"))) \
    .withColumn("profit_ratio", when(col("budget") > 0, (col("revenue") - col("budget")) / col("budget")).otherwise(None))