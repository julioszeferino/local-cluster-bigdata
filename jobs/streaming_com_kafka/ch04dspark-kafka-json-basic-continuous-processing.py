"""
This code is to read data from Kafka topics in JSON format and print the data to the console.

jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" for readStream Kafka format
schema = declare schema of the topics and create streaming DataFrames for each topic
.config("spark.driver.host", "localhost") = set the driver as localhost
def create_streaming_df(topic, schema) = function to create readStream for each topic

continuous processing = data processed continuously
writeStream custom to trigger every 100 milliseconds

consider experimental feature and may not be stable in some versions
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# TODO Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkContinuousProcessing") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# TODO Define the schema for rides
ride_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("start_location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]), True),
    StructField("end_location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]), True),
    StructField("start_time", LongType(), True),
    StructField("end_time", LongType(), True),
    StructField("fare", DoubleType(), True)
])


# Function to create a streaming DataFrame from a Kafka topic and schema
def create_streaming_df(topic, schema):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "138.197.228.187:9094") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")
    return parsed_df


# TODO Create streaming DataFrame for the rides topic
rides_df = create_streaming_df("src-app-ride-json", ride_schema)

# TODO Start streaming query for the DataFrame in continuous mode and print the data to the console
rides_query = rides_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(continuous="100 milliseconds") \
    .option("truncate", "false") \
    .start()

# TODO  Await termination of the query
rides_query.awaitTermination()
