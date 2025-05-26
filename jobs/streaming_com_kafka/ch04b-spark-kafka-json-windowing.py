"""
This code is to read data from Kafka topics in JSON format and show windowing operations.

jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" for readStream Kafka format
schema = declare schema of the topics and create streaming DataFrames for each topic
.config("spark.driver.host", "localhost") = set the driver as localhost
def create_streaming_df(topic, schema) = function to create readStream for each topic

tumbling window = fixed-size window that does not overlap
session window = window that groups events based on a session key
sliding window = window that slides over the data at regular intervals
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, session_window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# TODO Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkRead") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# TODO you could read a data file from HDFS or S3 to get the schema of the data.

# TODO Define the schema for customers
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True)
])

# TODO Define the schema for drivers
driver_schema = StructType([
    StructField("driver_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("rating", DoubleType(), True)
])

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


# TODO Function to create a streaming DataFrame from a Kafka topic and schema
def create_streaming_df(topic, schema):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "138.197.228.187:9094") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value", "timestamp as event_time")
    parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data"), col("event_time")).select("data.*", "event_time")
    return parsed_df

# TODO Create streaming DataFrames for each topic
customers_df = create_streaming_df("src-app-customers-json", customer_schema)
drivers_df = create_streaming_df("src-app-drivers-json", driver_schema)
rides_df = create_streaming_df("src-app-ride-json", ride_schema)

# TODO Sliding window aggregation for customers
customer_agg_df = customers_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "5 minutes", "2 minutes"), col("customer_id")) \
    .count() \
    .withColumnRenamed("count", "activity_count")\
    .withColumnRenamed("window", "sliding_window")

# TODO Session window aggregation for drivers
driver_agg_df = drivers_df \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(session_window(col("event_time"), "10 minutes"), col("driver_id")) \
    .count() \
    .withColumnRenamed("count", "session_count")

# TODO Tumbling window aggregation for rides
ride_agg_df = rides_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("driver_id")) \
    .agg({
        "fare": "sum",
        "ride_id": "count"
    }) \
    .withColumnRenamed("sum(fare)", "total_fare") \
    .withColumnRenamed("count(ride_id)", "ride_count") \
    .withColumnRenamed("window", "tumbling_window")

# TODO Start streaming queries for each DataFrame and print the data to the console
customer_query = customer_agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

driver_query = driver_agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

ride_query = ride_agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# TODO Await termination of all queries
spark.streams.awaitAnyTermination()
