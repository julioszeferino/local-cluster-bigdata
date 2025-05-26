"""
This code is to read data from Kafka topics in JSON format and join two streaming DataFrames.

jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" for readStream Kafka format
schema = declare schema of the topics and create streaming DataFrames for each topic
.config("spark.driver.host", "localhost") = set the driver as localhost
def create_streaming_df(topic, schema) = function to create readStream for each topic

Fraud detection logic: detect high number of rides within a short period for customers
and detect the same driver having more than one ride within the same event time
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

# TODO Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkFraudDetection") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,") \
    .getOrCreate()

# TODO Define the schema for customers
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True)
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

# TODO Define the schema for drivers
driver_schema = StructType([
    StructField("driver_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("rating", DoubleType(), True)
])

# Function to create a streaming DataFrame from a Kafka topic and schema
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
rides_df = create_streaming_df("src-app-ride-json", ride_schema)
drivers_df = create_streaming_df("src-app-drivers-json", driver_schema)

# TODO Join rides with customers and drivers
# TODO project lightspeed
joined_df = rides_df \
    .join(customers_df, rides_df.customer_id == customers_df.customer_id, "inner") \
    .join(drivers_df, rides_df.driver_id == drivers_df.driver_id, "inner") \
    .select(
        rides_df.ride_id,
        rides_df.driver_id,
        rides_df.customer_id,
        rides_df.start_location,
        rides_df.end_location,
        rides_df.start_time,
        rides_df.end_time,
        rides_df.fare,
        rides_df.event_time,
        customers_df.name.alias("customer_name"),
        customers_df.email,
        customers_df.phone_number,
        customers_df.address,
        drivers_df.name.alias("driver_name"),
        drivers_df.license_number,
        drivers_df.rating
    )

# TODO Apply tumbling window aggregation for customer fraud detection
customer_agg_df = joined_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("customer_id")) \
    .agg({
        "fare": "sum",
        "ride_id": "count"
    }) \
    .withColumnRenamed("sum(fare)", "total_fare") \
    .withColumnRenamed("count(ride_id)", "ride_count")

# TODO Customer fraud detection logic (example: detect high number of rides within a short period)
customer_fraud_df = customer_agg_df.filter(col("ride_count") > 5)

# TODO Apply tumbling window aggregation for driver fraud detection
driver_agg_df = joined_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute"), col("driver_id")) \
    .agg(
        count("ride_id").alias("ride_count"),
        collect_list("customer_id").alias("customer_ids")
    )

# TODO Driver fraud detection logic (example: detect the same driver having more than one ride within the same event time)
driver_fraud_df = driver_agg_df.filter(col("ride_count") > 1)

# TODO Start streaming queries and output the results to the console
customer_query = customer_fraud_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

driver_query = driver_fraud_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()
