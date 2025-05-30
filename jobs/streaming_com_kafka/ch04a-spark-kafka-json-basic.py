"""
This code is to read data from Kafka topics in JSON format and print the data to the console.

jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" for readStream Kafka format
schema = declare schema of the topics and create streaming DataFrames for each topic
.config("spark.driver.host", "localhost") = set the driver as localhost
def create_streaming_df(topic, schema) = function to create readStream for each topic
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# TODO Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkRead") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

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


# Function to create a streaming DataFrame from a Kafka topic and schema
def create_streaming_df(topic, schema):
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "138.197.228.187:9094") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # TODO use avro & schema registry
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    parsed_df = value_df.select(from_json(col("json_value"), schema).alias("data")).select("data.*")
    return parsed_df


# TODO  Create streaming DataFrames for each topic
customers_df = create_streaming_df("src-app-customers-json", customer_schema)
drivers_df = create_streaming_df("src-app-drivers-json", driver_schema)
rides_df = create_streaming_df("src-app-ride-json", ride_schema)

# TODO  Start streaming queries for each DataFrame and print the data to the console
customers_query = customers_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

drivers_query = drivers_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

rides_query = rides_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# TODO  Await termination of all queries
spark.streams.awaitAnyTermination()
