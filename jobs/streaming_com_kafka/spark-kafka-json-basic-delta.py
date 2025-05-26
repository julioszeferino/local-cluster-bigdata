"""
This code reads data from Kafka topics in JSON format and writes the data to a Delta table in a MinIO S3 bucket.

jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" for readStream Kafka format
schema = declare schema of the topics and create streaming DataFrames for each topic
.config("spark.driver.host", "localhost") = set the driver as localhost
def create_streaming_df(topic, schema) = function to create readStream for each topic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize Spark session with Kafka and Delta packages
spark = SparkSession.builder \
    .appName("KafkaBasic") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://138.197.224.4") \
    .config("spark.hadoop.fs.s3a.access.key", "data-lake") \
    .config("spark.hadoop.fs.s3a.secret.key", "12620ee6-2162-11ee-be56-0242ac120002") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.fast.upload", True) \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
    .config("fs.s3a.connection.maximum", 100) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# Define schemas for the topics
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address", StringType(), True)
])

driver_schema = StructType([
    StructField("driver_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("rating", DoubleType(), True)
])

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

# Create streaming DataFrames for each topic
customers_df = create_streaming_df("src-app-customers-json", customer_schema)
drivers_df = create_streaming_df("src-app-drivers-json", driver_schema)
rides_df = create_streaming_df("src-app-ride-json", ride_schema)

# Define Delta Lake path
delta_path = "s3a://stream/delta/"

# Function to write streaming DataFrame to Delta Lake
def write_stream_to_delta(df, path):
    return df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{path}/_checkpoints") \
        .start(path)

# Write each DataFrame to Delta Lake
customers_query = write_stream_to_delta(customers_df, f"{delta_path}customers")
drivers_query = write_stream_to_delta(drivers_df, f"{delta_path}drivers")
rides_query = write_stream_to_delta(rides_df, f"{delta_path}rides")

# Await termination of all queries
spark.streams.awaitAnyTermination()
