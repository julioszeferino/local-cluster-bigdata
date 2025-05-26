"""
The [GateKeeper] team has requested that the schema of the Yelp dataset be tested for performance with and without schema enforcement.

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-sch-enforce.py
"""

import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, IntegerType
from utils import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_business_data_with_schema(spark: SparkSession, file_path: str):
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("stars", FloatType(), True),
        StructField("review_count", LongType(), True),
        StructField("is_open", IntegerType(), True)
    ])
    logger.info(f"Reading business data from {file_path} with schema enforcement")
    return spark.read.schema(schema).parquet(file_path)


def read_review_data_with_schema(spark: SparkSession, file_path: str):
    schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("text", StringType(), True),
        StructField("useful", LongType(), True),
        StructField("funny", LongType(), True),
        StructField("cool", LongType(), True)
    ])
    logger.info(f"Reading review data from {file_path} with schema enforcement")
    return spark.read.schema(schema).parquet(file_path)


def read_user_data_with_schema(spark: SparkSession, file_path: str):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", LongType(), True),
        StructField("yelping_since", StringType(), True),
        StructField("useful", LongType(), True),
        StructField("funny", LongType(), True),
        StructField("cool", LongType(), True),
        StructField("fans", LongType(), True),
        StructField("average_stars", FloatType(), True),
        StructField("compliment_hot", LongType(), True),
        StructField("compliment_more", LongType(), True),
        StructField("compliment_profile", LongType(), True),
        StructField("compliment_cute", LongType(), True),
        StructField("compliment_list", LongType(), True),
        StructField("compliment_note", LongType(), True),
        StructField("compliment_plain", LongType(), True),
        StructField("compliment_cool", LongType(), True),
        StructField("compliment_funny", LongType(), True),
        StructField("compliment_writer", LongType(), True),
        StructField("compliment_photos", LongType(), True)
    ])
    logger.info(f"Reading user data from {file_path} with schema enforcement")
    return spark.read.schema(schema).parquet(file_path)


def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)


def measure_performance(read_func, spark, file_path, description):
    start_time = time.time()
    df = read_func(spark, file_path)
    count = df.count()
    duration = time.time() - start_time
    logger.info(f"{description} took {duration:.2f} seconds and contains {count} records")
    return duration, count


def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-sch-enforce")

    # TODO Measure performance with schema enforcement
    business_with_schema_duration, business_with_schema_count = measure_performance(
        read_business_data_with_schema, spark, config["loc_path_business"], "Reading business data with schema enforcement")

    reviews_with_schema_duration, reviews_with_schema_count = measure_performance(
        read_review_data_with_schema, spark, config["loc_path_reviews"], "Reading review data with schema enforcement")

    users_with_schema_duration, users_with_schema_count = measure_performance(
        read_user_data_with_schema, spark, config["loc_path_users"], "Reading user data with schema enforcement")

    # TODO Measure performance without schema enforcement
    business_no_schema_duration, business_no_schema_count = measure_performance(
        read_data_without_schema, spark, config["loc_path_business"], "Reading business data without schema enforcement")

    reviews_no_schema_duration, reviews_no_schema_count = measure_performance(
        read_data_without_schema, spark, config["loc_path_reviews"], "Reading review data without schema enforcement")

    users_no_schema_duration, users_no_schema_count = measure_performance(
        read_data_without_schema, spark, config["loc_path_users"], "Reading user data without schema enforcement")

    # TODO Log performance comparison
    logger.info("Performance comparison (time in seconds):")
    logger.info(f"Business data with schema: {business_with_schema_duration:.2f} | without schema: {business_no_schema_duration:.2f}")
    logger.info(f"Review data with schema: {reviews_with_schema_duration:.2f} | without schema: {reviews_no_schema_duration:.2f}")
    logger.info(f"User data with schema: {users_with_schema_duration:.2f} | without schema: {users_no_schema_duration:.2f}")

    spark.stop()


if __name__ == "__main__":
    main()
