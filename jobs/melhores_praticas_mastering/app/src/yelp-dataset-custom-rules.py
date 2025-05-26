"""
Custom Rules for Yelp Dataset

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-custom-rules.py
"""

import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
from utils import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)


def validate_business_data(df):
    logger.info("Validating business data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
    missing_values.show()

    # TODO Check for unique business_id
    duplicate_business_id = df.groupBy("business_id").count().filter(col("count") > 1)
    if duplicate_business_id.count() > 0:
        logger.warning("Duplicate business_id found")
        duplicate_business_id.show()

    # TODO Check for valid star ratings (0.5 to 5.0 in increments of 0.5)
    invalid_stars = df.filter((col("stars") < 0.5) | (col("stars") > 5.0) | (col("stars") % 0.5 != 0))
    if invalid_stars.count() > 0:
        logger.warning("Invalid star ratings found")
        invalid_stars.show()


def validate_review_data(df):
    logger.info("Validating review data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
    missing_values.show()

    # TODO Check for unique review_id
    duplicate_review_id = df.groupBy("review_id").count().filter(col("count") > 1)
    if duplicate_review_id.count() > 0:
        logger.warning("Duplicate review_id found")
        duplicate_review_id.show()

    # TODO Check for valid star ratings (1 to 5)
    invalid_stars = df.filter((col("stars") < 1) | (col("stars") > 5))
    if invalid_stars.count() > 0:
        logger.warning("Invalid star ratings found")
        invalid_stars.show()


def validate_user_data(df):
    logger.info("Validating user data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])
    missing_values.show()

    # TODO Check for unique user_id
    duplicate_user_id = df.groupBy("user_id").count().filter(col("count") > 1)
    if duplicate_user_id.count() > 0:
        logger.warning("Duplicate user_id found")
        duplicate_user_id.show()

    # TODO Check for valid average_stars (0.0 to 5.0)
    invalid_avg_stars = df.filter((col("average_stars") < 0.0) | (col("average_stars") > 5.0))
    if invalid_avg_stars.count() > 0:
        logger.warning("Invalid average stars found")
        invalid_avg_stars.show()


def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-custom-rules")

    # TODO Read data without schema enforcement
    # df_business = read_data_without_schema(spark, config["loc_path_business"])
    # df_reviews = read_data_without_schema(spark, config["loc_path_reviews"])
    df_users = read_data_without_schema(spark, config["loc_path_users"])

    # TODO Validate data
    # validate_business_data(df_business)
    # validate_review_data(df_reviews)
    validate_user_data(df_users)

    spark.stop()


if __name__ == "__main__":
    main()
