import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="yelp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)

def validate_business_data(df):
    logger.info("Validating business data")

    # Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # Check for unique business_id
    duplicate_business_id = df.groupBy("business_id").count().filter(col("count") > 1)

    # Check for valid star ratings (0.5 to 5.0 in increments of 0.5)
    invalid_stars = df.filter((col("stars") < 0.5) | (col("stars") > 5.0) | ((col("stars") * 10) % 5 != 0))

    return missing_values, duplicate_business_id, invalid_stars

def validate_review_data(df):
    logger.info("Validating review data")

    # Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # Check for unique review_id
    duplicate_review_id = df.groupBy("review_id").count().filter(col("count") > 1)

    # Check for valid star ratings (1 to 5)
    invalid_stars = df.filter((col("stars") < 1) | (col("stars") > 5))

    return missing_values, duplicate_review_id, invalid_stars

def validate_user_data(df):
    logger.info("Validating user data")

    # Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # Check for unique user_id
    duplicate_user_id = df.groupBy("user_id").count().filter(col("count") > 1)

    # Check for valid average_stars (0.0 to 5.0)
    invalid_avg_stars = df.filter((col("average_stars") < 0.0) | (col("average_stars") > 5.0))

    return missing_values, duplicate_user_id, invalid_avg_stars

def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-custom-rules")

    # Read data without schema enforcement
    df_business = read_data_without_schema(spark, config["loc_path_business"])
    df_reviews = read_data_without_schema(spark, config["loc_path_reviews"])
    df_users = read_data_without_schema(spark, config["loc_path_users"])

    # Validate data
    validate_business_data(df_business)
    validate_review_data(df_reviews)
    validate_user_data(df_users)

    spark.stop()

if __name__ == "__main__":
    main()
