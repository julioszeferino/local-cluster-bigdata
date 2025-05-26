"""
Large DataSet Analysis

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-large-dataset-analysis.py
"""

import json
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when
from utils import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)


def measure_performance(read_func, spark, file_path, description):
    start_time = time.time()
    df = read_func(spark, file_path)
    count = df.count()
    duration = time.time() - start_time
    logger.info(f"{description} took {duration:.2f} seconds and contains {count} records")
    return df, duration, count


def basic_statistics(df):
    logger.info("Generating basic statistics")
    df.describe().show()


def schema_information(df):
    logger.info("Printing schema information")
    df.printSchema()


def missing_values(df):
    logger.info("Calculating missing values")
    missing_df = df.select([(count(when(col(c).isNull() | isnan(c), c)).alias(c)) for c in df.columns])
    missing_df.show()


def value_counts(df, column):
    logger.info(f"Calculating value counts for column: {column}")
    df.groupBy(column).count().orderBy('count', ascending=False).show()


def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-large-dataset-analysis")

    # TODO Measure performance without schema enforcement
    df_reviews, reviews_no_schema_duration, reviews_no_schema_count = measure_performance(
        read_data_without_schema, spark, config["loc_path_reviews"], "Reading review data without schema enforcement")

    # TODO Analyze the Reviews DataFrame
    logger.info("Analyzing Reviews DataFrame")
    basic_statistics(df_reviews)
    schema_information(df_reviews)
    missing_values(df_reviews)
    value_counts(df_reviews, "stars")

    spark.stop()


if __name__ == "__main__":
    main()
