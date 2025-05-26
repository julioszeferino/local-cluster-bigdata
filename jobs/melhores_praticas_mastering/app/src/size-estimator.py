"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/size-estimator.py

The estimate_dataframe_size function estimates the size of the DataFrame in memory by:
Defining an inner function calculate_size that will convert each partition of the DataFrame to a Pandas DataFrame and calculate its memory usage using pandas.DataFrame.memory_usage.
Using the rdd.mapPartitions method to apply calculate_size to each partition of the DataFrame.
Summing up the memory usage of all partitions using reduce.
Converting the total size from bytes to megabytes for easier readability.


Summary
This script demonstrates how to:

Set up a Spark session for running PySpark code.
Read a dataset (in this case, the Users dataset from Yelp) into a DataFrame.
Estimate the size of the DataFrame in memory by calculating the memory usage of each column.
Log the estimated size for easy monitoring and debugging.

# TODO INFO:__main__:Estimating DataFrame size in memory
# TODO INFO:__main__:Estimated DataFrame size in memory: 995.03 MB
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from utils import create_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_users_data(spark: SparkSession, file_path: str):
    logger.info(f"Reading users data from {file_path}")
    return spark.read.parquet(file_path)

def estimate_dataframe_size(df):
    logger.info("Estimating DataFrame size in memory")

    def sizeof_column(column):
        return df.select(column).rdd.map(lambda row: row[0].__sizeof__()).sum()

    columns = df.columns
    size = sum(sizeof_column(column) for column in columns)
    size_in_mb = size / (1024 ** 2)

    logger.info(f"Estimated DataFrame size in memory: {size_in_mb:.2f} MB")
    return size_in_mb

def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)

    spark = create_spark_session(app_name="yelp-users-size-estimator")

    # Read users data
    df_users = read_users_data(spark, config["loc_path_users"])

    # Estimate size of DataFrame
    estimate_dataframe_size(df_users)

    spark.stop()

if __name__ == "__main__":
    main()