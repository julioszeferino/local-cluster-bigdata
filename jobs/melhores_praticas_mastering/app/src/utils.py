"""
This module contains utility functions for the Yelp ETL pipeline.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def create_spark_session(app_name):
    """
    Create a Spark session with the given application name.

    Parameters:
        app_name (str): The name of the Spark application (default: "yelp_etl")

    Returns:
        SparkSession: The created Spark session.
    """

    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.executor.memory", "3g") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://138.197.224.4") \
        .config("spark.hadoop.fs.s3a.access.key", "data-lake") \
        .config("spark.hadoop.fs.s3a.secret.key", "12620ee6-2162-11ee-be56-0242ac120002") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_num_partitions(df: DataFrame) -> int:
    """
    This method `get_num_partitions` returns the number of partitions for the given DataFrame `df`.

    Parameters:
    - `df`: The input DataFrame.

    Returns:
    - An integer representing the number of partitions in the DataFrame.
    """

    logger.info("getting the number of partitions")
    num_partitions = df.rdd.getNumPartitions()
    logger.info(f"number of partitions: {num_partitions}")

    return num_partitions
