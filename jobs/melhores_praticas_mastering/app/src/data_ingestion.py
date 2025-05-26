"""
Ingestion module for reading data from Parquet files using Spark.
"""

import logging

logger = logging.getLogger(__name__)


def read_business_data(spark, file_path):
    """
    Reads business data from a Parquet file.

    Args:
        spark (SparkSession): The SparkSession object.
        file_path (str): The path to the Parquet file.

    Returns:
        DataFrame: The DataFrame containing the business data.

    """

    logger.info("start reading business data")
    return spark.read.parquet(file_path)


def read_review_data(spark, file_path):
    """
    Reads review data from a parquet file.

    :param spark: The Spark Session object.
    :type spark: pyspark.sql.SparkSession
    :param file_path: The path to the parquet file.
    :type file_path: str
    :return: The DataFrame object containing the review data.
    :rtype: pyspark.sql.DataFrame
    """

    logger.info("start reading review data")
    return spark.read.parquet(file_path)


def read_user_data(spark, file_path):
    """
    Reads user data from a Parquet file using Spark.

    :param spark: The SparkSession object.
    :param file_path: The path to the Parquet file containing user data.
    :return: A DataFrame containing the user data.
    """

    logger.info("start reading user data")
    return spark.read.parquet(file_path)
