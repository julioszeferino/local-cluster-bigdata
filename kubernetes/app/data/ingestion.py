"""
Generic Ingestion module for reading data using Spark.
"""

from pyspark.sql import SparkSession, DataFrame


def read_data(spark: SparkSession, file_path: str, file_format: str = 'parquet') -> DataFrame:
    """
    Read data from a file using Spark.

    :param spark: The SparkSession object used to read the data.
    :type spark: pyspark.sql.SparkSession
    :param file_path: The path to the file.
    :type file_path: str
    :param file_format: The format of the file (e.g., 'parquet', 'csv', 'json'). Defaults to 'parquet'.
    :type file_format: str
    :return: A DataFrame containing the data.
    :rtype: pyspark.sql.DataFrame
    """

    if file_format == 'parquet':
        return spark.read.parquet(file_path)
    elif file_format == 'csv':
        return spark.read.option("header", "true").csv(file_path)
    elif file_format == 'json':
        return spark.read.json(file_path)
