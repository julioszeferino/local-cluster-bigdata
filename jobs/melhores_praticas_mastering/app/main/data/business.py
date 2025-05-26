"""
Business-Specific Transformation module for data processing using Spark.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from data.transformation import filter_data, calculate_aggregate, join_data


def filter_active_businesses(business_df: DataFrame) -> DataFrame:
    """
    Filter active businesses from the business DataFrame.

    :param business_df: The input DataFrame containing business information.
    :type business_df: pyspark.sql.DataFrame
    :return: A DataFrame with only active businesses.
    :rtype: pyspark.sql.DataFrame
    """

    return filter_data(business_df, col("stars") >= 1)


def calculate_average_rating(review_df: DataFrame) -> DataFrame:
    """
    Calculate the average rating for each business.

    :param review_df: The DataFrame containing review data.
    :type review_df: pyspark.sql.DataFrame
    :return: A DataFrame with business_id and average rating for each business.
    :rtype: pyspark.sql.DataFrame
    """

    return calculate_aggregate(review_df, 'business_id', 'stars', 'avg', 'avg_rating')


def join_business_review_data(business_df: DataFrame, review_df: DataFrame) -> DataFrame:
    """
    Join business and review data on business_id.

    :param business_df: The DataFrame containing business data.
    :type business_df: pyspark.sql.DataFrame
    :param review_df: The DataFrame containing review data.
    :type review_df: pyspark.sql.DataFrame
    :return: The joined DataFrame.
    :rtype: pyspark.sql.DataFrame
    """

    return join_data(business_df.withColumnRenamed("business_id", "id"), review_df, "id", "business_id")
