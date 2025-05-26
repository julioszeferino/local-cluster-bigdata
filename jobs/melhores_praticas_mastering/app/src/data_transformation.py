"""
Transformation module for data processing.
"""

import logging
from pyspark.sql.functions import col, avg

logger = logging.getLogger(__name__)


def filter_active_businesses(business_df):
    """
    This method `filter_active_businesses` filters the given DataFrame `business_df` to only include active businesses.

    Parameters:
    - `business_df`: The input DataFrame containing business information.

    Returns:
    - A new DataFrame that includes only active businesses.
    """

    logger.info("filtering active businesses")
    filtered_df = business_df.filter(col("stars") >= 1)
    logger.info(f"filtered {filtered_df.count()} active businesses")
    return filtered_df


def calculate_average_rating(review_df):
    """
    Calculate the average rating for each business.

    Parameters:
    - review_df: DataFrame that contains the review data.

    Returns:
    - DataFrame: DataFrame with business_id and average rating for each business.

    Example usage:
    review_data = spark.read.csv("review_data.csv")
    average_ratings = calculate_average_rating(review_data)
    """

    logger.info("calculating average ratings")
    avg_rating_df = review_df.groupBy("business_id").agg(avg("stars").alias("avg_rating"))
    logger.info(f"calculated average ratings for {avg_rating_df.count()} businesses")
    return avg_rating_df


def join_data(business_df, review_df):
    """
    Joins two dataframes based on the business_id column.

    Args:
        business_df (DataFrame): The dataframe containing business data.
        review_df (DataFrame): The dataframe containing review data.

    Returns:
        DataFrame: The joined dataframe.
    """

    logger.info("joining business and review data")
    business_df_renamed = business_df.withColumnRenamed("business_id", "id")
    joined_df = business_df_renamed.join(review_df, business_df_renamed.id == review_df.business_id)
    logger.info(f"joined data contains {joined_df.count()} records")
    return joined_df
