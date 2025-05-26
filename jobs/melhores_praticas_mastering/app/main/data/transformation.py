"""
Generic Transformation module for data processing using Spark.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg


def filter_data(df: DataFrame, condition: str) -> DataFrame:
    """
    Filter the DataFrame based on the given condition.

    :param df: The input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param condition: The filter condition as a string.
    :type condition: str
    :return: A filtered DataFrame.
    :rtype: pyspark.sql.DataFrame
    """

    filtered_df = df.filter(condition)

    return filtered_df


def calculate_aggregate(df: DataFrame, group_by_col: str, agg_col: str, agg_func: str, alias: str) -> DataFrame:
    """
    Calculate an aggregate value for each group in the DataFrame.

    :param df: The input DataFrame.
    :type df: pyspark.sql.DataFrame
    :param group_by_col: The column to group by.
    :type group_by_col: str
    :param agg_col: The column to aggregate.
    :type agg_col: str
    :param agg_func: The aggregate function (e.g., 'avg', 'sum').
    :type agg_func: str
    :param alias: The alias name for the aggregate column.
    :type alias: str
    :return: A DataFrame with the aggregated values.
    :rtype: pyspark.sql.DataFrame
    """

    if agg_func == 'avg':
        agg_df = df.groupBy(group_by_col).agg(avg(agg_col).alias(alias))

        return agg_df


def join_data(df1: DataFrame, df2: DataFrame, join_col1: str, join_col2: str, how: str = 'inner') -> DataFrame:
    """
    Join two DataFrames based on the specified columns.

    :param df1: The first DataFrame.
    :type df1: pyspark.sql.DataFrame
    :param df2: The second DataFrame.
    :type df2: pyspark.sql.DataFrame
    :param join_col1: The column in the first DataFrame to join on.
    :type join_col1: str
    :param join_col2: The column in the second DataFrame to join on.
    :type join_col2: str
    :param how: The type of join (e.g., 'inner', 'outer', 'left', 'right'). Defaults to 'inner'.
    :type how: str
    :return: The joined DataFrame.
    :rtype: pyspark.sql.DataFrame
    """

    joined_df = df1.join(df2, df1[join_col1] == df2[join_col2], how=how)

    return joined_df
