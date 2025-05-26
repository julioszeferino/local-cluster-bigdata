"""
Generic Output module for writing data using Spark.
"""

from pyspark.sql import DataFrame


def write_data(df: DataFrame, output_path: str, format_data: str = "parquet"):
    """
    Writes the DataFrame to the specified output path in the specified format.

    :param df: The DataFrame to write.
    :type df: pyspark.sql.DataFrame
    :param output_path: The path to write the DataFrame to.
    :type output_path: str
    :param format_data: The format to write the DataFrame in. Defaults to "parquet".
    :type format_data: str
    :return: None
    """

    try:
        df.write.mode("overwrite").format(format_data).save(output_path)
    except Exception as e:
        print(e)
        raise
