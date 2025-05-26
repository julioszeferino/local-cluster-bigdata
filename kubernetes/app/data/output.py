"""
Generic Output module for writing data using Spark.
"""

from pyspark.sql import DataFrame


def write_data(df: DataFrame, output_path: str, format_data: str = "parquet"):
    """
    Writes the DataFrame to the specified output path in the specified data format.

    Args:
        df (DataFrame): The DataFrame to be written.
        output_path (str): The path where the data should be written.
        format_data (str, optional): The data format for writing the data. Defaults to "parquet".

    Raises:
        Exception: If an error occurs during writing the data, raises the exception.

    Returns:
        None
    """

    try:
        if format_data == "delta":
            df.write.format("delta").mode("overwrite").save(output_path)
        elif format_data == "iceberg":
            df.write.format("iceberg").mode("overwrite").save(output_path)
        else:
            df.write.mode("overwrite").format(format_data).save(output_path)
    except Exception as e:
        print(e)
        raise
