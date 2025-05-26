"""
Output functions for writing data.
"""

import logging

logger = logging.getLogger(__name__)


def write_data(df, output_path, format_data="parquet"):
    """
    Writes the DataFrame to the specified output path in the specified format.

    Parameters:
        df (pyspark.sql.DataFrame): The DataFrame to write.
        output_path (str): The path to write the DataFrame to.
        format_data (str, optional): The format to write the DataFrame in. Defaults to "parquet".

    Returns:
        None
    """

    logger.info("starting to write data to %s in %s format", output_path, format_data)
    try:
        df.write.mode("overwrite").format(format_data).save(output_path)
        logger.info("successfully wrote data to %s in %s format", output_path, format_data)
    except Exception as e:
        logger.error("failed to write data to %s in %s format", output_path, format_data)
        logger.exception("Exception occurred: %s", e)
        raise
