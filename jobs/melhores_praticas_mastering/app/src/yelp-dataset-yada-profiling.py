"""
YData Profiling for Yelp Dataset
https://docs.profiling.ydata.ai/latest/

Data quality profiling and exploratory data analysis are crucial steps in
the process of Data Science and Machine Learning development

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-yada-profiling.py
"""

import json
import logging
from pyspark.sql import SparkSession
from utils import create_spark_session
from ydata_profiling import ProfileReport

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)


def generate_profile_report(df, report_path):
    logger.info(f"Generating profile report for DataFrame and saving to {report_path}")
    pdf = df.toPandas()  # TODO Convert to Pandas DataFrame
    profile = ProfileReport(pdf, title="Yelp Dataset Profile Report")
    profile.to_file(report_path)
    logger.info(f"Profile report saved to {report_path}")


def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-yada-profiling")

    # TODO Read data without schema enforcement
    # TODO df_reviews = read_data_without_schema(spark, config["loc_path_reviews"])
    # TODO df_business = read_data_without_schema(spark, config["loc_path_business"])
    df_users = read_data_without_schema(spark, config["loc_path_users"])

    # TODO Generate profile reports
    # TODO generate_profile_report(df_reviews, "review_data_profile_report.html")
    generate_profile_report(df_users, "/opt/bitnami/spark/jobs/app/reports/user_data_profile_report.html")

    spark.stop()


if __name__ == "__main__":
    main()
