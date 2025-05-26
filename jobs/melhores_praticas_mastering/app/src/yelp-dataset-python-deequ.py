"""
python-deequ is a Python wrapper for Amazon's Deequ library,
which is used for data quality validation. It helps in defining constraints and
checks on data and then validating them. Below is a detailed example of how you can use python-deequ
to perform data quality checks on the Yelp dataset, specifically using the Users, Business, and Reviews datasets.

docker exec -it spark-master /bin/bash -c "export SPARK_VERSION=3.5 && /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-python-deequ.py"

https://github.com/awslabs/python-deequ

PyDeequ is a Python API for Deequ, a library built on top of Apache Spark for defining "unit tests for data",
which measure data quality in large datasets. PyDeequ is written to support usage of Deequ in Python.
"""

import os
import logging
from utils import create_spark_session
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT

# TODO set SPARK_VERSION environment variable
os.environ["SPARK_VERSION"] = "3.5"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_data_without_schema(spark: SparkSession, file_path: str):
    logger.info(f"Reading data from {file_path} without schema enforcement")
    return spark.read.parquet(file_path)


def run_data_quality_checks(spark, df, check):
    check_result = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(check) \
        .run()

    result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
    result_df.show(truncate=False)
    return result_df


def run_analyzers(spark, df):
    analysis_result = AnalysisRunner(spark) \
        .onData(df) \
        .addAnalyzer(Size()) \
        .addAnalyzer(Completeness("business_id")) \
        .addAnalyzer(ApproxCountDistinct("business_id")) \
        .addAnalyzer(Mean("stars")) \
        .run()

    analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
    analysis_result_df.show(truncate=False)
    return analysis_result_df


def run_profile(spark, df):
    result = ColumnProfilerRunner(spark) \
        .onData(df) \
        .run()

    for col, profile in result.profiles.items():
        print(f"Column: {col}")
        print(f"\tData Type: {profile.dataType}")
        print(f"\tCompleteness: {profile.completeness}")
        print(f"\tApproximate Number of Distinct Values: {profile.approxNumDistinctValues}")
        if hasattr(profile, 'isUnique'):
            print(f"\tIs Unique: {profile.isUnique}")
        if hasattr(profile, 'mean'):
            print(f"\tMean: {profile.mean}")
        if hasattr(profile, 'min'):
            print(f"\tMinimum: {profile.min}")
        if hasattr(profile, 'max'):
            print(f"\tMaximum: {profile.max}")


def run_constraint_suggestions(spark, df):
    suggestion_result = ConstraintSuggestionRunner(spark) \
        .onData(df) \
        .addConstraintRule(DEFAULT()) \
        .run()

    suggestion_result_df = suggestion_result.constraint_suggestions
    for key in suggestion_result_df.keys():
        print(f"Column: {key}")
        for suggestion in suggestion_result_df[key]:
            print(f"\t{suggestion.description} -- {suggestion.code_for_constraint}")


def main():
    with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
        config = json.load(config_file)
    spark = create_spark_session(app_name="yelp-dataset-deequ")

    # TODO Read data without schema enforcement
    df_business = read_data_without_schema(spark, config["loc_path_business"])
    df_users = read_data_without_schema(spark, config["loc_path_users"])

    # TODO Define Checks
    business_check = Check(spark, CheckLevel.Error, "Business Data Check") \
        .isComplete("business_id") \
        .isUnique("business_id") \
        .isContainedIn("stars", ["0.5", "1.0", "1.5", "2.0", "2.5", "3.0", "3.5", "4.0", "4.5", "5.0"]) \
        .isNonNegative("review_count")

    # TODO Run Checks
    logger.info("Running Business Data Quality Checks")
    run_data_quality_checks(spark, df_business, business_check)

    # TODO Run Analyzers
    logger.info("Running Analyzers on Business Data")
    run_analyzers(spark, df_business)

    spark.stop()


if __name__ == "__main__":
    main()
