"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-quinn.py
"""

import os
import logging
from pyspark.sql import SparkSession
from chispa import assert_df_equality
import quinn
from pyspark.sql.functions import col

# Sample data for testing
business_data = [
    ("1", "Business A", 4.5, 10),
    ("2", "Business B", 3.0, 5),
    ("3", "Business C", 5.0, 8)
]

review_data = [
    ("1", "", "1", 5),
    ("2", "User2", "2", 4),
    ("3", "User1", "3", 3)
]

user_data = [
    ("User1", "User A"),
    ("User2", "User B")
]


# Create Spark Session
def create_spark_session(app_name="quinn-demo"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Create DataFrames
def create_dataframes(spark):
    business_schema = ["business_id", "name", "stars", "review_count"]
    review_schema = ["review_id", "user_id", "business_id", "stars"]
    user_schema = ["user_id", "name"]

    df_business = spark.createDataFrame(business_data, business_schema)
    df_review = spark.createDataFrame(review_data, review_schema)
    df_user = spark.createDataFrame(user_data, user_schema)

    return df_business, df_review, df_user


# Example transformation function using quinn
def transform_data(df_business, df_review):
    df_business = df_business.withColumnRenamed("name", "business_name")

    # Using quinn to transform data
    df_review = df_review.withColumn("user_id_empty", quinn.is_null_or_blank(col("user_id")))
    joined_df = df_review.join(df_business, "business_id", "inner")

    return joined_df


# Test function using chispa
def test_transform_data(spark):
    df_business, df_review, df_user = create_dataframes(spark)
    result_df = transform_data(df_business, df_review)

    expected_data = [
        ("1", "User1", "1", 5, 5.0, "Business A", 4.5, 10),
        ("2", "User2", "2", 4, 4.0, "Business B", 3.0, 5),
        ("3", "User1", "3", 3, 3.0, "Business C", 5.0, 8)
    ]
    expected_schema = ["business_id", "user_id", "review_id", "stars_review", "stars_double", "business_name", "stars",
                       "review_count"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # assert_df_equality(result_df, expected_df, ignore_nullable=True)


def main():
    spark = create_spark_session()

    # Run the test
    test_transform_data(spark)

    spark.stop()


if __name__ == "__main__":
    main()
