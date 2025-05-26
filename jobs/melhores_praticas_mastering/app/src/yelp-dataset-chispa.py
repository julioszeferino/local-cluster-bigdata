""""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/yelp-dataset-chispa.py
"""

import os
import json
import logging
from pyspark.sql import SparkSession
from chispa import assert_df_equality

# Sample data for testing
business_data = [
    ("1", "Business A", 4.5, 10),
    ("2", "Business B", 3.0, 5),
    ("3", "Business C", 5.0, 8)
]

review_data = [
    ("1", "User1", "1", 5),
    ("2", "User2", "2", 4),
    ("3", "User1", "3", 3)
]

user_data = [
    ("User1", "User A"),
    ("User2", "User B")
]

# Create Spark Session
def create_spark_session(app_name="chispa-demo"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
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

# Example transformation function
def calculate_average_stars(df_review, df_business):
    joined_df = df_review.join(df_business, "business_id", "inner")
    joined_df = joined_df.select(df_review["business_id"], df_review["stars"])
    avg_stars_df = joined_df.groupBy("business_id").avg("stars").withColumnRenamed("avg(stars)", "average_stars")
    return avg_stars_df

# Test function using chispa
def test_calculate_average_stars(spark):
    df_business, df_review, df_user = create_dataframes(spark)
    result_df = calculate_average_stars(df_review, df_business)

    expected_data = [
        ("1", 5.0),
        ("2", 4.0),
        ("3", 3.0)
    ]
    expected_schema = ["business_id", "average_stars"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)

def main():
    spark = create_spark_session()

    # Run the test
    test_calculate_average_stars(spark)

    spark.stop()

if __name__ == "__main__":
    main()
