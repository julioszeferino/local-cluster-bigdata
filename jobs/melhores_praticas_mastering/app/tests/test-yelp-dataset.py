"""
Use of Unit Testing in PySpark to validate Yelp Dataset

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/tests/test-yelp-dataset.py
"""

import unittest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Validation Functions
def validate_business_data(df):
    logger.info("Validating business data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # TODO Check for unique business_id
    duplicate_business_id = df.groupBy("business_id").count().filter(col("count") > 1)

    # TODO Check for valid star ratings (0.5 to 5.0 in increments of 0.5)
    invalid_stars = df.filter((col("stars") < 0.5) | (col("stars") > 5.0) | ((col("stars") * 10) % 5 != 0))

    return missing_values, duplicate_business_id, invalid_stars


def validate_review_data(df):
    logger.info("Validating review data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # TODO Check for unique review_id
    duplicate_review_id = df.groupBy("review_id").count().filter(col("count") > 1)

    # TODO Check for valid star ratings (1 to 5)
    invalid_stars = df.filter((col("stars") < 1) | (col("stars") > 5))

    return missing_values, duplicate_review_id, invalid_stars


def validate_user_data(df):
    logger.info("Validating user data")

    # TODO Check for null or missing values
    missing_values = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

    # TODO Check for unique user_id
    duplicate_user_id = df.groupBy("user_id").count().filter(col("count") > 1)

    # TODO Check for valid average_stars (0.0 to 5.0)
    invalid_avg_stars = df.filter((col("average_stars") < 0.0) | (col("average_stars") > 5.0))

    return missing_values, duplicate_user_id, invalid_avg_stars


# Test Cases
class YelpDatasetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("test-yelp-dataset") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_validate_business_data(self):
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("stars", FloatType(), True),
            StructField("review_count", LongType(), True),
            StructField("is_open", IntegerType(), True)
        ])

        data = [
            ("1", "Business A", "Address A", "City A", "State A", "12345", 10.0, 20.0, 4.5, 10, 1),
            ("2", "Business B", "Address B", "City B", "State B", "67890", 30.0, 40.0, 3.0, 5, 1),
            ("3", "Business C", "Address C", "City C", "State C", "11223", 50.0, 60.0, 5.0, 8, 1),
            # ("1", "Business D", "Address D", "City D", "State D", "44556", 70.0, 80.0, 4.0, 7, 1) # TODO duplicate ID for testing
        ]

        df = self.spark.createDataFrame(data, schema)
        missing_values, duplicate_business_id, invalid_stars = validate_business_data(df)

        self.assertEqual(duplicate_business_id.count(), 1, "Duplicate business_id found")
        self.assertEqual(invalid_stars.count(), 0, "Invalid star ratings found")
        for row in missing_values.collect():
            for col_name in df.columns:
                self.assertEqual(row[col_name], 0, f"Missing values found in column {col_name}")

    def test_validate_review_data(self):
        schema = StructType([
            StructField("review_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("business_id", StringType(), True),
            StructField("stars", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("useful", LongType(), True),
            StructField("funny", LongType(), True),
            StructField("cool", LongType(), True)
        ])

        data = [
            ("1", "U1", "B1", 5, "2020-01-01", "Great!", 1, 0, 0),
            ("2", "U2", "B2", 4, "2020-01-02", "Good", 2, 1, 0),
            # ("3", "U3", "B3", 6, "2020-01-03", "Excellent", 3, 2, 1),  # TODO Invalid star rating for testing
            # ("1", "U4", "B4", 3, "2020-01-04", "Average", 4, 3, 2)  # TODO Duplicate ID for testing
        ]

        df = self.spark.createDataFrame(data, schema)
        missing_values, duplicate_review_id, invalid_stars = validate_review_data(df)

        self.assertEqual(duplicate_review_id.count(), 1, "Duplicate review_id found")
        self.assertEqual(invalid_stars.count(), 1, "Invalid star ratings found")
        for row in missing_values.collect():
            for col_name in df.columns:
                self.assertEqual(row[col_name], 0, f"Missing values found in column {col_name}")

    def test_validate_user_data(self):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("yelping_since", StringType(), True),
            StructField("useful", LongType(), True),
            StructField("funny", LongType(), True),
            StructField("cool", LongType(), True),
            StructField("fans", LongType(), True),
            StructField("average_stars", FloatType(), True),
            StructField("compliment_hot", LongType(), True),
            StructField("compliment_more", LongType(), True),
            StructField("compliment_profile", LongType(), True),
            StructField("compliment_cute", LongType(), True),
            StructField("compliment_list", LongType(), True),
            StructField("compliment_note", LongType(), True),
            StructField("compliment_plain", LongType(), True),
            StructField("compliment_cool", LongType(), True),
            StructField("compliment_funny", LongType(), True),
            StructField("compliment_writer", LongType(), True),
            StructField("compliment_photos", LongType(), True)
        ])

        data = [
            ("1", "User A", 10, "2010-01-01", 1, 0, 0, 1, 4.5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
            # ("2", "User B", 5, "2011-01-01", 2, 1, 0, 0, 5.5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),  # TODO Invalid avg stars for testing
            ("3", "User C", 8, "2012-01-01", 3, 2, 1, 2, 4.0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
            # ("1", "User D", 7, "2013-01-01", 4, 3, 2, 3, 3.5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1)  # TODO Duplicate ID for testing
        ]

        df = self.spark.createDataFrame(data, schema)
        missing_values, duplicate_user_id, invalid_avg_stars = validate_user_data(df)

        self.assertEqual(duplicate_user_id.count(), 1, "Duplicate user_id found")
        self.assertEqual(invalid_avg_stars.count(), 1, "Invalid average stars found")
        for row in missing_values.collect():
            for col_name in df.columns:
                self.assertEqual(row[col_name], 0, f"Missing values found in column {col_name}")


if __name__ == "__main__":
    unittest.main()

