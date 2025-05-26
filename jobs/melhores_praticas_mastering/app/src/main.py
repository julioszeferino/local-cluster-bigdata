"""
Script to execute Spark job using Yelp dataset.

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/src/etl-yelp-batch.py

TODO set PYTHONPATH to include src/app/src
TODO sc.setJobGroup("", "")
"""

import json
import logging
from utils import create_spark_session, get_num_partitions
from data_ingestion import read_business_data, read_review_data
from data_transformation import filter_active_businesses, calculate_average_rating, join_data
from data_output import write_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("reading configuration file")
        with open("/opt/bitnami/spark/jobs/app/config/config.json", "r") as config_file:
            config = json.load(config_file)

        logger.info("creating spark session")
        spark = create_spark_session(app_name="yelp_etl")
        sc = spark.sparkContext
        sc.setJobGroup("yelp_etl", "ETL job for Yelp Dataset")

        # TODO Data Ingestion
        logger.info("reading business data")
        df_business = read_business_data(spark, config["loc_path_business"])
        logger.info("reading review data")
        df_reviews = read_review_data(spark, config["loc_path_reviews"])

        # TODO Utils
        num_partitions_business = get_num_partitions(df_business)
        num_partitions_reviews = get_num_partitions(df_reviews)
        logger.info(f"business dataframe has {num_partitions_business} partitions")
        logger.info(f"reviews dataframe has {num_partitions_reviews} partitions")

        # TODO Data Transformation
        logger.info("filtering active businesses")
        active_business_df = filter_active_businesses(df_business)
        logger.info("calculating average ratings")
        avg_rating_df = calculate_average_rating(df_reviews)
        logger.info("joining business and review data")
        df_join = join_data(active_business_df, avg_rating_df)

        # TODO Data Output
        logger.info("writing joined data to output path")
        write_data(df_join, config["output_path"])

        logger.info("stopping Spark session")
        spark.stop()

    except Exception as e:
        logger.error("an error occurred during the ETL process")
        logger.exception("exception details: %s", e)
        raise


if __name__ == "__main__":
    logger.info("Starting Yelp ETL Job")
    main()
    logger.info("Yelp ETL Job Finished")
