"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/main/etl-yelp-batch.py
"""

import json
import logging
from utils.helpers import create_spark_session, get_num_partitions
from data.ingestion import read_data
from data.business import filter_active_businesses, calculate_average_rating, join_business_review_data
from data.output import write_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    try:
        with open("/app/config/config.json", "r") as config_file:
            config = json.load(config_file)

        spark = create_spark_session(app_name="etl-yelp-batch")
        sc = spark.sparkContext
        sc.setJobGroup("etl-yelp-batch", "ETL Batch Job for Yelp Dataset")

        # TODO Data Ingestion
        df_business = read_data(spark, config["loc_path_business"])
        df_reviews = read_data(spark, config["loc_path_reviews"])

        # TODO Utils
        num_partitions_business = get_num_partitions(df_business)
        num_partitions_reviews = get_num_partitions(df_reviews)

        print(num_partitions_business)
        print(num_partitions_reviews)

        # TODO Data Transformation [Business]
        active_business_df = filter_active_businesses(df_business)
        avg_rating_df = calculate_average_rating(df_reviews)
        df_join = join_business_review_data(active_business_df, avg_rating_df)

        # TODO Data Output
        write_data(df=df_join, output_path=config["output_path"] + "/yelp/join", format_data="delta")
        write_data(df=df_business, output_path=config["output_path"] + "/yelp/business", format_data="delta")
        write_data(df=df_reviews, output_path=config["output_path"] + "/yelp/reviews", format_data="delta")

        spark.stop()

    except Exception as e:
        print(e)
        raise


if __name__ == "__main__":
    main()
