"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_patterns_repartition_coalesce.py

3 Executors with 2 Cores Each x 4 Times = 24 [Partition = Tasks]
====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

from utils.logger import setup_logger


def sessao_spark(app_name):

    minio_endpoint = "http://minio:9000"
    minio_access_key = os.getenv("MINIO_ACCESS")
    minio_secret_key = os.getenv("MINIO_SECRET")
    
    # Configuração do Spark
    spark = (
        SparkSession 
        .builder 
        .appName(f"{app_name}") 
        .master("spark://spark-master:7077") 
        .config("spark.executor.memory", "3g") 
        .config("spark.executor.cores", "2")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) 
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) 
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) 
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
        .config("spark.hadoop.fs.s3a.path.style.access", "true") 
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .getOrCreate()
    )

    return spark


def main():

    spark = sessao_spark('ch02-patterns-repartition-coalesce')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/parquet"

    file_loc_reviews = f"s3a://{minio_bucket}/yelp/review/review/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/yelp/business/business/*.parquet"

    df_reviews = spark.read.parquet(file_loc_reviews)

    def measure_time_and_partitions(df, action="count"):
        start_time = time.time()
        if action == "count":
            df.count()
        elif action == "show":
            df.show()
        end_time = time.time()
        num_partitions = df.rdd.getNumPartitions()
        return end_time - start_time, num_partitions


    # TODO initial DataFrame
    # TODO initial dataframe partitions: 16, time: 7.2576 seconds
    initial_time, initial_partitions = measure_time_and_partitions(df_reviews, "count")
    print(f"initial dataframe partitions: {initial_partitions}, time: {initial_time:.4f} seconds")

    # TODO repartition DataFrame
    # TODO 24 Partitions => repartitioned dataframe partitions: 24, time: 4.5003 seconds
    df_repartitioned = df_reviews.repartition(24)
    repartition_time, repartition_partitions = measure_time_and_partitions(df_repartitioned, "count")
    print(f"repartitioned dataframe partitions: {repartition_partitions}, time: {repartition_time:.4f} seconds")

    # TODO coalesce DataFrame
    # TODO 24 Partitions => coalesced dataframe partitions: 16, time: 3.1158 seconds
    # TODO 12 Partitions => coalesced dataframe partitions: 12, time: 4.5969 seconds
    df_coalesced = df_reviews.coalesce(12)
    coalesce_time, coalesce_partitions = measure_time_and_partitions(df_coalesced, "count")
    print(f"coalesced dataframe partitions: {coalesce_partitions}, time: {coalesce_time:.4f} seconds")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()