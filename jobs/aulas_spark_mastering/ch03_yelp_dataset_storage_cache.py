"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_yelp_dataset_storage_cache.py

Tempo de Execucao: 44 segundos

====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, concat
from sparkmeasure import StageMetrics

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

    spark = sessao_spark('ch03-yelp-dataset-skew')

    logger = setup_logger()

    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/parquet"

    file_loc_reviews = f"s3a://{minio_bucket}/yelp/review/review/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/yelp/business/business/*.parquet"

    logger.info("Executando a task.")

    logger.info("Leitura dos Datasets")
    df_reviews = spark.read.parquet(file_loc_reviews)
    df_business = spark.read.parquet(file_loc_business)

    df_state_nyc = df_business.filter(df_business["city"] == "Las Vegas")
    df_joined = df_reviews.join(df_state_nyc, df_reviews["business_id"] == df_reviews["business_id"])


    logger.info("Medindo o Tempo sem Cache")
    start_time = time.time()
    df_joined.count()
    first_execution_time = time.time() - start_time

    logger.info("Medindo o Tempo com Cache")
    df_joined.cache()
    start_time = time.time()
    df_joined.count()
    second_execution_time = time.time() - start_time
    
    logger.info(f"execution time without caching: {first_execution_time} seconds")
    logger.info(f"execution time with caching: {second_execution_time} seconds")

    logger.info("Medindo tempo com transformacao adicional")
    avg_stars_by_city = df_joined.groupBy("city").avg("stars")
    start_time = time.time()
    avg_stars_by_city.show()
    third_execution_time = time.time() - start_time

    logger.info(f"execution time for additional transformation with caching: {third_execution_time} seconds")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
