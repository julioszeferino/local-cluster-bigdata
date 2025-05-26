"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_cache_persist.py

Tempo: 36seg
====================================================================
"""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import time
from pyspark.storagelevel import StorageLevel

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


def measure_time(df: DataFrame, action: str):
    start_time = time.time()
    if action == "count":
        df.count()
    elif action == "show":
        df.show()
    end_time = time.time()
    return end_time - start_time


def main():

    spark = sessao_spark('ch02-cache-persist')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("ERROR")
    minio_bucket = "production/landing/parquet"

    file_loc_reviews = f"s3a://{minio_bucket}/yelp/review/review/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/yelp/business/business/*.parquet"

    logger.info("Executando a task.")

    logger.info("Leitura dos Dados")
    df_reviews = spark.read.parquet(file_loc_reviews)
    df_business = spark.read.parquet(file_loc_business)
    
    df_joined = df_reviews.join(df_business, "business_id")
    df_transformed = df_joined.withColumn("category_length", F.length("categories"))

    # TODO without cache
    # TODO 8.77 seconds
    no_cache_time = measure_time(df_transformed, "count")
    logger.info(f"Tempo sem cache e persist: {no_cache_time} seconds")

    # TODO with cache
    # TODO initial execution: 120 seconds
    # TODO subsequent execution: 3.44 seconds
    df_cached = df_transformed.cache()
    cache_time_first = measure_time(df_cached, "count")
    cache_time_second = measure_time(df_cached, "count")
    print(f"Primeira execucao de cache: {cache_time_first} seconds")
    print(f"Segundo execucao de cache: {cache_time_second} seconds")

    logger.info("Limpando o cache")
    spark.catalog.clearCache()

    # TODO use persist
    # TODO initial execution: 60 seconds
    # TODO subsequent execution: 4.76 second
    df_persisted = df_transformed.persist(StorageLevel.DISK_ONLY)
    persist_time_first = measure_time(df_persisted, "count")
    persist_time_second = measure_time(df_persisted, "count")
    print(f"Primeira Execucao com persist: {persist_time_first} seconds")
    print(f"Segunda Execucao com persist: {persist_time_second} seconds")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()