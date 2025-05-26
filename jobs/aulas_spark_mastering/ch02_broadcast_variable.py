"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_broadcast_variable.py

Tempo: 36seg
====================================================================
"""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import time

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

    spark = sessao_spark('ch02-broadcast-variable')

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
    
    logger.info("Sem Broadcast")
    start_time = time.time()
    # 32.5 seconds
    df_join = df_reviews.join(df_business, "business_id")
    df_join.show(5)
    end_time = time.time()
    logger.info(f"without broadcast execution time: {end_time - start_time} seconds")

    logger.info("Com Broadcast")
    # 5 seconds
    start_time = time.time()
    df_join_broadcast = df_reviews.join(broadcast(df_business), "business_id")
    df_join_broadcast.show(5)
    end_time = time.time()
    print(f"with broadcast execution time: {end_time - start_time} seconds")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()