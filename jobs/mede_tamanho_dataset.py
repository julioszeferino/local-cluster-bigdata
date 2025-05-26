"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/mede_tamanho_dataset.py

https://github.com/sakjung/repartipy
====================================================================
"""
import os

import repartipy

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, to_date

from aulas_spark_mastering.utils.logger import setup_logger


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


def convert_size_bytes(size_bytes):
    """
    Converts a size in bytes to a human readable string using SI units.
    """
    import math
    import sys

    if not isinstance(size_bytes, int):
        size_bytes = sys.getsizeof(size_bytes)

    if size_bytes == 0:
        return "0B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return s, size_name[i]


def main():

    spark = sessao_spark('ch02-join-bhj')

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

    logger.info("Leitura do Arquivo")
    df_reviews = spark.read.parquet(file_loc_reviews)

    # --------------- FORMA 01 --------------- #
    # TAMANHO ESTIMADO: 4.59 GB
    df_reviews.cache() # configura o cache
    df_reviews.count() # ativa o cache

    # acessando os parametros da sessao
    size_bytes = spark._jsparkSession.sessionState().executePlan(
        df_reviews._jdf.queryExecution().logical(),
        df_reviews._jdf.queryExecution().mode()
    ).optimizedPlan().stats().sizeInBytes()
    
    df_reviews.unpersist() # remove o cache


    tamanho = convert_size_bytes(size_bytes)
    logger.info(f"Total table size: {tamanho[0]} {tamanho[1]}")


    # --------------- FORMA 02 --------------- #
    # TAMANHO ESTIMADO: 2.9 GB
    df_reviews_2 = spark.read.parquet(file_loc_reviews)
    df_reviews_2.explain('cost')

    # --------------- FORMA 03 - Com Repartipy --------------- #
    # TAMANHO ESTIMADO: 4.59 GB
    # TAMANHO ESTIMADO COM FILTRO: 1.49 MB
    df_reviews_3 = spark.read.parquet(file_loc_reviews).filter(to_date(col("date")) == '2019-09-09')
    with repartipy.SizeEstimator(spark=spark, df=df_reviews_3) as se:
        df_size_in_bytes = se.estimate()
        tamanho = convert_size_bytes(df_size_in_bytes)
        logger.info(f"Total table size: {tamanho[0]} {tamanho[1]}")


    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()