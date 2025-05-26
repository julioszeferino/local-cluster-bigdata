"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_skew_e_sparkmetrics_a.py

RODAR ESTE ARQUIVO ANTES DA VERSAO B, ELE E RESPONSAVEL POR GERAR
O PROBLEMA DE DATA SKEW NOS DADOS.
====================================================================
"""
import os

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

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
        .config("spark.sql.files.maxPartitionBytes", "134217728") # 128MB
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

    spark = sessao_spark('ch02-skew-e-sparkmetrics-a')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/parquet/yelp/review/review/*.parquet"
    minio_output = "production/landing/parquet/yelp/review/skewed_reviews"

    logger.info("Executando a task.")

    logger.info("Leitura do Arquivo PARQUET")
    df_reviews = spark.read.parquet(f"s3a://{minio_bucket}")

    today = '2025-04-20'
    date_today = to_date(lit(today), 'yyyy-MM-dd')
    skew_multiplier = 5

    # TODO today is 20/04/2025
    df_reviews_skew = df_reviews.withColumn("date", date_today)

    df_reviews_current_date = df_reviews_skew
    for _ in range(skew_multiplier - 1):
        df_reviews_current_date = df_reviews_current_date.union(df_reviews_skew)

    df_reviews_last = df_reviews.union(df_reviews_current_date)

    df_reviews_last.write.mode("overwrite").parquet(f"s3a://{minio_output}")

    logger.info("Qtde de linhas do DataFrame: %s", df_reviews_last.count())
    logger.info("Script concluído.")
    spark.stop()



if __name__ == "__main__":
    main()