"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch01_basic_query_execution.py

Exemplo 01: join sem usar AQE e broadcast
- Tempo Execucao: 1.4 minutos

Exemplo 02: join usando broadcast
- O broadcast e recomendado até 10MB no spark.
- No entanto podemos forcar o broadcast na chamada do join
- Tempo Execucao: 1.2 minutos
- Vai reduzir o input de exchange (shuffle) ja que vai
movimentar menos dados entre os nós.

====================================================================
"""
import os

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
        # -- Adaptive Query Execution -- #
        .config("spark.sql.adaptive.enabled", False)
        .config("spark.sql.adaptive.coalescePartitions.enabled", False)
        .config("spark.sql.adaptive.skewJoin.enabled", False)
        .getOrCreate()
    )

    return spark


def main():

    spark = sessao_spark('ch01-basic-query-execution')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing"

    logger.info("Executando a task.")

    logger.info("Leitura do Arquivo de Business")
    file_loc_business = f"s3a://{minio_bucket}/parquet/yelp/business/business/*.parquet"
    df_business = spark.read.parquet(file_loc_business)
    df_business.describe().show()

    logger.info("Leitura do Arquivo de Review")
    file_loc_review = f"s3a://{minio_bucket}/parquet/yelp/review/review/*.parquet"
    df_review = spark.read.parquet(file_loc_review)
    df_review.describe().show()


    logger.info("Realizando o Join")
    df_join_reviews_business = (
        df_review.alias('r')
        .join(
            # df_business.alias("b"),
            broadcast(df_business.alias("b")),
            col("r.business_id") == col("b.business_id"),
            )
    )

    df_join_reviews_business.explain(True)

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()