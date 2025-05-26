"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_collect_fnc.py

the collect() function:
retrieve the entire dataset from the distributed cluster to the driver node.

1. Data Analysis and Debugging:
- inspect the data to understand its structure

2. Driver-Side Operations:
- pandas_df = spark_df.collect().toPandas()

3. Output Generation:
- generating reports, visualizations, or exporting data to local files


reviews_dataset = df_reviews.collect() [1.8 minutes]
- java.lang.IllegalStateException: unread block data
- http://localhost:18080/history/app-20240702172145-0034/jobs/

reviews_dataset = df_reviews.take(5) [1.2 minutes]
====================================================================
"""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

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

    spark = sessao_spark('ch02-collect-fnc')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing"

    file_loc_reviews = f"s3a://{minio_bucket}/parquet/yelp/review/review/*.parquet"
    file_loc_users = f"s3a://{minio_bucket}/parquet/yelp/user/user/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/parquet/yelp/business/business/*.parquet"

    logger.info("Executando a task.")

    logger.info("Leitura dos Arquivos")
    df_reviews = spark.read.parquet(file_loc_reviews)
    df_users = spark.read.parquet(file_loc_users)
    df_business = spark.read.parquet(file_loc_business)
    
    logger.info(f"total rows [reviews]: {df_reviews.count()}")
    logger.info(f"total rows [users]: {df_users.count()}")
    logger.info(f"total rows [business]: {df_business.count()}")

    df_reviews.show(5)
    df_users.show(5)
    df_business.show(5)

    # Uso do collect em uma tabbela pequena
    business_list = df_business.collect()
    print(business_list[:5])

    try:
        # Uso do collect em uma tabela grande
        # reviews_dataset = df_reviews.collect()
        reviews_dataset = df_reviews.take(5)
        print({len(reviews_dataset)})
    except Exception as e:
        print(f"Error: {e}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()