"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_join_otimizado_shuffle.py

The Suffle
Shuffle is a Spark mechanism that redistributes data so that it's grouped differently across partitions. This typically
involves copying data across executors and machines and, while it's sometimes necessary,
it can be a complex and costly operation.

The Broadcast Join
Can be significantly faster than shuffle joins if one of the tables is very large and the other is small.
Unfortunately, broadcast joins only work if at least one of the tables are less than 100MB in size.
In case of joining bigger tables,  if we want to avoid the shuffle we may need to reconsider our schema to avoid
having to do the join in the first place.
====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
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

    spark = sessao_spark('ch02-join-otimizado-shuffle')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("ERROR")
    minio_bucket = "production/landing/parquet"

    logger.info("Executando a task.")

    logger.info("Gera dataset com 150M de linhas.")
    df_transactions = spark.range(0, 150_000_000, 1, 32) \
        .select('id',
                round(rand() * 10000, 2).alias('amount'),
                (col('id') % 10).alias('country_id'),
                (col('id') % 100).alias('store_id')
        )
    
    logger.info("Escreve o dataset com 150M de linhas.")
    (
        df_transactions
        .write
        .format("parquet")
        .mode("overwrite")
        .save(f"s3a://{minio_bucket}/transactions/")
    )


    logger.info("Gera dataset com 100 linhas.")
    df_stores = spark.range(0, 99) \
        .select('id',
                round(rand() * 100, 0).alias('employees'),
                (col('id') % 10).alias('country_id'), expr('uuid()').alias('name')
        )
    
    logger.info("Escreve o dataset com 100 linhas.")
    (
        df_stores
        .write
        .format("parquet")
        .mode("overwrite")
        .save(f"s3a://{minio_bucket}/stores/")
    )


    logger.info("Gera dataset de paises.")
    columns = ["id", "name"]
    countries = [
        (0, "Italy"),
        (1, "Canada"),
        (2, "Mexico"),
        (3, "China"),
        (4, "Germany"),
        (5, "UK"),
        (6, "Japan"),
        (7, "Korea"),
        (8, "Australia"),
        (9, "France"),
        (10, "Spain"),
        (11, "USA")
    ]

    logger.info("Escreve dataset de paises.")
    df_countries = spark.createDataFrame(data=countries, schema=columns)
    (
        df_countries
        .write
        .format("parquet")
        .mode("overwrite")
        .save(f"s3a://{minio_bucket}/countries/")
    )

    logger.info("Desligando o broadcast automatica.")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

    logger.info("Leitura dos Dados")
    df_transactions.createOrReplaceTempView("transactions")
    df_stores.createOrReplaceTempView("stores")
    df_countries.createOrReplaceTempView("countries")


    logger.info("Fazendo join sem estrategia de broadcast e escrevendo o arquivo.")
    # 1.48 de shuffle read por join
    start_time = time.time()
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN stores
        ON transactions.store_id = stores.id
        LEFT JOIN countries
        ON transactions.country_id = countries.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    print(f"Tempo de Execucao sem Broadcast: {end_time - start_time} seconds")


    logger.info("Ligando o broadcast automatico.")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

    logger.info("Fazendo join com estrategia de broadcast e escrevendo o arquivo.")
    start_time = time.time()
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN stores
        ON transactions.store_id = stores.id
        LEFT JOIN countries
        ON transactions.country_id = countries.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    print(f"transact countries time [broadcast]: {end_time - start_time} seconds")


    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()