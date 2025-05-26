"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_spark_loop_seq.py

====================================================================
"""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

    spark = sessao_spark('ch02-spark-loop-seq')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing"
    json_prefix = "mongodb/stripe/json"

    json_files = [
        f"s3a://{minio_bucket}/{json_prefix}/2025_04_20_15_15_18.json",
        f"s3a://{minio_bucket}/{json_prefix}/2025_04_20_16_08_37.json",
        f"s3a://{minio_bucket}/{json_prefix}/2025_04_20_16_08_47.json"
    ]
    logger.info("Executando a task.")

    logger.info("Leitura dos Arquivos")
    stripe_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("dt_current_timestamp", StringType(), True)
    ])

    stripe_data = spark.createDataFrame([], schema=stripe_schema)

    # NAO USA O PARALELISMO DO SPARK
    # escaneia a cada iteracao
    for json_file in json_files:
        df_stripe = spark.read.schema(stripe_schema).json(json_file)
        stripe_data_df = stripe_data.union(df_stripe)


    stripe_data_df.createOrReplaceTempView("stripe_loop")
    spark.sql("""
        SELECT user_id, COUNT(user_id) AS total
        FROM stripe_loop
        GROUP BY user_id
    """).show()

    # USANDO O PARALELISMO DO SPARK
    # escaneia apenas uma vez
    df_stripe_py = spark.read.json(json_files)
    df_stripe_py.createOrReplaceTempView("stripe_parallel")
    spark.sql("""
        SELECT user_id, COUNT(user_id) AS total
        FROM stripe_parallel
        GROUP BY user_id
    """).show()

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()