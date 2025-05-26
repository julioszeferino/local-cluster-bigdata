"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_file_explosion.py

- A querie em cima dos 50M (32 arquivos) foi tao rapida quanto a querie em cima dos 100
arquivos.
- Logo, nao vale a pena particionar os dados.

====================================================================
"""
import os

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

    spark = sessao_spark('ch02-file-explosion')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/iot"

    logger.info("Executando a task.")

    # ---- Teste com leitura particionada em 100 registros ----
    logger.info("Gera dados IoT")
    df_iot_data_id = spark.range(0, 100) \
        .select(
        hash('id').alias('id'),
        rand().alias('value'),
        from_unixtime(lit(1701692381 + col('id'))).alias('time')
        )

    logger.info("Escreve dados IoT particionado por id")
    (
        df_iot_data_id.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("id")
        .save(f"s3a://{minio_bucket}/iot_id")
    )

    df_iot_data_id.createOrReplaceTempView("iot_id")

    # leitura muito rapida
    spark.sql("""
        SELECT * 
        FROM iot_id 
        WHERE id = 519220707
    """).show()

    # leitura demorada
    spark.sql("""
        SELECT avg(value) 
        FROM iot_id 
        WHERE time >= "2023-12-04 12:19:00" 
        AND time <= "2023-12-04 13:01:20"
    """).show()

    # ---- Teste com leitura nao particionada em 50M de registros ----
    df_iot_data = spark.range(0,50000000, 1, 32) \
    .select(
        hash('id').alias('id'),
        rand().alias('value'),
        from_unixtime(lit(1701692381 + col('id'))).alias('time')
        )
    
    (
        df_iot_data.write
        .format("parquet")
        .mode("overwrite")
        .save(f"s3a://{minio_bucket}/iot")
    )
    df_iot_data.createOrReplaceTempView("iot")

    spark.sql("""
        SELECT * 
        FROM iot 
        WHERE id = 519220707
    """).show()

    spark.sql("""
        SELECT avg(value) 
        FROM iot 
        WHERE time >= "2023-12-04 12:19:00" AND time <= "2023-12-04 13:01:20"
    """).show()

    
    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()