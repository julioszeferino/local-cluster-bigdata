# Configurações do MinIO
import os
from pyspark.sql import SparkSession


def sessao_spark(app_name):
    minio_endpoint = "http://minio:9000"
    minio_access_key = "lakehouse"
    minio_secret_key = "2fUDaiyqNFhoMmgYuXjO4d24fchviXQjM2TWTgUe"
    
    # Lista todos os JARs na pasta
    jars_dir = "./jars"
    jars = [os.path.join(jars_dir, jar) for jar in os.listdir(jars_dir) if jar.endswith(".jar")]
    jars_str = ",".join(jars)

    # Configuração do Spark
    spark = (
        SparkSession 
        .builder 
        .appName(f"{app_name}") 
        .master("spark://spark-master:7077") 
        .config("spark.jars", jars_str)
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
