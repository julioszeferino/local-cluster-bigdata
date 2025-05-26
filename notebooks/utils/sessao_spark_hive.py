# Configurações do MinIO
import os
from pyspark.sql import SparkSession


def sessao_spark(app_name):

    # Configurações do Hive
    hive_metastore_uris = "thrift://hive-metastore:9083"

    # Configurações do MinIO
    minio_endpoint = "http://minio:9000"
    minio_access_key = "eUr2VdvHhnCL0GdS3BgI"
    minio_secret_key = "vZTGZhldy3I05FW0Ex7UHhgONmszmxtS5JIobk15"
    minio_bucket = "landing-zone/postgres/VENDAS_082024.csv"
    
    # Lista todos os JARs na pasta
    jars_dir = "./jars"
    jars = [os.path.join(jars_dir, jar) for jar in os.listdir(jars_dir) if jar.endswith(".jar")]
    jars_str = ",".join(jars)
    
    # Configuração do Spark
    spark = SparkSession \
        .builder \
        .appName(f"{app_name}") \
        .master("spark://spark-master:7077") \
        .config("spark.jars", jars_str) \
        .config("spark.executor.memory", "3g") \
        .config("hive.metastore.uris", hive_metastore_uris) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.s3a.endpoint", f"{minio_endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", f"{minio_access_key}") \
        .config("spark.hadoop.fs.s3a.secret.key", f"{minio_secret_key}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "s3a://data-lake/warehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.local.warehouse", "s3a://data-lake/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
        
        


        
        

    return spark