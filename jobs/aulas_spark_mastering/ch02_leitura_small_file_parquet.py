"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_leitura_small_file_parquet.py

Numero de Particoes: 6


tamanho input:
- dsf.blocksize (default e 128MB)
- a capacidade de se dividir vai depender do formato do arquivo [json]
- depende do storage

tamanho particao:
- alinhada com o paralelismo padrao do spark ( 2 * numero cores) [6 cores]

tamanho particao ideal:
- 6 [cores] * 4 [vezes] = 24 [particoes]


Small files:
- Grande parte do tempo do job sera listando os arquivos
- Total de arquivos 188
- Tamanho dos arquivos lidos total = 3.5MB [PARQUET]
** O PARQUET CONSEGUIU LER 45.31% MENOR QUE O JSON
** So de mudar o tamanho do arquivo voce ja tem um ganho de performance
** O tempo de execução é menor, mas a redução é pequena, ja que continua o problema
de small files

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

    spark = sessao_spark('ch02-leitura-parquet')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/mongodb/rides/parquet/*.parquet"

    logger.info("Executando a task.")

    logger.info("Leitura do Arquivo PARQUET")
    df_rides = spark.read.parquet(f"s3a://{minio_bucket}")
    
    logger.info(f"Numero de Particoes: {df_rides.rdd.getNumPartitions()}")
    logger.info(f"Numero de Linhas: {df_rides.count()}")


    logger.info("Script concluído.")
    spark.stop()



if __name__ == "__main__":
    main()