"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch01_basic_partitions_formatos_bigdata.py


Tabela de Bytes -> MB:
- 134217728 bytes = 128 MB
- 67108864 bytes = 64 MB
- 33554432 bytes = 32 MB
- 17825792 bytes = 17 MB
- 4194304 bytes = 4 MB

Configuracoes Spark:
- Executres: 3
- Cores por executor: 2
- Memoria por executor: 3g
- Total Cores [Slots]: 6
- Total Memoria: 9g

# Experimento 01: [13 segundos pra rodar]
Arquivo Input: 357.3 MBs
Tamanho padrão para partições: 128 MB
Particoes esperadas: 357.3 / 128 = 3 (Ficaria ocioso, usaria so 3 slots dos 6)
Particoes Geradas: 6 (O spark toma essa decisao em cima da qtde de blocos 
                        nos metadados do parquet e estatisticas, em media
                        357.3 / 6 = 59.55 MBs por particao)
Conclusao: O spark conseguiu gerar a quantidade de particoes do cluster no entanto
continua ocioso ja que a medida que uma particao termina, outra particao ainda
vai estar em processamento. Isso pode ser um problema de performance.


# Experimento 02 - Usando Repartition: [55 segundos pra rodar]
Arquivo Input: 357.3 MBs
Tamanho ideal particao:
    - Regra: 3-4x a quantidade de cpu cores
    - Minimo = 6 cores x 3 = 18 particoes
    - Maximo = 6 cores x 4 = 24 particoes
    - Calculo do tamanho ideal: 357.3 MBs / 24 = 14.89MBs
Conclusao: ao usar o repartition, mesmo obtendo o tamanho ideal de particao,
o tempo de processamento aumentou. Isso acontece porque o repartition
realiza um shuffle, ou seja, ele vai pegar todas as particoes e redistribuir
os dados entre as particoes. Isso gera um overhead de processamento
e aumenta o tempo de processamento.


# Experimento 03 - Usando a configuracao de maxPartitionBytes: [11 segundos para rodar]
Arquivo Input: 357.3 MBs
Tamanho ideal particao: 14.89MBs (bytes: 15676416)

##################### ESTRATEGIA #####################
- Usar o MaxPartitionBytes -> Quando sei o tamanho do dataset de entrada (quando
ele sempre vai ter o mesmo tamanho na entrada, geralmente na silver ja que eu
posso fazer a bronze cuspir os dados na particao exata que eu quero consumir)

- Usar o Repartition -> Quando não sei o tamanho do dataset de entrada (quando
ele pode variar, geralmente na bronze)


** Se voce quer pegar os dados e colocar na bronze, a melhor pratica
e fazer o repartition antes de salvar os dados. Paga na escrita mas
ganha na leitura.


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
        # .config("spark.sql.files.maxPartitionBytes", "134217728") # Experimento 01: 128MB
        .config("spark.sql.files.maxPartitionBytes", "15676416") # Experimento 03: 14.89MB
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

    spark = sessao_spark('ch01_basic_partitions-arquivos-bigdata')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("ERROR")
    minio_bucket = "production/landing"

    logger.info("Executando a task.")
    df = spark.read.parquet(
        f"s3a://{minio_bucket}/tlc/fhvhv/2022/fhvhv_tripdata_2022-01.parquet", header=True, inferSchema=True)
    logger.info(f"Numero de partições: {df.rdd.getNumPartitions()}")

    # logger.info("Realizando o Repartition")
    # df_repart = df.repartition(24)
    # logger.info(f"Numero de partições: {df_repart.rdd.getNumPartitions()}")

    df.show()

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()