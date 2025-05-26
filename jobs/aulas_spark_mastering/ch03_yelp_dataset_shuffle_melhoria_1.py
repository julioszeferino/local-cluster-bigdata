"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_yelp_dataset_shuffle_melhoria_1.py

Tempo de Execucao: 32 segundos [(32 / 49) - 1] * 100 = 34.69% [Reduzido]

Melhorias Aplicadas:
- Reduzindo o numero de particoes do shuffle para o
total de cores do cluster: .config("spark.sql.shuffle.partitions", "6") 

- Aumentando o buffer de escrita do shuffle para 1MB:
.config("spark.shuffle.file.buffer", "1000")

# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 6
numTasks => 86
elapsedTime => 31600 (32 s) [Tempo Total Execucao]
stageDuration => 47024 (47 s)
executorRunTime => 128024 (2.1 min)
executorCpuTime => 66607 (1.1 min)
executorDeserializeTime => 12293 (12 s)
executorDeserializeCpuTime => 3519 (4 s)
resultSerializationTime => 384 (0.4 s)
jvmGCTime => 3854 (4 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 7983 (8 s)
resultSize => 104021 (101.6 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 940572240
recordsRead => 7140626
bytesRead => 3094882743 (2.9 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 1175100
shuffleTotalBlocksFetched => 40
shuffleLocalBlocksFetched => 13
shuffleRemoteBlocksFetched => 27
shuffleTotalBytesRead => 591418672 (564.0 MB)
shuffleLocalBytesRead => 194242052 (185.2 MB)
shuffleRemoteBytesRead => 397176620 (378.8 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 3584474728 (3.3 GB) [Tamanho do Shuffle]
shuffleRecordsWritten => 7140626

Average number of active tasks => 4.1

Stages and their duration:
Stage 0 duration => 5806 (6 s)
Stage 1 duration => 580 (0.6 s)
Stage 2 duration => 131 (0.1 s)
Stage 3 duration => 18689 (19 s) [PESADO, mas mais leve que o anterior]
Stage 4 duration => 18994 (19 s) [PESADO, mas mais leve que o anterior]
Stage 7 duration => 2824 (3 s)
Metricas elapsedTime = 31600

# ------------------- * -------------------- * -------------------



====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from sparkmeasure import StageMetrics

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
        # Configs Shuffle
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.shuffle.file.buffer", "1000")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .getOrCreate()
    )

    return spark


def main():

    spark = sessao_spark('ch03-yelp-dataset-shuffle-1')

    logger = setup_logger()

    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/parquet"

    file_loc_reviews = f"s3a://{minio_bucket}/yelp/review/review/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/yelp/business/business/*.parquet"

    logger.info("Inicia o Metrics.")
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    logger.info("Executando a task.")

    logger.info("Leitura dos Datasets")
    df_reviews = spark.read.parquet(file_loc_reviews)
    df_business = spark.read.parquet(file_loc_business)
    
    logger.info("Join dos datasets")
    start_time = time.time()

    df_join_reviews_business = (
        df_reviews.alias("r")
        .join(df_business.alias("b"), col("r.business_id") == col("b.business_id"))
    )

    df_join_reviews_business.show()
    df_join_reviews_business.explain(True)

    end_time = time.time()
    logger.info("Tempo de execução do join: %s segundos", end_time - start_time)

    logger.info("Stage Metrics Report: ")
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"Metricas elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
