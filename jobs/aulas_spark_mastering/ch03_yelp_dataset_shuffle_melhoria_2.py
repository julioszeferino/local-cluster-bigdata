"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_yelp_dataset_shuffle_melhoria_2.py

Tempo de Execucao: 15 segundos [(15 / 49) - 1] * 100 = 69.39% [Reduzido]

Melhorias Aplicadas:
- Reduzindo o numero de particoes do shuffle para o
total de cores do cluster: .config("spark.sql.shuffle.partitions", "6") 

- Aumentando o buffer de escrita do shuffle para 1MB:
.config("spark.shuffle.file.buffer", "1000")

- Broadcast join:
    - .join(broadcast(df_business.alias("b")), col("r.business_id") == col("b.business_id"))

# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 5
numTasks => 47
elapsedTime => 14583 (15 s) [Tempo Total Execucao]
stageDuration => 10766 (11 s)
executorRunTime => 27869 (28 s)
executorCpuTime => 9417 (9 s)
executorDeserializeTime => 10311 (10 s)
executorDeserializeCpuTime => 3069 (3 s)
resultSerializationTime => 391 (0.4 s)
jvmGCTime => 441 (0.4 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 0 (0 ms)
resultSize => 53323939 (50.9 MB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 176160688
recordsRead => 154442
bytesRead => 97044360 (92.5 MB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 0
shuffleTotalBlocksFetched => 0
shuffleLocalBlocksFetched => 0
shuffleRemoteBlocksFetched => 0
shuffleTotalBytesRead => 0 (0 Bytes)
shuffleLocalBytesRead => 0 (0 Bytes)
shuffleRemoteBytesRead => 0 (0 Bytes)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes) [SHUFFLE ZERADO!!!!]
shuffleBytesWritten => 0 (0 Bytes)
shuffleRecordsWritten => 0

Average number of active tasks => 1.9

Stages and their duration:
Stage 0 duration => 5380 (5 s)
Stage 1 duration => 652 (0.7 s)
Stage 2 duration => 578 (0.6 s)
Stage 3 duration => 3203 (3 s)
Stage 4 duration => 953 (1.0 s)
Metricas elapsedTime = 14583

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

    spark = sessao_spark('ch03-yelp-dataset-shuffle-2')

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
        .join(broadcast(df_business).alias("b"), col("r.business_id") == col("b.business_id"))
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
