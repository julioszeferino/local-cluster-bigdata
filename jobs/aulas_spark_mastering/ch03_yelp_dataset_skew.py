"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_yelp_dataset_skew.py

Tempo de Execucao: 44 segundos


# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 10
numTasks => 717
elapsedTime => 44207 (44 s)
stageDuration => 56609 (57 s)
executorRunTime => 182029 (3.0 min)
executorCpuTime => 70995 (1.2 min)
executorDeserializeTime => 21598 (22 s)
executorDeserializeCpuTime => 7476 (7 s)
resultSerializationTime => 725 (0.7 s)
jvmGCTime => 4305 (4 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 7723 (8 s)
resultSize => 692861 (676.6 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 296550304
recordsRead => 70203492
bytesRead => 2252785843 (2.1 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 795237
shuffleTotalBlocksFetched => 397
shuffleLocalBlocksFetched => 131
shuffleRemoteBlocksFetched => 266
shuffleTotalBytesRead => 48374851 (46.1 MB)
shuffleLocalBytesRead => 16259612 (15.5 MB)
shuffleRemoteBytesRead => 32115239 (30.6 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 293230113 (279.6 MB)
shuffleRecordsWritten => 4852529

Average number of active tasks => 4.1

Stages and their duration:
Stage 0 duration => 8864 (9 s)
Stage 1 duration => 390 (0.4 s)
Stage 2 duration => 92 (92 ms)
Stage 3 duration => 12426 (12 s)
Stage 5 duration => 305 (0.3 s)
Stage 6 duration => 489 (0.5 s)
Stage 8 duration => 132 (0.1 s)
Stage 9 duration => 16379 (16 s)
Stage 10 duration => 16380 (16 s)
Stage 13 duration => 1152 (1 s)
Metricas elapsedTime = 44207

# ------------------- * -------------------- * -------------------



====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, concat
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
        .getOrCreate()
    )

    return spark


def main():

    spark = sessao_spark('ch03-yelp-dataset-skew')

    logger = setup_logger()

    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/parquet"

    file_loc_reviews = f"s3a://{minio_bucket}/yelp/review/skewed_reviews/*.parquet"
    file_loc_business = f"s3a://{minio_bucket}/yelp/business/business/*.parquet"

    logger.info("Inicia o Metrics.")
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    logger.info("Executando a task.")

    logger.info("Leitura dos Datasets")
    df_reviews = spark.read.parquet(file_loc_reviews)
    df_business = spark.read.parquet(file_loc_business)

    df_reviews.createOrReplaceTempView("reviews")
    df_business.createOrReplaceTempView("business")

    logger.info("Desligando o broadcast automatica.")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)

    logger.info("Visualizando o Dataset Desbalanceado")
    # Dados em 2025-04-20: 34.951.400
    spark.sql("""
        SELECT r.date,
            COUNT(*) AS qtd
        FROM reviews r
        WHERE r.date = '2025-04-20'
        GROUP BY r.date
        ORDER BY qtd DESC
    """).show()


    logger.info("Realizando o Join com Skew")
    start_time = time.time()
    # Tempo: 19seg
    df_join_skew = spark.sql("""
        SELECT r.review_id, r.user_id, r.date, b.name, b.city
        FROM reviews r
        JOIN business b
        ON r.business_id = b.business_id
        WHERE r.date = '2025-04-20'
        AND b.city = 'Philadelphia'
    """)
    df_join_skew.show()
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
