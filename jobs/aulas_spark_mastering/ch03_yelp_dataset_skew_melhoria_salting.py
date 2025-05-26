"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_yelp_dataset_skew_melhoria_salting.py

Tempo de Execucao: 49 segundos

Salting> Randomizar para deixar os dados homogeneas


# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 14
numTasks => 716
elapsedTime => 57612 (58 s)
stageDuration => 52300 (52 s)
executorRunTime => 238642 (4.0 min)
executorCpuTime => 70064 (1.2 min)
executorDeserializeTime => 23616 (24 s)
executorDeserializeCpuTime => 7471 (7 s)
resultSerializationTime => 931 (0.9 s)
jvmGCTime => 7566 (8 s)
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 1943 (2 s)
resultSize => 10015677 (9.6 MB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 9909825872
recordsRead => 77051618
bytesRead => 2859029043 (2.7 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 195
shuffleTotalBlocksFetched => 195
shuffleLocalBlocksFetched => 67
shuffleRemoteBlocksFetched => 128
shuffleTotalBytesRead => 15930 (15.6 KB) [Saiu de 46.1 MB para 15.6 KB]
shuffleLocalBytesRead => 5471 (5.3 KB)
shuffleRemoteBytesRead => 10459 (10.2 KB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 15930 (15.6 KB) [Saiu de 279.6 MB para 15.6 KB]
shuffleRecordsWritten => 195

Average number of active tasks => 4.1

Stages and their duration:
Stage 0 duration => 10334 (10 s)
Stage 1 duration => 482 (0.5 s)
Stage 2 duration => 465 (0.5 s)
Stage 3 duration => 16391 (16 s)
Stage 5 duration => 466 (0.5 s)
Stage 6 duration => 438 (0.4 s)
Stage 7 duration => 169 (0.2 s)
Stage 8 duration => 339 (0.3 s)
Stage 9 duration => 494 (0.5 s) [Saiu de 16 s para 0.5 s]
Stage 10 duration => 695 (0.7 s)
Stage 11 duration => 1063 (1 s)
Stage 12 duration => 2115 (2 s)
Stage 13 duration => 9520 (10 s)
Stage 14 duration => 9329 (9 s)
Metricas elapsedTime = 57612

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


    logger.info("Aplicando a tecnica de Salting")
    salt_factor = 400

    # cria uma coluna com a data_[valor salting aleatorio] (coluna com problema)
    # cria uma coluna com o business_id_[valor salting aleatorio] (Key do join)
    df_reviews_salt = (
        df_reviews
        .withColumn(
            "salt_date", 
            concat(col("date"), lit("_"), (rand() * salt_factor).cast("int"))
        )
        .withColumn(
            "salt_business_id", 
            concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
        )
    )
    print(df_reviews_salt.limit(10).show())


    # cria a chave de salting no outro dataset business_id_[valor salting aleatorio]
    df_business_salt = (
        df_business
        .withColumn(
            "salt_business_id", 
            concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
        )
    )
    print(df_business_salt.limit(10).show())

    df_reviews_salt.createOrReplaceTempView("reviews")
    df_business_salt.createOrReplaceTempView("business")


    logger.info("Realizando o Join sem Skew")
    start_time = time.time()
    # Tempo: 19seg
    df_join_skew = spark.sql("""
        SELECT r.review_id, r.user_id, r.date, b.name, b.city
        FROM reviews r
        JOIN business b
        ON r.salt_business_id = b.salt_business_id
        WHERE r.salt_date = '2024-07-02%'
    """)
    print(f"[salt]: {df_join_skew.take(10)}")
    end_time = time.time()
    logger.info("Tempo de execução do join: %s segundos", end_time - start_time)

    # TODO add .coalesce(200) before writing into storage

    logger.info("Stage Metrics Report: ")
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"Metricas elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
