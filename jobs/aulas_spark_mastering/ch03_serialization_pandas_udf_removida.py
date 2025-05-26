"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_serialization_pandas_udf_removida.py

Tempo de Execucao: 30 segundos [Anterior: 2.8 minutos]

- Tiramos a UDF e substituimos por spark nativo
- Tiramos o orderby que estava gerando muito shuffle


# ------------------- * -------------------- * -------------------

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 10
numTasks => 22
elapsedTime => 30406 (30 s)
stageDuration => 27929 (28 s)
executorRunTime => 49582 (50 s)
executorCpuTime => 27901 (28 s)
executorDeserializeTime => 8070 (8 s)
executorDeserializeCpuTime => 3488 (3 s)
resultSerializationTime => 37 (37 ms)
jvmGCTime => 1761 (2 s) [Antes: 13 s]
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 34 (34 ms)
resultSize => 43204 (42.2 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 36877680
recordsRead => 50445974
bytesRead => 1234657027 (1177.5 MB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 6
shuffleTotalBlocksFetched => 3
shuffleLocalBlocksFetched => 1
shuffleRemoteBlocksFetched => 2
shuffleTotalBytesRead => 551 (551 Bytes) [Antes: 1711.7 MB]
shuffleLocalBytesRead => 183 (183 Bytes)
shuffleRemoteBytesRead => 368 (368 Bytes)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 551 (551 Bytes)
shuffleRecordsWritten => 6

Average number of active tasks => 1.6

Stages and their duration:
Stage 0 duration => 2757 (3 s)
Stage 1 duration => 2256 (2 s)
Stage 2 duration => 3315 (3 s)
Stage 3 duration => 3265 (3 s)
Stage 4 duration => 856 (0.9 s)
Stage 5 duration => 3614 (4 s)
Stage 6 duration => 751 (0.8 s)
Stage 7 duration => 441 (0.4 s)
Stage 8 duration => 10446 (10 s)
Stage 10 duration => 228 (0.2 s)
Metricas elapsedTime = 30406

# ------------------- * -------------------- * -------------------



====================================================================
"""
import os

import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, when

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


def hvfhs_license_num(df):
    """
    Transform the hvfhs_license_num field based on the following logic:

    - HV0002: Juno
    - HV0003: Uber
    - HV0004: Via
    - HV0005: Lyft

    :param df: Input DataFrame with Hvfhs_license_num field
    :return: DataFrame with transformed Hvfhs_license_num field
    """

    transformed_df = df.withColumn("hvfhs_license_num",
         when(col("hvfhs_license_num") == "HV0002", "Juno")
        .when(col("hvfhs_license_num") == "HV0003", "Uber")
        .when(col("hvfhs_license_num") == "HV0004", "Via")
        .when(col("hvfhs_license_num") == "HV0005", "Lyft")
        .otherwise(col("hvfhs_license_num"))
    )

    return transformed_df


def main():

    spark = sessao_spark('ch03-yelp-dataset-skew')

    logger = setup_logger()

    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    minio_bucket = "production/landing/tlc"

    file_fhvhv = f"s3a://{minio_bucket}/fhvhv/2022/*.parquet"
    file_zones = f"s3a://{minio_bucket}/zones/zones.csv"

    logger.info("Inicia o Metrics.")
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    logger.info("Executando a task.")

    logger.info("Leitura dos Datasets")
    df_fhvhv = spark.read.parquet(file_fhvhv)
    df_zones = spark.read.option("delimiter", ",").option("header", True).csv(file_zones)

    logger.info("Uso da Funcao Nativa Spark")
    start_time = time.time()
    df_fhvhv = df_fhvhv = hvfhs_license_num(df_fhvhv)

    df_fhvhv.createOrReplaceTempView("hvfhs")
    df_zones.createOrReplaceTempView("zones")

    df_rides = spark.sql("""
        SELECT hvfhs_license_num,
            zones_pu.Borough AS PU_Borough,
            zones_pu.Zone AS PU_Zone,
            zones_do.Borough AS DO_Borough,
            zones_do.Zone AS DO_Zone,
            request_datetime,
            pickup_datetime,
            dropoff_datetime,
            trip_miles,
            trip_time,
            base_passenger_fare,
            tolls,
            bcf,
            sales_tax,
            congestion_surcharge,
            tips,
            driver_pay,
            shared_request_flag,
            shared_match_flag
        FROM hvfhs
        INNER JOIN zones AS zones_pu
        ON CAST(hvfhs.PULocationID AS INT) = zones_pu.LocationID
        INNER JOIN zones AS zones_do
        ON hvfhs.DOLocationID = zones_do.LocationID
        -- ORDER BY request_datetime DESC
    """)

    df_rides.createOrReplaceTempView("rides")


    df_hvfhs_license_num = spark.sql("""
        SELECT 
            hvfhs_license_num,
            SUM(base_passenger_fare + tolls + bcf + sales_tax + congestion_surcharge + tips) AS total_fare,
            SUM(trip_miles) AS total_trip_miles,
            SUM(trip_time) AS total_trip_time
        FROM 
            rides
        GROUP BY 
            hvfhs_license_num
    """)

    df_rides.show()
    df_hvfhs_license_num.show()

    end_time = time.time()
    logger.info(f"time taken [spark-native]: {end_time - start_time} seconds")

    logger.info("Stage Metrics Report: ")
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"Metricas elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
