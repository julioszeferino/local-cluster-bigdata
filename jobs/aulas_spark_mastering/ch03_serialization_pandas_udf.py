"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_serialization_pandas_udf.py

Tempo de Execucao: 2.8 minutos


# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 12
numTasks => 69
elapsedTime => 169077 (2.8 min)
stageDuration => 167047 (2.8 min)
executorRunTime => 433306 (7.2 min)
executorCpuTime => 185491 (3.1 min)
executorDeserializeTime => 11307 (11 s)
executorDeserializeCpuTime => 4368 (4 s)
resultSerializationTime => 500 (0.5 s)
jvmGCTime => 12517 (13 s) [Tempo de Garbage Collection]
shuffleFetchWaitTime => 0 (0 ms)
shuffleWriteTime => 5140 (5 s)
resultSize => 626386 (611.7 KB)
diskBytesSpilled => 0 (0 Bytes)
memoryBytesSpilled => 0 (0 Bytes)
peakExecutionMemory => 7693010448
recordsRead => 186996354
bytesRead => 3059352333 (2.8 GB)
recordsWritten => 0
bytesWritten => 0 (0 Bytes)
shuffleRecordsRead => 50436781
shuffleTotalBlocksFetched => 79
shuffleLocalBlocksFetched => 41
shuffleRemoteBlocksFetched => 38
shuffleTotalBytesRead => 1794831665 (1711.7 MB) [Muito Shuffle!!!]
shuffleLocalBytesRead => 1452901822 (1385.6 MB)
shuffleRemoteBytesRead => 341929843 (326.1 MB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 1794831665 (1711.7 MB)
shuffleRecordsWritten => 50436781

Average number of active tasks => 2.6

Stages and their duration:
Stage 0 duration => 1732 (2 s)
Stage 1 duration => 1829 (2 s)
Stage 2 duration => 2332 (2 s)
Stage 3 duration => 2314 (2 s)
Stage 4 duration => 47657 (48 s)
Stage 5 duration => 296 (0.3 s)
Stage 6 duration => 283 (0.3 s)
Stage 7 duration => 33571 (34 s)
Stage 8 duration => 27770 (28 s)
Stage 9 duration => 37737 (38 s)
Stage 11 duration => 11396 (11 s)
Stage 14 duration => 130 (0.1 s)
Metricas elapsedTime = 169077

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


def license_num(num):
    """
    :param num: The license number of the ride-sharing service
    :return: The name of the ride-sharing service associated with the given license number
    """

    if num == 'HV0002':
        return 'Juno'
    elif num == 'HV0003':
        return 'Uber'
    elif num == 'HV0004':
        return 'Via'
    elif num == 'HV0005':
        return 'Lyft'
    else:
        return 'Unknown'


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

    logger.info("Uso do pandas UDF")
    start_time = time.time()
    udf_license_num = udf(license_num, StringType())
    spark.udf.register("license_num", udf_license_num)
    df_fhvhv = (
        df_fhvhv
        .withColumn(
            'hvfhs_license_num', 
            udf_license_num(df_fhvhv['hvfhs_license_num'])
            )
    )

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
        ORDER BY request_datetime DESC
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
    logger.info(f"time taken [pandas udf]: {end_time - start_time} seconds")

    logger.info("Stage Metrics Report: ")
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"Metricas elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
