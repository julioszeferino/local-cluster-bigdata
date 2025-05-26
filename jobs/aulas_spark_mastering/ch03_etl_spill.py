"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch03_etl_spill.py

Tempo de Execucao: 6.3 min


# ------------------- * -------------------- * -------------------
Stage Metrics Report:

Scheduling mode = FIFO
Spark Context default degree of parallelism = 6

Aggregated Spark stage metrics:
numStages => 36
numTasks => 430
elapsedTime => 380333 (6.3 min)
stageDuration => 411323 (6.9 min)
executorRunTime => 1747993 (29 min)
executorCpuTime => 1454383 (24 min)
executorDeserializeTime => 15872 (16 s)
executorDeserializeCpuTime => 6312 (6 s)
resultSerializationTime => 1519 (2 s)
jvmGCTime => 38709 (39 s)
shuffleFetchWaitTime => 5 (5 ms)
shuffleWriteTime => 24803 (25 s)
resultSize => 245260 (239.5 KB)
diskBytesSpilled => 8892499195 (8.3 GB)
memoryBytesSpilled => 44090503392 (41.1 GB)
peakExecutionMemory => 43973630176
recordsRead => 300090318
bytesRead => 8474434128 (7.9 GB)
recordsWritten => 750000111
bytesWritten => 7413807442 (6.9 GB)
shuffleRecordsRead => 900000421
shuffleTotalBlocksFetched => 2085
shuffleLocalBlocksFetched => 686
shuffleRemoteBlocksFetched => 1399
shuffleTotalBytesRead => 12731831930 (11.9 GB)
shuffleLocalBytesRead => 4405447819 (4.1 GB)
shuffleRemoteBytesRead => 8326384111 (7.8 GB)
shuffleRemoteBytesReadToDisk => 0 (0 Bytes)
shuffleBytesWritten => 12731831930 (11.9 GB)
shuffleRecordsWritten => 900000421

Average number of active tasks => 4.6

Stages and their duration:
Stage 0 duration => 33412 (33 s)
Stage 1 duration => 834 (0.8 s)
Stage 2 duration => 1546 (2 s)
Stage 3 duration => 33765 (34 s)
Stage 4 duration => 1747 (2 s)
Stage 6 duration => 213 (0.2 s)
Stage 7 duration => 116 (0.1 s)
Stage 8 duration => 89 (89 ms)
Stage 10 duration => 49 (49 ms)
Stage 11 duration => 334 (0.3 s)
Stage 12 duration => 112 (0.1 s)
Stage 14 duration => 169 (0.2 s)
Stage 15 duration => 11315 (11 s)
Stage 16 duration => 11116 (11 s)
Stage 17 duration => 11109 (11 s)
Stage 20 duration => 25367 (25 s)
Stage 25 duration => 47517 (48 s)
Stage 26 duration => 7073 (7 s)
Stage 27 duration => 6861 (7 s)
Stage 28 duration => 6837 (7 s)
Stage 31 duration => 29301 (29 s)
Stage 36 duration => 53342 (53 s)
Stage 37 duration => 7004 (7 s)
Stage 38 duration => 6595 (7 s)
Stage 39 duration => 6616 (7 s)
Stage 42 duration => 31210 (31 s)
Stage 47 duration => 45383 (45 s)
Stage 48 duration => 5508 (6 s)
Stage 50 duration => 132 (0.1 s)
Stage 51 duration => 132 (0.1 s)
Stage 53 duration => 64 (64 ms)
Stage 54 duration => 110 (0.1 s)
Stage 56 duration => 54 (54 ms)
Stage 57 duration => 55 (55 ms)
Stage 58 duration => 69 (69 ms)
Stage 59 duration => 26167 (26 s)
2025-04-27 15:58:53 - utils.logger - INFO - None
Metricas elapsedTime = 380333
2025-04-27 15:58:53 - utils.logger - INFO - Script concluído.
2025-04-27 15:58:53 - py4j.clientserver - INFO - Closing down clientserver connection

# ------------------- * -------------------- * -------------------



====================================================================
"""
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics, TaskMetrics


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

    spark = sessao_spark('ch03-etl-spill')

    logger = setup_logger()

    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("ERROR")
    minio_bucket = "production/landing/parquet"

    logger.info("Inicia o Metrics Stage e Task.")
    stage_metrics = StageMetrics(spark)
    task_metrics = TaskMetrics(spark)
    stage_metrics.begin()
    task_metrics.begin()

    logger.info("Executando a task.")

    logger.info("Gera dataset de transacoes")
    df_transactions = spark.range(0, 150_000_000, 1, 32) \
    .select('id',
            round(rand() * 10000, 2).alias('amount'),
            (col('id') % 10).alias('country_id'),
            (col('id') % 100).alias('store_id')
    )
    df_transactions.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transactions/")
    
    logger.info("Gera dataset de stores")
    df_stores = spark.range(0, 99) \
    .select('id',
            round(rand() * 100, 0).alias('employees'),
            (col('id') % 10).alias('country_id'),
            expr('uuid()').alias('name')
    )
    df_stores.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/stores/")


    logger.info("Gera dataset de countries")
    columns = ["id", "name"]
    countries = [
        (0, "Italy"),
        (1, "Canada"),
        (2, "Mexico"),
        (3, "China"),
        (4, "Germany"),
        (5, "UK"),
        (6, "Japan"),
        (7, "Korea"),
        (8, "Australia"),
        (9, "France"),
        (10, "Spain"),
        (11, "USA")
    ]

    # TODO build countries table
    df_countries = spark.createDataFrame(data=countries, schema=columns)
    df_countries.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/countries/")


    logger.info("Desliga o broadcast automatico")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)


    logger.info("Cache nos datasets views")
    df_transactions.createOrReplaceTempView("transactions")
    df_stores.createOrReplaceTempView("stores")
    df_countries.createOrReplaceTempView("countries")

    spark.sql("CACHE TABLE transactions")
    spark.sql("CACHE TABLE stores")
    spark.sql("CACHE TABLE countries")

    logger.info("Join dos Datasets")
    start_time = time.time()
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN stores
        ON transactions.store_id = stores.id
        LEFT JOIN countries
        ON transactions.country_id = countries.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    logger.info("Tempo de execução do join: %s segundos", end_time - start_time)


    logger.info("Configura o shuffle partitions para 8")
    spark.conf.set("spark.sql.shuffle.partitions", 8)

    logger.info("Join dos Datasets")
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN stores
        ON transactions.store_id = stores.id
        LEFT JOIN countries
        ON transactions.country_id = countries.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    logger.info("Tempo de execução do join [shuffle partitions to 8]: %s segundos", end_time - start_time)


    logger.info("Alterando a ordem dos joins")
    start_time = time.time()
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN countries
        ON transactions.country_id = countries.id
        LEFT JOIN stores
        ON transactions.store_id = stores.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    logger.info("Tempo de execução do join [order changed]: %s segundos", end_time - start_time)


    logger.info("Computando as estatisticas e ativando o broadcast")
    spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

    spark.sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS country_id, store_id")
    spark.sql("ANALYZE TABLE stores COMPUTE STATISTICS FOR COLUMNS id")
    spark.sql("ANALYZE TABLE countries COMPUTE STATISTICS FOR COLUMNS id")

    logger.info("Join dos Datasets")
    start_time = time.time()
    joined_df = spark.sql("""
        SELECT 
            transactions.id,
            amount,
            countries.name as country_name,
            employees,
            stores.name as store_name
        FROM transactions
        LEFT JOIN countries
        ON transactions.country_id = countries.id
        LEFT JOIN stores
        ON transactions.store_id = stores.id
    """)

    joined_df.write.format("parquet").mode("overwrite").save(f"s3a://{minio_bucket}/transact_countries/")
    end_time = time.time()
    logger.info("Tempo de execução do join [statistics computed]: %s segundos", end_time - start_time)


    logger.info("Stage Metrics Report: ")
    stage_metrics.end()
    task_metrics.end()
    
    logger.info(stage_metrics.print_report())

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"Metricas elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()
