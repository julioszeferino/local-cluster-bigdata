"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client ./apps/math/spark-measure/teste_save_metrics.py

====================================================================
"""
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
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
        # -- Adaptive Query Execution -- #
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.adaptive.coalescePartitions.enabled", True)
        .config("spark.sql.adaptive.skewJoin.enabled", True)
        .getOrCreate()
    )

    return spark


def main():

    nome_job = "teste-metrics"
    spark = sessao_spark(f'{nome_job}')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")
    storage = "/opt/spark/data"

    logger.info("Iniciando o Metrics")
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()

    logger.info("Executando a task.")

    logger.info("Leitura do Arquivo")
    arquivo = f"{storage}/clientes.csv"
    df = spark.read.option("delimiter", ",").option("header", True).csv(arquivo)

    df.createOrReplaceTempView("clientes")

    df_clientes = spark.sql("""
        SELECT id, nome, idade, cidade
        FROM clientes
        WHERE idade > 30
    """)


    df_clientes.show()
    df_clientes.explain(True)

    logger.info("Encerra o Metrics")
    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    metrics = f"{storage}/metrics/{nome_job}/"
    df_stage_metrics = stage_metrics.create_stagemetrics_DF("PerfStageMetrics")
    df_stage_metrics.repartition(1).orderBy("jobId", "stageId").write.mode("overwrite").json(metrics + "stagemetrics")

    df_aggregated_metrics = stage_metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    df_aggregated_metrics.write.mode("overwrite").json(metrics + "stagemetrics_agg")


    logger.info("Script concluído.")
    spark.stop()


if __name__ == "__main__":
    main()