"""
====================================================================
docker exec dsa-spark-master spark-submit --deploy-mode client \
    ./apps/aulas_spark_mastering/ch02_skew_e_sparkmetrics_b.py


RODAR O ARQUIVO A ANTES PARA GERAR OS DADOS COM SKEW!!!

====================================================================
"""
import os

from sparkmeasure import StageMetrics

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, lit, rand, concat

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

    spark = sessao_spark('ch02-skew-e-sparkmetrics-b')

    logger = setup_logger()
    logger.info("Iniciando o script.")

    # configs
    logger.info(spark)
    logger.info(f"Configs: {SparkConf().getAll()}")
    spark.sparkContext.setLogLevel("INFO")


    file_loc_reviews = "production/landing/parquet/yelp/review/skewed_reviews/*.parquet"
    file_loc_business = "production/landing/parquet/yelp/business/business/*.parquet"

    logger.info("Inicia o Metrics.")
    stage_metrics = StageMetrics(spark)
    stage_metrics.begin()


    logger.info("Executando a task.")

    logger.info("Leitura do Arquivo PARQUET")
    df_reviews = spark.read.parquet(f"s3a://{file_loc_reviews}")
    df_business = spark.read.parquet(f"s3a://{file_loc_business}")

    df_reviews.createOrReplaceTempView("reviews")
    df_business.createOrReplaceTempView("business")

    spark.sql("""
        SELECT r.date,
            COUNT(*) AS qtd
        FROM reviews r
        WHERE date(r.date) = '2025-04-20'
        GROUP BY r.date
        ORDER BY qtd DESC
    """).show()


    
    # Tecnica de Salting
    # logger.info("Aplicando o Salting.")
    # salt_factor = 400
    # df_reviews_salt = df_reviews.withColumn(
    #     "salt_date", concat(col("date"), lit("_"), (rand() * salt_factor).cast("int"))).withColumn(
    #     "salt_business_id", concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
    # )

    # df_business_salt = df_business.withColumn(
    #     "salt_business_id", concat(col("business_id"), lit("_"), (rand() * salt_factor).cast("int"))
    # )

    # logger.info("Salting aplicado.")
    # print(df_reviews_salt.limit(10).show())
    # print(df_business_salt.limit(10).show())


    # df_reviews_salt.createOrReplaceTempView("reviews")
    # df_business_salt.createOrReplaceTempView("business")

    # df_join_skew = spark.sql("""
    #     SELECT r.review_id, r.user_id, r.date, b.name, b.city
    #     FROM reviews r
    #     JOIN business b
    #     ON r.salt_business_id = b.salt_business_id
    #     WHERE r.salt_date = '2025-04-20%'
    # """)
    # print(f"[salt]: {df_join_skew.take(10)}")

    stage_metrics.end()
    stage_metrics.print_report()

    metrics = stage_metrics.aggregate_stagemetrics()
    print(f"metrics elapsedTime = {metrics.get('elapsedTime')}")

    logger.info("Script concluído.")
    spark.stop()



if __name__ == "__main__":
    main()