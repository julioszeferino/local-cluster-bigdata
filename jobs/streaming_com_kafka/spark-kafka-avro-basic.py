from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Initialize Spark session with Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkRead") \
    .config("spark.driver.host", "localhost") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1") \
    .getOrCreate()

kafka_bootstrap_servers = '138.197.228.187:9094'  # Correct Kafka broker port
schema_registry_url = 'http://165.227.254.142:8081'  # Schema Registry URL
driver_topic = 'src-app-drivers'

def get_spark_schema_from_registry(schema_registry_url, topic):
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    schema_id = schema_registry_client.get_latest_version(f"{topic}-value").schema_id
    avro_schema_str = schema_registry_client.get_schema(schema_id).schema_str

    # Parse the Avro schema JSON string
    avro_schema = json.loads(avro_schema_str)

    # Convert the Avro schema to Spark StructType
    fields = []
    for field in avro_schema['fields']:
        field_name = field['name']
        field_type = field['type']

        if field_type == 'int':
            spark_type = IntegerType()
        elif field_type == 'string':
            spark_type = StringType()
        elif field_type == 'float':
            spark_type = FloatType()
        else:
            raise ValueError(f"Unsupported field type: {field_type}")

        fields.append(StructField(field_name, spark_type, True))

    return StructType(fields)

# Get Spark schema from Schema Registry
spark_schema = get_spark_schema_from_registry(schema_registry_url, driver_topic)

# Read from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", driver_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Load the Avro schema from the Schema Registry for drivers
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
driver_schema_id = schema_registry_client.get_latest_version(f"{driver_topic}-value").schema_id
driver_avro_schema = schema_registry_client.get_schema(driver_schema_id).schema_str

# Parse the Avro data
parsed_driver_df = kafka_df.select(
    from_avro(
        col("value"),
        driver_avro_schema
    ).alias("driver")
)

# Process the parsed DataFrame for drivers using the dynamically fetched schema
processed_driver_df = parsed_driver_df.select(
    col("driver.driver_id"),
    col("driver.name"),
    col("driver.license_number"),
    col("driver.rating")
)

