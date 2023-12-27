import os

from pyflink.table import *
from pyflink.table.expressions import col

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"

# environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode()
)
t_env.get_config().set(
    "pipeline.jars",
    f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-table-api-java-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-confluent-registry-1.17.1.jar;"
    + f"file://{JARS_PATH}/flink-avro-1.17.1.jar;"
    + f"file://{JARS_PATH}/avro-1.11.1.jar;"
    + f"file://{JARS_PATH}/jackson-databind-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-core-2.14.2.jar;"
    + f"file://{JARS_PATH}/jackson-annotations-2.14.2.jar;"
    + f"file://{JARS_PATH}/kafka-schema-registry-client-5.3.0.jar;"
    + f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
)

# register Taxi table sink in table environment
source_ddl = f"""
    CREATE TABLE taxi (
        VendorID string,
        tpep_pickup_datetime datetime,
        tpep_dropoff_datetime datetime,
        passenger_count float,
        trip_distance float,
        RatecodeID float, 
        store_and_fwd_flag string 
        PULocationID string
        DOLocationID string, 
        payment_type string, 
        fare_amount float, 
        extra float, 
        mta_tax float, 
        tip_amount float, 
        tolls_amount float, 
        improvement_surcharge string,
        total_amount float,
        congestion_surcharge float, 
        Airport_fee float
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'taxi',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'value.format' = 'json'
    )
    """
t_env.execute_sql(source_ddl)


# specify table program
taxis = t_env.from_path("taxi")

taxis.select(col("VendorID"), 
             col("tpep_pickup_datetime"), 
             col("tpep_dropoff_datetime"),
             col("passenger_count"),
             col("trip_distance"),
             col("RatecodeID"),
             col("store_and_fwd_flag"),
             col("PULocationID"),
             col("DOLocationID"),
             col("payment_type"),
             col("fare_amount"),
             col("extra"),
             col("mta_tax"),
             col("tip_amount"),
             col("tolls_amount"),
             col("improvement_surcharge"),
             col("total_amount"),
             col("congestion_surcharge"),
             col("Airport_fee"),).execute_insert(
    "taxi"
).wait()
