import json
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink,
    KafkaSource)

JARS_PATH = f"{os.getcwd()}/data_ingestion/kafka_connect/jars/"


def merge_features(record):
    """
    Merged feature columns into one single data column
    and keep other columns unchanged.
    """
    # Convert Row to dict
    record = json.loads(record)

    # Create a dictionary of all features
    # and create a data column for this
    data = {}
    for key in record:
        if key.startswith("feature"):
            data[key] = record[key]

    # Convert the data column to string
    # and add other features back to record
    return json.dumps(
        {
            "VendorID": record["VendorID"],
            "tpep_pickup_datetime": record["tpep_pickup_datetime"],
            "tpep_dropoff_datetime ": record["tpep_dropoff_datetime "],
            "passenger_count ": record["passenger_count "],
            "trip_distance": record["trip_distance"],
            "RatecodeID": record["RatecodeID"],
            "store_and_fwd_flag": record["store_and_fwd_flag"],
            "PULocationID": record["PULocationID"],
            "DOLocationID": record["DOLocationID"],
            "payment_type ": record["payment_type"],
            "fare_amount ": record["fare_amount"],
            "extra": record["extra"],
            "mta_tax ": record["mta_tax"],
            "tip_amount ": record["tip_amount "],
            "tolls_amount ": record["tolls_amount "],
            "total_amount ": record["total_amount "],
            "congestion_surcharge": record["congestion_surcharge"],
            "Airport_fee": record["Airport_fee"],
            "data": data,
        }
    )


def filter_small_features(record):
    """
    Skip records containing a feature that is smaller than 0.5.
    """
    # Convert Row to dict
    record = json.loads(record)

    # for key in record:
    #     if key.startswith("feature"):
    #         if record[key] < 0.5:
    #             return False

    print("Found record: ", record)
    return True


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # The other commented lines are for Avro format
    env.add_jars(
        f"file://{JARS_PATH}/flink-connector-kafka-1.17.1.jar",
        # f"file://{JARS_PATH}/flink-avro-1.17.1.jar",
        # f"file://{JARS_PATH}/flink-avro-confluent-registry-1.17.1.jar",
        # f"file://{JARS_PATH}/avro-1.11.1.jar",
        # f"file://{JARS_PATH}/jackson-databind-2.14.2.jar",
        # f"file://{JARS_PATH}/jackson-core-2.14.2.jar",
        # f"file://{JARS_PATH}/jackson-annotations-2.14.2.jar",
        f"file://{JARS_PATH}/kafka-clients-3.4.0.jar",
        # f"file://{JARS_PATH}/kafka-schema-registry-client-5.3.0.jar",
    )

    # Avro will need it for validation from the schema registry
    # schema_path = "./data_ingestion/kafka_producer/avro_schemas/schema_0.avsc"
    # with open(schema_path) as f:
    #     schema = f.read()

    # Define the source to take data from
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("taxi_0")
        .set_group_id("taxi-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Define the sink to save the processed data to
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("sink_ds_taxi_0")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # No sink, just print out to the terminal
    # env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
    #     filter_small_features
    # ).map(merge_features).print()

    # Add a sink to be more industrial, remember to cast to STRING in map
    # it will not work if you don't do it
    env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source").filter(
        filter_small_features
    ).map(merge_features, output_type=Types.STRING()).sink_to(sink=sink)

    # Execute the job
    env.execute("flink_datastream_demo")
    print("Your job has been started successfully!")


if __name__ == "__main__":
    main()
