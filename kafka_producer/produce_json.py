import argparse
import io
import json
from datetime import datetime
from time import sleep
import random

import numpy as np
from bson import json_util
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events. Setup will teardown before beginning emitting events.",
)
parser.add_argument(
    "-b",
    "--bootstrap_servers",
    default="localhost:9092",
    help="Where the bootstrap server is",
)
parser.add_argument(
    "-c",
    "--schemas_path",
    default="./avro_schemas",
    help="Folder containing all generated avro schemas",
)

args = parser.parse_args()

# Define some constants
NUM_TAXI = 1


def create_topic(admin, topic_name):
    # Create topic if not exists
    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin.create_topics([topic])
        print(f"A new topic {topic_name} has been created!")
    except Exception:
        print(f"Topic {topic_name} already exists. Skipping creation!")
        pass


def create_streams(servers, schemas_path):
    producer = None
    admin = None
    for _ in range(10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            admin = KafkaAdminClient(bootstrap_servers=servers)
            print("SUCCESS: instantiated Kafka admin and producer")
            break
        except Exception as e:
            print(
                f"Trying to instantiate admin and producer with bootstrap servers {servers} with error {e}"
            )
            sleep(10)
            pass

    while True:
        record = {
            "schema": {
            "type": "struct",
                "fields": [
                {
                    "type": "int64",
                    "optional": False,
                    "field": "VendorID"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "tpep_pickup_datetime"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "tpep_dropoff_datetime"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "passenger_count"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "trip_distance"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "RatecodeID"
                },
                {
                    "type": "string",
                    "optional": False,
                    "field": "store_and_fwd_flag"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "PULocationID"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "DOLocationID"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "payment_type"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "fare_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "extra"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "mta_tax"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "tip_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "tolls_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "improvement_surcharge"
                },
                {
                    "type": "int64",
                    "optional": False,
                    "field": "total_amount"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "congestion_surcharge"
                },
                {
                    "type": "double",
                    "optional": False,
                    "field": "airport_fee"
                }
                ]
            }
        }
        record["payload"] = {}

        record["payload"]["taxi_id"] = random.randint(0, NUM_TAXI)
        record["payload"]["VendorID"] = random.randint(1, 2)
        # Make event one more year recent to simulate fresher data
        record["payload"]["tpep_pickup_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        record["payload"]["tpep_dropoff_datetime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        record["payload"]["passenger_count"] = random.randint(  0, 5)
        record["payload"]["trip_distance"] = random.uniform (0.0, 30.0)
        record["payload"]["RatecodeID"] = random.randint(  0, 2)
        record["payload"]["store_and_fwd_flag"] =random.choice(["Y","N"])
        record["payload"]["PULocationID"] = random.randint( 0, 300)
        record["payload"]["DOLocationID"] = random.randint( 0, 300)
        record["payload"]["payment_type"] = random.randint( 0, 70.0)
        record["payload"]["fare_amount"] = random.uniform ( 0, 70.0)
        record["payload"]["extra"] = random.uniform(0 , 2.5)
        record["payload"]["mta_tax"] = random.uniform (0, 0.5)
        record["payload"]["tip_amount"] = random.uniform(  0, 20.00)
        record["payload"]["tolls_amount"] = random.uniform(0, 6.55)
        record["payload"]["improvement_surcharge"] = random.uniform(-0.3, 0.3)
        record["payload"]["total_amount"] = random.randint(  0.00, 30.00)
        record["payload"]["congestion_surcharge"] = random.uniform(0 , 2.5)
        record["payload"]["airport_fee"] = random.uniform(0 , 2.5)

        # Read columns from schema
        # schema_path = f"{schemas_path}/schema_{record['taxi_id']}.avsc"
        # with open(schema_path, "r") as f:
        #     parsed_schema = json.loads(f.read())

#         for field in parsed_schema["fields"]:
#             if field["name"] not in ["taxi_id", "VendorID","tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance","RatecodeID"
# ,"store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge","total_amount"
# , "congestion_surcharge", "Airport_fee"]:
#                 record[field["taxi_id"]] = np.random.rand()

        # Get topic name for this taxo
        topic_name = f'taxi_0'

        # Create a new topic for this taxi id if not exists
        create_topic(admin, topic_name=topic_name)

        # Send messages to this topic
        producer.send(
            topic_name, json.dumps(record, default=json_util.default).encode("utf-8")
        )
        print(record)
        sleep(2)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    servers = parsed_args["bootstrap_servers"]

    # Tear down all previous streams
    print("Tearing down all existing topics!")
    for taxi_id in range(NUM_TAXI):
        try:
            teardown_stream(f"taxi_{taxi_id}", [servers])
        except Exception as e:
            print(f"Topic taxi_{taxi_id} does not exist. Skipping...!")

    if mode == "setup":
        schemas_path = parsed_args["schemas_path"]
        create_streams([servers], schemas_path)
