import argparse
import json
import os
import random
import shutil

import numpy as np


def main(args):
    # Clean up the avro schema folder if exists
    if os.path.exists(args["schema_folder"]):
        shutil.rmtree(args["schema_folder"])

    os.mkdir(args["schema_folder"])

    for schema_idx in range(args["num_schemas"]):
        num_features = np.random.randint(args["min_features"], args["max_features"])
        sampled_features = random.sample(range(args["max_features"]), num_features)

        # Initialize schema template
        schema = {
            "doc": "Sample schema to help you get started.",
            "fields": [
                {"name": "VendorID", "type": "string"},
                {"name": "tpep_pickup_datetime", "type": "string"},
                {"name": "tpep_dropoff_datetime", "type": "string"},
                {"name": "passenger_count", "type": "decimal"},
                {"name": "trip_distance", "type": "DECIMAL"},
                {"name": "RatecodeID", "type": "DECIMAL"},
                {"name": "store_and_fwd_flag", "type": "string"},
                {"name": "PULocationID", "type": "string"},
                {"name": "DOLocationID", "type": "string"},
                {"name": "payment_type", "type": "string"},
                {"name": "fare_amount", "type": "DECIMAL"},
                {"name": "extra", "type": "DECIMAL"},
                {"name": "mta_tax", "type": "DECIMAL"},
                {"name": "tip_amount", "type": "DECIMAL"},
                {"name": "tolls_amount", "type": "DECIMAL"},
                {"name": "improvement_surcharge", "type": "string"},
                {"name": "total_amount", "type": "DECIMAL"},
                {"name": "congestion_surcharge", "type": "DECIMAL"},
                {"name": "Airport_fee", "type": "DECIMAL"},
            ],
            "name": "Taxi",
            "namespace": "example.avro",
            "type": "record",
        }

        # Add new features to the schema template
        for feature_idx in sampled_features:
            schema["fields"].append({"name": f"feature_{feature_idx}", "type": "float"})

        # Write this schema to the Avro output folder
        with open(f'{args["schema_folder"]}/schema_{schema_idx}.avsc', "w+") as f:
            json.dump(schema, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_schemas",
        default=1,
        type=int,
        help="Number of avro schemas to generate.",
    )
    parser.add_argument(
        "-m",
        "--min_features",
        default=2,
        type=int,
        help="Minumum number of features for each taxi",
    )
    parser.add_argument(
        "-a",
        "--max_features",
        default=10,
        type=int,
        help="Maximum number of features for each taxi",
    )
    parser.add_argument(
        "-o",
        "--schema_folder",
        default="./avro_schemas",
        help="Folder containing all generated avro schemas",
    )
    args = vars(parser.parse_args())
    main(args)
