from pyspark.sql import SparkSession
import argparse


def main(args):
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.ui.port", "4050")
        .appName("Python Spark read parquet example")
        .getOrCreate()
    )

    # Read from a parquet file (transformation)
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    df = spark.read.parquet(args.input)

    # Set the number of partitions with repartitions
    num_partitions = 4  # Replace with the desired number of partitions
    df = df.repartition(
        num_partitions
    )  # Default using hash-based partition, which can potentially be lead to skew problem!

    # Show data (action)
    df.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        default="/home/hungnguyen/Caption-Project/data/taxi_combined/part0",
        help="Data file in parquet format.",
    )
    args = parser.parse_args()
    main(args)
