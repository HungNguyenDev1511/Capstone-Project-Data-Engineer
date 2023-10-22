from pyspark.sql import SparkSession
import argparse
import os
import dotenv
from pydeequ.profiles import ColumnProfilerRunner

dotenv.load_dotenv("../.env")


def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "../jars/postgresql-42.6.0.jar,../jars/deequ-2.0.3-spark-3.3.jar",
        )
        .appName("Python Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )

    # Read from PostgreSQL database via jdbc connection
    # https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
    df = (
        spark.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql:{os.getenv('POSTGRES_DB')}")
        .option("dbtable", "public.devices")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD"))
        .load()
    )

    # Profile data with Deequ
    # https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/
    # https://pydeequ.readthedocs.io/en/latest/README.html
    result = ColumnProfilerRunner(spark).onData(df).run()

    for col, profile in result.profiles.items():
        if col == "index":
            continue

        print("*" * 30)
        print(f"Column: {col}")
        print(profile)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-o",
        "--output",
        default="../data",
        help="Directory to save the data file in parquet format.",
    )
    args = parser.parse_args()
    main(args)