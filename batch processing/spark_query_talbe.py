from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Python Spark query optimizer")
        .getOrCreate()
    )

    # Read Data from parquet file
    taxi_data = spark.read.parquet("/home/hungnguyen/Caption-Project/data/taxi_combined/part0/0-ec436d91-69d4-42db-b8d0-8ce835ba225b-0.parquet")

    # Register the DataFrame as a temporary SQL table
    taxi_data.createOrReplaceTempView("taxi_data")

    # Run SQL queries on the DataFrame
    querry = spark.sql("SELECT * FROM taxi_data WHERE passenger_count > 0")

    # Show the result
    querry.show()


if __name__ == "__main__":
    main()


