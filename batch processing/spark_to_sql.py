from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Python Spark query optimizer example")
        .getOrCreate()
    )

    # Create a DataFrame with user records
    data = [(1, "Alice", 28), (2, "Bob", 25), (3, "Charlie", 32), (4, "David", 22)]
    columns = ["user_id", "name", "age"]
    df = spark.createDataFrame(data, columns)

    # Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("people")

    # Run SQL queries on the DataFrame
    result = spark.sql("SELECT name, age FROM people WHERE name = 'Alice'")

    # Show the result
    result.show()


if __name__ == "__main__":
    main()