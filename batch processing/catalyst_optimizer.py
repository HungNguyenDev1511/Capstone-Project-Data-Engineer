from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import pandas as pd 

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

    # Define the filter condition to select age
    # greater than 25. This operation corresponds to the SQL
    # SELECT * FROM df WHERE age > 25.
    filter_condition = df["age"] > 25

    # Apply the filter, aggregate the age column and
    # calculate the average.
    filtered_df = df.filter(filter_condition)
    average_age = filtered_df.agg(avg("age")).collect()[0][0]

    """
    The recognizer can recognize that we first filter the data,
    then conduct the aggreation to take the average of the age column.
    The filter can be pushed down to the data source (normally a database) 
    to reduce the amount of data to process.
    """

    # Show the result
    filtered_df.show()
    print("Average Age:", average_age)


if __name__ == "__main__":
    main()
