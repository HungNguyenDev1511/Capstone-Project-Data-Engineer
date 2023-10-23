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
    df = pd.read_parquet("/home/hungnguyen/Caption-Project/data/taxi_combined/part0/0-ec436d91-69d4-42db-b8d0-8ce835ba225b-0.parquet")
    df = spark.createDataFrame(pd)

    # Define the filter condition to select age
    # SELECT * FROM df WHERE trip_distance > 15.2.
    filter_condition = df["trip_distance"] > 15.2

    # Apply the filter, aggregate the age column and
    # calculate the average.
    filtered_df = df.filter(filter_condition)
    average_trip_distance = filtered_df.agg(avg("trip_distance")).collect()[0][0]

    """
    The recognizer can recognize that we first filter the data,
    then conduct the aggreation to take the average of the age column.
    The filter can be pushed down to the data source (normally a database) 
    to reduce the amount of data to process.
    """

    # Show the result
    filtered_df.show()
    print("Average trip_distance:", average_trip_distance)


if __name__ == "__main__":
    main()
