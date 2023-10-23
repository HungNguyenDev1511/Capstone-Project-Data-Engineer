from pyspark.sql import SparkSession
import logging
import sys

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logger = logging.getLogger("py_basic_rdd")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(log_format)

stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def main():
    # The entrypoint to access all functions of Spark
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Python Spark RDD example")
        .getOrCreate()
    )
    sc = spark.sparkContext

    # Get Log4j JVM Object and set log level to your log level
    # Notice the level of logs: DEBUG, INFO, WARN, ERROR, FATAL
    # Read more about it here:https://polarpersonal.medium.com/writing-pyspark-logs-in-apache-spark-and-databricks-8590c28d1d51
    log4jLogger = sc._jvm.org.apache.log4j
    my_logger = log4jLogger.LogManager.getLogger("basic_rdd")
    my_logger.setLevel(log4jLogger.Level.DEBUG)
    my_logger.info("My application is working well!")

    # Create your first RDD
    my_rdd = sc.parallelize([0, 2, 3, 4, 10, 1], numSlices=2)

    # Number of elements in the RDD
    print(my_rdd.count())

    # Verify number of partitions is equal to numSlices
    print(my_rdd.getNumPartitions())


if __name__ == "__main__":
    main()
