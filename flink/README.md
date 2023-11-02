This tutorial aims at running stream processing with Flink

# How-to guide
Please start the docker compose first using command

```shell
docker compose -f docker-compose.yaml up -d
```

then, you can check if Kafka producer is running normally by using

```shell
docker logs kafka_producer
```

Take the schema from these logs, to update the schema in the path `data_ingestion/kafka_producer/avro_schemas/schema_0.avsc`. 

This schema will be used to our Flink consumer.

# PyFlink guide

- Activate you conda environment and install required packages: 
    ```shell
    conda create -n flink python=3.8
    conda activate flink
    pip install -r requirements.txt
    ```
- Install Java JDK 11 following the section `Using apt
` in this [tutorial](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/generic-linux-install.html).
- Enjoy your programs by running the following command:
    ```shell
    python scripts/*.py # Replace * with your file
    ```

# More examples
Please take a look at [this source](https://github.com/apache/flink/blob/master/flink-python/pyflink/examples)