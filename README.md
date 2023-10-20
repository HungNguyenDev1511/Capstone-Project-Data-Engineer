# How-to Guide

## Start our data lake infrastructure
```shell
docker compose -f docker-compose.yml -d
```

## Generate data and push them to MinIO
### 1. Generate data
```shell
python utils/generate_fake_data.py
```
### 2. Push data to MinIO
```shell
python utils/export_data_to_datalake.py
```

**Note:** Don't forget to install dependencies from `requirements.txt` first (and we use `python 3.9`).

## Create data schema
After putting your files to `MinIO``, please execute `trino` container by the following command:
```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an interactive mode

After that, run the following command to register a new schema for our data:

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.nyc_taxi
WITH (location = 's3://nyc-taxi/');

CREATE TABLE IF NOT EXISTS lakehouse.nyc_taxi.taxi (
  VendorID VARCHAR(50),
  tpep_pickup_datetime VARCHAR (50),
  tpep_dropoff_datetime VARCHAR (50),
  passenger_count INT,
  trip_distance DECIMAL,
  RatecodeID DECIMAL, 
  store_and_fwd_flag VARCHAR(50), 
  PULocationID VARCHAR(50),
  DOLocationID VARCHAR(50), 
  payment_type VARCHAR(50), 
  fare_amount DECIMAL, 
  extra DECIMAL, 
  mta_tax DECIMAL, 
  tip_amount DECIMAL, 
  tolls_amount DECIMAL, 
  improvement_surcharge VARCHAR(50),
  total_amount DECIMAL,
  congestion_surcharge DECIMAL, 
  airport_fee DECIMAL
) WITH (
  location = 's3://nyc-taxi/nyc-taxi/taxi_combined/part0'
);
```

## Query with DBeaver
1. Install `DBeaver` as in the following [guide](https://dbeaver.io/download/)
2. Connect to our database (type `trino`) using the following information (empty `password`):
  ![DBeaver Trino](./imgs/trino.png)