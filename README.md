Start: the data i work in this project is NYC taxi data. About dataset, Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP). You can dowload and use this dataset in here: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. The format data i use in this Project is Parquet file but you can work with CSV if you want. 

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
the meaning of this step in here is - you can import the data - parquet file - to the MINIO bucket to manager. you can also put the data to the MINIO by manual like this
![image](https://github.com/HungNguyenDev1511/Caption-Project/assets/69066161/c0c80f73-c8db-4e46-b320-6a42230b744f)

**Note:** Don't forget to install dependencies from `requirements.txt` first (and we use `python 3.9`).

## Create data schema
After putting your files to `MinIO``, please execute `trino` container by the following command:
```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an interactive mode

After that, run the following command to register a new schema for our data:

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.taxi
WITH (location = 's3://taxi/');

CREATE TABLE IF NOT EXISTS lakehouse.taxi.taxi (
  VendorID VARCHAR(50),
  tpep_pickup_datetime VARCHAR (50),
  tpep_dropoff_datetime VARCHAR (50),
  passenger_count DECIMAL,
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
  Airport_fee DECIMAL
) WITH (
  location = 's3://taxi/part0'
);
```

## Query with DBeaver
1. Install `DBeaver` as in the following [guide](https://dbeaver.io/download/)
2. Connect to our database (type `trino`) using the following information (empty `password`):
  ![DBeaver Trino](./imgs/trino.png)

Note: one thing your should notice in here is the string querry you use to create data should be match with the data you import in Minio. If you execute the create table fail and not match with the data you import on minio so DBeaver will response the error. And the last, the location you storage data - the parquet file - should be match with the location in the string query create table. if you follow the step carefully, you will see the data in the database and can use trino to query or use it
![image](https://github.com/HungNguyenDev1511/Caption-Project/assets/69066161/fdaa5182-7336-4bf9-8c3f-dbe4e95a12b6)
