from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from pendulum import datetime

POSTGRES_CONN_ID = "postgres_default"


with DAG(dag_id="gx", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS taxi (
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
                );

            INSERT INTO taxi (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
            PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge,
            Airport_fee)
            VALUES ('001', 'Strawberry Order 1', 10),
                ('002', 'Strawberry Order 2', 5),
                ('003', 'Strawberry Order 3', 8),
                ('004', 'Strawberry Order 4', 3),
                ('005', 'Strawberry Order 5', 12);
            """,
    )

    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="public.strawberries",
        database="k6",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )

    drop_table_pg = PostgresOperator(
        task_id="drop_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DROP TABLE taxi;
            """,
    )

    create_table_pg >> gx_validate_pg >> drop_table_pg
