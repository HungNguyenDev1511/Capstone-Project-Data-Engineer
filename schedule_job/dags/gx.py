from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from pendulum import datetime

POSTGRES_CONN_ID = "postgres_default"


with DAG(dag_id="gx", start_date=datetime(2023, 7, 1), schedule=None) as dag:

    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir="include/great_expectations",
        data_asset_name="public.taxi_0",
        database="k6",
        expectation_suite_name="taxi_suite",
        return_json_dict=True,
    )

    gx_validate_pg
