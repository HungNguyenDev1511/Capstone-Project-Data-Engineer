import numpy as np
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from pendulum import datetime

with DAG(dag_id="sklearn", start_date=datetime(2023, 7, 1), schedule=None) as dag:

    # @task
    # def generate_dataframe():
    #     data = pd.DataFrame(np.random.rand(10, 10))
    #     return data

    @task
    def train_model(data):
        print(f"Retrieving mock dataset with shape {data.shape}...")
        print("Training the model...")

    mock_dataframe = pd.read_parquet("/home/hungnguyen/Caption-Project/airflow/run_env/data/regression/yellow_tripdata_2022-08.parquet")
    train_model_dataframe = train_model(mock_dataframe)
