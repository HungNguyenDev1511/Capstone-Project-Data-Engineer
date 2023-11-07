from datetime import datetime
from pathlib import Path

from airflow import DAG
from docker.types import Mount 
from airflow.providers.docker.operators.docker import DockerOperator

TRAINING_DIR = "/home/quandv/Documents/fsds/m2/pipeline-orchestration-with-airflow/run_env"

with DAG(dag_id="docker", start_date=datetime(2023, 7, 1), schedule=None) as dag:
    # This is often used to seperate environment dependencies
    # from other components
    train_task = DockerOperator(
        task_id="train_task",
        image="fullstackdatascience/airflow-lgb-stage:0.0.1",
        container_name="airflow-lgb-stage",
        api_version="auto",
        auto_remove=True,
        network_mode="bridge",
        docker_url="tcp://docker-proxy:2375",
        mounts=[
            Mount(
                source=f"{TRAINING_DIR}/dags/lightgbm",
                target="/training/code",
                type='bind',
            ),
            Mount(
                source=f"{TRAINING_DIR}/data",
                target="/training/data",
                type='bind',
            )
        ],
        working_dir="/training",
        command='python code/train.py',
    )
