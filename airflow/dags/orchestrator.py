import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Make API module importable
sys.path.append("/opt/airflow/api-request")

from insert_records import main

default_args = {
    "description": "API ingestion + dbt orchestration DAG",
    "start_date": datetime(2025, 12, 25),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="api_dbt_orchestrator",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    tags=["api", "dbt", "orchestration"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=main,
    )

    dbt_task = DockerOperator(
        task_id="run_dbt_models",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source="/home/john/projects/automated_data_pipeline/dbt/my_project",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                source="/home/john/projects/automated_data_pipeline/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind",
            ),
        ],
        network_mode="airflow_network",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
    )

    ingest_task >> dbt_task
