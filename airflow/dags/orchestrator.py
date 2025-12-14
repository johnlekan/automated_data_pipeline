import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/api-request')

def safe_main_callable():
    from insert_records import main
    return main()

default_args = {
    "description": "Orchestrator DAG for managing workflow",
    "start_date": datetime(2025, 4, 30),

}

with DAG(
    dag_id = "orchestrator_dag",
    default_args = default_args,
    schedule_interval = timedelta(minutes=1),
) as dag:

    # Define your tasks here

    task1 = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=safe_main_callable)

    task2 = DockerOperator(
        task_id='run_dbt_models',  
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command = '',
        working_dir='/usr/app/dbt',
        mounts=[
            Mount(source='/home/john/projects/weather-data-project/dbt/my_project', target='/usr/app/dbt', type='bind'),
            Mount(source='/home/john/projects/weather-data-project/dbt/profiles.yml', target='/root/.dbt/profiles.yml', type='bind'),
        ],
        network_mode='weather-data-project_my-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',

    )

    task1 >> task2