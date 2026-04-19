import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

IMAGE_NAME = 'dbt'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dbt_dag',
    default_args=default_args,
    start_date=datetime(2026, 4, 17),
    schedule='30 14 * * *',  # 8:00 PM IST = 14:30 UTC
    catchup=False,
    tags=['dbt', 'bigquery'],
)
def dbt_dag():

    def create_dbt_task(task_id, dbt_command):
        return DockerOperator(
            task_id=task_id,
            image=IMAGE_NAME,
            command=dbt_command,
            auto_remove='success',
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            environment={
                'GOOGLE_APPLICATION_CREDENTIALS': '/app/creds/g_creds.json',
                'GCP_PROJECT_ID': os.environ['DATAPROC_PROJECT_ID'],
                'GCS_BUCKET_NAME': os.environ['GCS_BUCKET_NAME'],
            },
            mounts=[
                Mount(source=os.environ['CREDENTIALS_HOST_DIR'], target='/app/creds', type='bind')
            ]
        )

    stage_external_sources = create_dbt_task(
        'stage_external_sources',
        'run-operation stage_external_sources'
    )

    run_staging = create_dbt_task(
        'run_staging',
        'run --select staging'
    )

    test_staging = create_dbt_task(
        'test_staging',
        'test --select staging'
    )

    run_snapshot = create_dbt_task(
        'run_snapshot',
        'snapshot'
    )

    run_mart = create_dbt_task(
        'run_mart',
        'run --select mart'
    )

    stage_external_sources >> run_staging >> test_staging >> run_snapshot >> run_mart

dbt_dag()
