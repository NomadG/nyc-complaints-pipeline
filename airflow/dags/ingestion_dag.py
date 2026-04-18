import os
from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

IMAGE_NAME = 'ingestion'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='ingestion_dag',
    default_args=default_args,
    start_date=datetime(2026, 4, 17),
    schedule='30 8 * * *',  # 2:00 PM IST (UTC+5:30) = 08:30 UTC
    catchup=False,
    tags=['complaints', 'median_income'] # Tags for easy filtering
)
def ingestion_dag():

    def create_ingest_task(task_id, task_arg):
        return DockerOperator(
            task_id=task_id,
            image=IMAGE_NAME,
            command=f'--task {task_arg}',
            auto_remove='success',
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            timeout=10800,
            environment={
                'SERVICE_ACCOUNT_JSON': '/app/creds/g_creds.json',
                'GCS_BUCKET_NAME': os.environ['GCS_BUCKET_NAME'],
                'SOCRATA_APP_TOKEN': os.environ['SOCRATA_APP_TOKEN'],
                'DEFAULT_START': os.environ.get('DEFAULT_START', '2026-04-09T01:35:59.943Z'),
                'PAGE_SIZE': os.environ.get('PAGE_SIZE', '10000'),
            },
            mounts=[
                Mount(source=os.environ['CREDENTIALS_HOST_DIR'], target='/app/creds', type='bind')
            ]
        )

    # Define tasks
    task_complaints = create_ingest_task('ingest_complaints', 'complaints')
    task_income = create_ingest_task('ingest_median_income', 'median_income')

    # PARALLEL EXECUTION:
    # By removing the >> and just listing them, Airflow will start both 
    # containers at the exact same time (if your laptop has enough RAM!)
    [task_complaints, task_income]

ingestion_dag()