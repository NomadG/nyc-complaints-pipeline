import os
from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-2",  # 2 vCPUs, 8 GB — orchestration only
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "e2-standard-4",  # 4 vCPUs, 16 GB — handles 10M+ records comfortably
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "software_config": {"image_version": "2.1-debian11"},
    "gce_cluster_config": {
        "zone_uri": "",  # let GCP auto-select the zone with available capacity
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    },
}

@dag(
    dag_id='spark_transform_dag',
    default_args=default_args,
    start_date=datetime(2026, 4, 17),
    schedule='30 12 * * *',  # 6:00 PM IST = 12:30 UTC
    catchup=False,
    tags=['spark', 'dataproc'],
)
def spark_transform_dag():

    PROJECT_ID = os.environ['DATAPROC_PROJECT_ID']
    REGION = os.environ['DATAPROC_REGION']
    BUCKET = os.environ['GCS_BUCKET_NAME']
    CLUSTER_NAME = "nyc-complaints-{{ ds_nodash }}"
    SCRIPT_URI = f"gs://{BUCKET}/scripts/transforms.py"

    upload_script = LocalFilesystemToGCSOperator(
        task_id='upload_transforms_script',
        src='/opt/airflow/spark_scripts/transforms.py',
        dst='scripts/transforms.py',
        bucket=BUCKET,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    def create_spark_task(task_id, task_arg):
        return DataprocSubmitJobOperator(
        task_id=task_id,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": SCRIPT_URI,
                "args": ["--task", task_arg, "--bucket", BUCKET],
                "properties": {},
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    spark_complaints = create_spark_task('spark_transform_complaints', 'complaints')
    spark_income = create_spark_task('spark_transform_median_income', 'median_income')

    wait_between_jobs = BashOperator(
        task_id='wait_between_jobs',
        bash_command='sleep 120',  # 2 min buffer for GCP to release job resources
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule='all_done',
    )

    upload_script >> create_cluster >> spark_complaints >> wait_between_jobs >> spark_income >> delete_cluster

spark_transform_dag()
