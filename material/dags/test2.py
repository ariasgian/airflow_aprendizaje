"""
Example Airflow DAG for Google BigQuery service testing tables.
"""
import os
import google.auth
from dotenv import load_dotenv
import time
from datetime import datetime
from pathlib import Path
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetTablesOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
credentials, project_id = google.auth.default()
load_dotenv()
dag_args = {
    "depemd_on_past" : False,
    "email" : ["test@test.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry:delay": timedelta(minutes=5)
}
dag = DAG(
    "Listar_tablas",
    description= "Mi primer DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021,1,1),
    catchup=False,
    tags=["example"]
)
DATASET_NAME = 'test_gian'
PROJECT=os.getenv("PROJECT_ID")
get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", 
        dataset_id=DATASET_NAME,
        project_id=project_id,
        dag=dag
    )

get_dataset_tables