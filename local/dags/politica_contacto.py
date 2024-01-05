"""
Example Airflow DAG for Google BigQuery service testing tables.
"""
import os
from pprint import pprint
import google.auth
from airflow.utils.context import Context
from dotenv import load_dotenv
import time
from datetime import datetime
from pathlib import Path
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetTablesOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
    BigQueryCheckOperator
)
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
credentials, project_id = google.auth.default()
DATASET_NAME = 'test_gian'
table_id = 'politica_contactacion'
location_teco = 'us-east4'
vez = 1

dag_args = {
    "depend_on_past" : False,
    "email" : ["test@test.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry:delay": timedelta(minutes=5)
}
dag = DAG(
    "politica_contacto",
    description= "Mi primer DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021,1,1),
    catchup=False,
    tags=["example"]
)
query_mora = (
    f'CALL `{project_id}.{DATASET_NAME}.mora_historico`()'
)
query_politica = (
    f'CALL `{project_id}.{DATASET_NAME}.{table_id}`()'
)
def mostrar_listado_func(**kwargs):    
    #print(kwargs)
    
    tables=check_count
    print(tables)
mora_job = BigQueryInsertJobOperator(
    task_id="mora_job",
    configuration={
        "query": {
            "query": query_mora,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    project_id=project_id,
    location=location_teco,
    dag=dag
)
pol_contacto_job = BigQueryInsertJobOperator(
    task_id="pol_contacto_job",
    configuration={
        "query": {
            "query": query_politica,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    project_id=project_id,
    location=location_teco,
    dag=dag
)
check_count = BigQueryCheckOperator(
        task_id="check_count",
        sql=f"SELECT COUNT(*) FROM {DATASET_NAME}.{table_id}",
        use_legacy_sql=False,
        location=location_teco,
        dag=dag
    )
#chequeo_operator = PythonOperator(
#    task_id = "chequeo_job",
#    python_callable=mostrar_listado_func,
#    dag = dag
#)
# delete_table = BigQueryDeleteTableOperator(
#     task_id="delete_table",
#     deletion_dataset_table=f"{project_id}.{DATASET_NAME}.{table_id}",
#     dag = dag
# )
mora_job >> pol_contacto_job >> check_count