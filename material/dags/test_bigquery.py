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
    BigQueryDeleteTableOperator
)
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
credentials, project_id = google.auth.default()
DATASET_NAME = 'test_gian'
table_id = 'airflow1'
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
    "Listar_tablas_2",
    description= "Mi primer DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021,1,1),
    catchup=False,
    tags=["example"]
)
query1 = (
    f'CALL `{project_id}.{DATASET_NAME}.bigquery_airflow`()'
)
def mostrar_listado_func(**kwargs):    
    #print(kwargs)
    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables", 
        dataset_id=DATASET_NAME,
        project_id=project_id,
        dag=dag
    )
    tables=get_dataset_tables.execute(context=kwargs)
    #print(tables) #mostrar lista
    tablas= [table.get('tableId') for table in tables]
    if table_id in tablas:
        print('esta')
    else:
        print('no esta')    
    return {"ok":0}
insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": query1,
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    project_id=project_id,
    location='us-east4',
    dag=dag
)


mostrar_listado_antes = PythonOperator(
    task_id = "mostrar_listado_antes",
    python_callable=mostrar_listado_func,
    dag = dag
)

mostrar_listado_despues = PythonOperator(
    task_id = "mostrar_listado_despues",
    python_callable=mostrar_listado_func,
    dag = dag
)
delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=f"{project_id}.{DATASET_NAME}.{table_id}",
    dag = dag
)
insert_query_job >> mostrar_listado_antes >> delete_table >> mostrar_listado_despues