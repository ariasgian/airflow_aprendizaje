from datetime import datetime, timedelta
from airflow import DAG
import sys
from pathlib import Path
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
airflow_config_path = Path("$AIRFLOW_HOME/config").expanduser().resolve()
dag_args = {
    "depemd_on_past" : False,
    "email" : ["test@test.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry:delay": timedelta(minutes=5)
}
dag = DAG(
    "test1",
    description= "Mi primer DAG",
    default_args=dag_args,
    schedule=timedelta(days=1),
    start_date=datetime(2021,1,1),
    catchup=False,
    tags=["example"]
)
def tarea0_func():
    "test0"
    return {"ok":0}
def tarea2_func():
    "test2"
    return {"ok":2}

tarea0 = PythonOperator(
    task_id = "tarea0",
    python_callable=tarea0_func,
    dag = dag
)
tarea2 = PythonOperator(
    task_id = "tarea2",
    python_callable=tarea2_func,
    dag = dag
)
tarea1 = BashOperator(
    task_id = "print_date",
    bash_command = 'echo "la fecha es $(date)"',
    dag = dag
)
tarea0 >> [tarea1, tarea2]