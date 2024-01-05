from airflow.models import DAG, Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils.trigger_rule import TriggerRule
import datetime
import google.auth
credentials, project_id = google.auth.default()
# Project Variables
# To set this variables, configure them as a key value pair in Airflow UI under Admin - Variable - Add new record (+)
#PROJECT_SETTINGS = Variable.get('project_settings', deserialize_json=True)
#PROJECT_ID = PROJECT_SETTINGS['project_id']
REGION = 'us-east4'
# Cluster configurations
CLUSTER_NAME = "cluster-name"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    }
}
# Pyspark Job configurations
PYSPARK_URI = "gs://bucket/file.py"
JAR_URI = "gs://bucket/file.jar"
PYSPARK_JOB = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
    "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"]
}
default_args = {
    'owner': 'airflow',                                     # Owner, use a team distinction
    'start_date': datetime.datetime(2022, 1, 1),                     # Date for start scheduling
    'retries': 1,                                           # Number of retries if job fails
    'retry_delay': datetime.timedelta(minutes=30),           # Time to wait between fails
    'catch_up': False                                       # Back fill execution for past dates, from start_date
}
with DAG(
        'dataproc_dag', 
        description= "Mi primer DAG",
        default_args=default_args, 
        schedule_interval='0 10 * * *'
    ) as dag:
    # Create cluster
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=project_id,
            cluster_config=CLUSTER_CONFIG,
            region=REGION,
            cluster_name=CLUSTER_NAME,
            deferrable=True,
        )
        # Execute Pyspark Job
        execute_pyspark_job = DataprocSubmitJobOperator(
            task_id='execute_pyspark_job',
            job = PYSPARK_JOB,
            region = REGION,
            project_id = project_id
        )
        # Delete de cluster
        delete_cluster = DataprocDeleteClusterOperator(
            task_id='delete_cluster',
            project_id=project_id,
            cluster_name=CLUSTER_NAME,
            region=REGION,
            trigger_rule=TriggerRule.ALL_DONE,    # Tirgger rule to be sure that the cluster is deleted even if the job fails to avoid costs
        )

create_cluster >> execute_pyspark_job >> delete_cluster