# data_ingestion_gcs_dag.py
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

# Import our operators
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# We installed the following from the requirements file which was specified in the Dockerfile.

# Helps to interact with Google Storage
from google.cloud import storage

# Helps interact with BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Helps convert our data to parquet format
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

# Helps to convert paequet to df
import pandas as pd
import operation as op

# Set some local variables based on environmental varaibles we specified in docker-compose.yaml
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Store environmental variables (in your docker container) locally. The second argument of each `.get` is what it will default to if it's empty.
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_green_taxi_data')
dataset_file = "green_taxi_2022"


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_operator_task = PythonOperator(
    task_id='download_green_taxi_data_task',
    python_callable=op.load_transform_parquet,
    op_kwargs={'save_path': path_to_local_home}
    )   

    upload_operator_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=op.upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "folder_path": path_to_local_home,
        }
    )



    download_operator_task >> upload_operator_task 
