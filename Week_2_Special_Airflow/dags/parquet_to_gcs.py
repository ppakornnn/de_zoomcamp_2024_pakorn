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
# Set some local variables based on environmental varaibles we specified in docker-compose.yaml
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet

# Specify our dataset
month_list = ['02','03','04','05','06','07','08','09','10','11','12']
# Store environmental variables (in your docker container) locally. The second argument of each `.get` is what it will default to if it's empty.
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_green_taxi_data')
dataset_file = "green_taxi_2022"
# Replace CSV with Parquet on our file
parquet_file = dataset_file.replace('.csv', '.parquet')

def load_transform_parquet(src_file):
    dataset_url_init = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"
    df = pd.read_parquet(dataset_url_init)
    for month in month_list:
        dataset_url =  f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet"
        df_2 = pd.read_parquet(dataset_url)
        df = pd.concat([df,df_2])
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "green_taxi_2022.parquet")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    load_parquet = PythonOperator(
        task_id="load_parquet",
        python_callable=load_transform_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    load_parquet >> local_to_gcs_task
