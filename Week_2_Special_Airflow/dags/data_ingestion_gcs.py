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
import pandas as pd

# Set some local variables based on environmental varaibles we specified in docker-compose.yaml
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Store environmental variables (in your docker container) locally. The second argument of each `.get` is what it will default to if it's empty.
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# Load dataset
def load_data_from_api(*args, **kwargs) -> pd.csv:
    """
    Template for loading data from API
    """
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz"
    taxi_dtypes ={
                    'VendorID':pd.Int64Dtype()
                ,   'store_and_fwd_flag':str
                ,   'RatecodeID':pd.Int64Dtype()
                ,   'PULocationID':pd.Int64Dtype()
                ,   'DOLocationID':pd.Int64Dtype()
                ,   'passenger_count':pd.Int64Dtype()
                ,   'trip_distance':float
                ,   'fare_amount':float
                ,   'extra':float
                ,   'mta_tax':float
                ,   'tip_amount':float
                ,   'tolls_amount':float
                ,   'ehail_fee':float
                ,   'improvement_surcharge':float
                ,   'total_amount':float
                ,   'payment_type':float
                ,   'trip_type':float
                ,   'congestion_surcharge':float
            }
    parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']
    df = pd.read_csv(url, sep=',', compression = "gzip", dtype = taxi_dtypes, parse_dates=parse_dates)

    month = ['11','12']
    for x in month:
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{x}.csv.gz"
        taxi_dtypes ={
                        'VendorID':pd.Int64Dtype()
                    ,   'store_and_fwd_flag':str
                    ,   'RatecodeID':pd.Int64Dtype()
                    ,   'PULocationID':pd.Int64Dtype()
                    ,   'DOLocationID':pd.Int64Dtype()
                    ,   'passenger_count':pd.Int64Dtype()
                    ,   'trip_distance':float
                    ,   'fare_amount':float
                    ,   'extra':float
                    ,   'mta_tax':float
                    ,   'tip_amount':float
                    ,   'tolls_amount':float
                    ,   'ehail_fee':float
                    ,   'improvement_surcharge':float
                    ,   'total_amount':float
                    ,   'payment_type':float
                    ,   'trip_type':float
                    ,   'congestion_surcharge':float
                }
        parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']
        pd_read = pd.read_csv(url, sep=',', compression = "gzip", dtype = taxi_dtypes, parse_dates=parse_dates)
        df = pd.concat([df,pd_read])
        df.to_csv(f'{path_to_local_home}/green_taxi.csv')

# Replace CSV with Parquet on our file

def format_to_parquet(df):
    """Takes our source file and converts it to parquet"""
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=f"{path_to_local_home}/green_taxi.parquet",
    )

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
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=load_data_from_api
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
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

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
