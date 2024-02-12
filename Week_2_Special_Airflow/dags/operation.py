import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import os

def load_transform_parquet(save_path):
    dataset_url_init = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"
    df = pd.read_parquet(dataset_url_init)
    for month in range(2,13):
        dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month:02d}.parquet"
        df_2 = pd.read_parquet(dataset_url)
        df = pd.concat([df,df_2])
        print(f"Downloaded{month:02d}")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "green_taxi_2022.parquet")

def upload_to_gcs(folder_path, bucket_name):

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.parquet'):
            blob = bucket.blob(file_name)
            blob.upload_from_filename(os.path.join(folder_path, file_name))
            print(f"Uploaded {file_name} to GCS")