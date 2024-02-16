import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import os
import requests

def load_transform_parquet(type : str,year : str) -> None:
    print(f'Download {type}_taxi_{year}_01')
    dataset_url_init = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-01.parquet"
    df = pd.read_parquet(dataset_url_init)
    for month in range(2,13):
        dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month:02d}.parquet"
        df_2 = pd.read_parquet(dataset_url)
        df = pd.concat([df,df_2])
        print(f"Downloaded{month:02d}")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f"{type}_taxi_{year}.parquet")

def upload_to_gcs(folder_path, bucket_name,file_name):

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(os.path.join(folder_path, file_name))
    print(f"Uploaded {file_name} to GCS")


def download_taxi_data(save_path,type : str,year : str):
    for month in range(1, 13):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month:02d}.parquet"
        filename = os.path.join(save_path, f"{type}_tripdata_{year}_{month:02d}.parquet")
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, "wb") as file:
                file.write(response.content)
            print(f"Downloaded {filename}")
        else:
            print(f"Failed to download {filename}. Status code: {response.status_code}")
