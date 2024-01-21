import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"

    # download the csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    df_iter = pd.read_csv(csv_name,iterator = True,chunksize = 1e6,compression='gzip',dtype={"lpep_dropoff_datetime":"string","store_and_fwd_flag":"string"})

    for iterator in df_iter:
        t_start = time()
        df = iterator
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=table_name,con=engine, if_exists = 'append')
        t_end = time()
        print('inserted another chunk..., took %.3f second ' %(t_end - t_start))


if __name__ == '__main__' :

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument('--user',help ='user name for postgres')
    parser.add_argument('--password',help ='password for postgres')
    parser.add_argument('--host',help ='host for postgres')
    parser.add_argument('--port',help ='port for postgres')
    parser.add_argument('--db',help ='databasename for postgres')
    parser.add_argument('--table_name',help ='table name for postgres')
    parser.add_argument('--url',help ='url of the csv')

    args = parser.parse_args()

    main(args)

