FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 argparse


WORKDIR /app
COPY ingest_zone_lookup.py ingest_zone_lookup.py 

ENTRYPOINT [ "python" , "ingest_zone_lookup.py"]
