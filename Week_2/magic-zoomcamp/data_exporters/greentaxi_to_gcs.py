from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
from pyarrow import Table
from pyarrow import parquet as pq
from io import BytesIO

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'terraform-demo.mage-zoomcamp-matt-pakorn.green_taxi'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    buffer = BytesIO()
    df.to_parquet(engine='pyarrow', partition_cols=df['lpep_pickup_date'])
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            buffer
        ,   table_id
        ,   if_exists='replace',  # Specify resolution policy if table name already exists
    )
