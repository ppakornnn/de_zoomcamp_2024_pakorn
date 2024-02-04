if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from datetime import datetime
import pandas as pd

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.strftime('%Y-%m-%d')
    data.rename(columns= {      'VendorID':'vendor_id'
                            ,   'RatecodeID':'ratecode_id'
                            ,   'PULocationID':'pu_location_id'
                            ,   'DOLocationID':'do_location_id'
                         }
                        ,   inplace = True)
    return data[(data['trip_distance'] > 0) & (data['passenger_count'] > 0)]



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'] is not None, 'vendor_id is one of the existing values in the column'
    assert output['passenger_count'].isin([0]).sum() == 0 , 'There are rides without 0 passenger'
    assert output['trip_distance'].isin([0]).sum() == 0 , 'There are rides without 0 passenger'