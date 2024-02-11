from pandas import DataFrame as df
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


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
    # data = data[(data['passenger_count'] != 0) & (data['trip_distance'] != 0)]
    
    # # Create a new column lpep_pickup_date by extracting the date
    # data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    # data.rename(columns={'VendorID': 'vendor_id'}, inplace=True)

    # unique_vendor_values = data['vendor_id'].unique()
    # print(unique_vendor_values)
    # assert all(data['vendor_id'].isin(unique_vendor_values)), 'vendor_id is not one of the existing values'
    # assert (data['passenger_count'] > 0).all(), 'passenger_count is not greater than 0'
    # assert (data['trip_distance'] > 0).all(), 'trip_distance is not greater than 0'

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
