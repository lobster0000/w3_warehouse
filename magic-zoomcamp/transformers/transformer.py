if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # data.columns = (data.columns
    #                 .str.replace(' ', '_')
    #                 .str.lower()
    # )

    data = data.drop(columns=['ehail_fee'])
    data = data.drop(columns=['store_and_fwd_flag'])
    
    
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
