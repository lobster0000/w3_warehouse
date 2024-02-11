import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    urls= [
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet',
    ]

    dfs = []
    
    for url in urls:
        print(url)

        taxi_dtypes = {
            'VendorID': pd.Int64Dtype(), 
            'lpep_pickup_datetime': 'object', 
            'lpep_dropoff_datetime': 'object',
            'store_and_fwd_flag': str,
            'RatecodeID': pd.Int64Dtype(),
            'PULocationID': pd.Int64Dtype(),
            'DOLocationID': pd.Int64Dtype(),
            'passenger_count': pd.Int64Dtype(),
            'trip_distance': float,
            'fare_amount': float,
            'extra': float,
            'mta_tax': float,
            'tip_amount': float,
            'tolls_amount': float,
            'improvement_surcharge': float,
            'total_amount': float,
            'payment_type': pd.Int64Dtype(),
            'trip_type': pd.Int64Dtype(),
            'congestion_surcharge': float,
            'tolls_amount': float,
        }
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

        df = pd.read_parquet(url)
        df.replace('', pd.NA, inplace=True)
        df = df.astype(taxi_dtypes)
        dfs.append(df)
        print('finish')

    return pd.concat(dfs, ignore_index=True)

@test
def test_output(output, *args) -> None:
    assert all(df is not None for df in output), 'At least one output is undefined'
    # assert all(isinstance(df, pd.DataFrame) for df in output), 'At least one output is not a DataFrame'
