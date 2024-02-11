import pyarrow as pa
import pyarrow.parquet as pq
import os
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/starry-core-413305-67e34a50ee1d.json"

bucket_name = 'mage-taxi-data'
object_key = 'taxi-data.parquet'
project_id = 'starry-core-413305'

table_name = "nyc_taxi_data"
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # data['lpep_pickup_date'] = date['lpep_pickup_datetime'].dt.date
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )


