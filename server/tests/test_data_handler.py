import pytest
from pyarrow import Table
from data_viewer_api.handler import DataHandler
from common import struct_dataframe


def test_data_handler_parquet():
    with open('./resources/df.parquet', 'rb') as file:
        data = file.read()
    dh = DataHandler(data, 'df.parquet')
    table = dh.return_table()
    assert table is not None
    assert isinstance(table, Table)
    print('Table:\n', table)


def test_data_handler_csv():
    with open('./resources/df.csv', 'rb') as file:
        data = file.read()
    dh = DataHandler(data, 'df.csv')
    table = dh.return_table()
    assert table is not None
    assert isinstance(table, Table)
    print('Table:\n', table)


def test_data_handler_parquet_when_df_size_is_huge():
    # df = struct_dataframe(10000000)
    # df.to_parquet('./resources/df_huge.parquet')
    with open('./resources/df_huge.parquet', 'rb') as file:
        data = file.read()
    dh = DataHandler(data, 'df.parquet')
    table = dh.return_table()
    assert table is not None
    assert isinstance(table, Table)
    print('Table:\n', table)


