import io
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from enum import Enum

from .error_handler import InvalidFileFormatException


class Types(str, Enum):
    PARQUET = "parquet"
    CSV = "csv"


class DataHandler:
    MAX_MP_THRESHOLD = 1000000
    CHUNK_SIZE = MAX_MP_THRESHOLD // 10

    def __init__(self,
                 file: bytes,
                 filename: str):
        self.file = file
        self.filename = filename

    def return_table(self):
        if self.filename.endswith(Types.PARQUET.value):
            return self.return_parquet()
        if self.filename.endswith(Types.CSV.value):
            return self.return_csv()

    def return_parquet(self):
        file_data = io.BytesIO(self.file)
        table = pq.read_table(file_data)
        table = self._convert_to_string(table)
        return table

    def return_csv(self):
        file_data = io.BytesIO(self.file)
        table = csv.read_csv(file_data)
        return table

    @staticmethod
    def _convert_to_string(table: pa.Table) -> pa.Table:
        schema = table.schema
        new_columns = []

        for col_name in schema.names:
            col = table[col_name]

            if pa.types.is_timestamp(col.type):
                col = col.cast(pa.string())

            new_columns.append(col)

        return pa.Table.from_arrays(arrays=new_columns, names=schema.names)


class Converter:
    def __init__(self,
                 file: str,
                 filename: str):
        self.file = file
        self.filename = filename

    def convert_parquet_to_csv(self):
        if not self.filename.endswith('parquet'):
            raise InvalidFileFormatException(
                file_format=self.filename.split('.')[-1], required_file_format=Types.PARQUET.value
            )

