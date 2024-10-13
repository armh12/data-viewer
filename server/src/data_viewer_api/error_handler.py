from functools import wraps
from typing import Union
from fastapi import HTTPException
import pyarrow


class InvalidFileFormatException(Exception):
    def __init__(self,
                 file_format: str,
                 required_file_format: str):
        self.file_format = file_format
        self.required_file_format = required_file_format

    def __str__(self):
        return f'Invalid file format. File format: {self.file_format}. Required file format: {self.required_file_format}'



def async_error_handler(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            if isinstance(err, Union[pyarrow.lib.ArrowIOError, pyarrow.lib.ArrowInvalid]):
                raise HTTPException(status_code=404, detail=str(err))
            if isinstance(err, pyarrow.lib.ArrowInvalid):
                raise HTTPException(status_code=400, detail=str(err))
            if isinstance(err, Union[ValueError, InvalidFileFormatException]):
                raise HTTPException(status_code=422, detail=str(err))
    return wrapper