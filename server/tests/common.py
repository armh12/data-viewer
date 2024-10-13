import datetime
from typing import Union

import httpx
import pandas as pd

HOST = '127.0.0.1'
PORT = '8000'


class APIClient:
    def __init__(self,
                 host: str,
                 port: Union[int, str]):
        self.host = host
        self.port = port
        self._client = httpx.Client()
        self._async_client = httpx.AsyncClient()
        self._url = f'http://{self.host}:{self.port}'

    async def async_get(self, params: dict) -> httpx.Response:
        return await self._async_client.get(
            url=self._url,
            params=params
        )

    def get(self, params: dict) -> httpx.Response:
        return self._client.get(
            url=self._url,
            params=params
        )

    async def async_post(self, request_body: dict, files: dict) -> httpx.Response:
        return await self._async_client.post(
            url=self._url,
            files=files,
            data=request_body
        )

    def post(self, request_body: dict, files: dict) -> httpx.Response:
        return self._client.post(
            url=self._url,
            files=files,
            data=request_body
        )


def struct_dataframe(length: int):
    df = pd.DataFrame(
        {
            "col1": [1] * length,
            "col2": [1] * length,
            "col3": ["a"] * length,
            "col4": [datetime.datetime(2024, 1, 1, 23, 59, 59)] * length,
            "col5": [pd.Timestamp("2024-01-01 23:59:59")] * length
        }
    )
    return df
