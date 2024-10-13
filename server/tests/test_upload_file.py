import pytest

from common import APIClient, HOST, PORT, struct_dataframe


@pytest.fixture
def http_client():
    return APIClient(host=HOST, port=PORT)



@pytest.mark.asyncio
async def test_upload_parquet(http_client):
    file_path = "resources/df.csv"
    struct_dataframe(10).to_csv(file_path)
    response = await http_client.async_post(
        files={"file": (file_path, open(file_path, "rb"))}, request_body={}
    )
    assert response.status_code == 200
    assert response is not None
