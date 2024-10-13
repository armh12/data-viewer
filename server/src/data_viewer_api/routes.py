from fastapi import File, UploadFile, APIRouter
from fastapi.responses import JSONResponse, FileResponse

from data_viewer_api.error_handler import async_error_handler
from data_viewer_api.handler import DataHandler


router = APIRouter()


@router.post("/upload/parquet", tags=['upload'])
@async_error_handler
async def upload_parquet(file: UploadFile = File(...)) -> JSONResponse:
    content = await file.read()
    table = DataHandler(content, file.filename).return_table()
    return JSONResponse(content=table.to_pydict())


@router.post("/upload/csv", tags=['upload'])
@async_error_handler
async def upload_csv(file: UploadFile = File(...)) -> JSONResponse:
    content = await file.read()
    table = DataHandler(content, file.filename).return_table()
    return JSONResponse(content=table.to_pydict())


@router.post("/convert/parquet_to_csv", tags=['convert'])
@async_error_handler
async def convert_parquet_to_csv(file: UploadFile = File(...)) -> FileResponse:
    content = await file.read()

