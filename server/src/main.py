import uvicorn
from fastapi import FastAPI

from data_viewer_api.routes import router

app = FastAPI()

app.include_router(router)

def main():
    uvicorn.run('main:app', host='127.0.0.1', port=8000, log_level="info", http="h11")


if __name__ == "__main__":
    main()