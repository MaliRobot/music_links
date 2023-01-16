from fastapi import FastAPI

from api.api import api_router
from config.settings import settings

app = FastAPI(title="Music Links")

app.include_router(api_router, prefix=settings.api_v1)
