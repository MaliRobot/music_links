from fastapi import APIRouter

from api.api_v1.endpoints import artist

api_router = APIRouter()
api_router.include_router(artist.router, prefix="/artists", tags=["artists"])
