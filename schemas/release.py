from typing import Optional, List, Dict
from pydantic import BaseModel


class ReleaseBase(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    discogs_id: Optional[str] = None
    page_url: Optional[str] = None
    image_url: Optional[str] = None
    artists: Optional[List[Dict]] = []


class ReleaseCreate(ReleaseBase):
    name: str
    discogs_id: str
    page_url: str

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class ReleaseUpdate(ReleaseCreate):
    pass
