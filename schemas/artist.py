from typing import Optional, List
from pydantic import BaseModel
from schemas.release import ReleaseCreate


class ArtistBase(BaseModel):
    id: Optional[int] = None
    name: Optional[str] = None
    discogs_id: Optional[str] = None
    page_url: Optional[str] = None
    image_url: Optional[str] = None
    releases: Optional[List[ReleaseCreate]]


class ArtistCreate(ArtistBase):
    name: str
    discogs_id: str
    page_url: str

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class ArtistUpdate(ArtistBase):
    pass


class ArtistDBListItem(ArtistBase):
    id: int
    name: str

    class Config:
        orm_mode = True
