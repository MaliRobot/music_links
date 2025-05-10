from typing import Optional, List
from pydantic import BaseModel, Field
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


class ArtistDBListItem(BaseModel):
    id: int
    name: str
    discogs_id: str
    image_url: Optional[str]
    page_url: str

    class Config:
        orm_mode = True


class ArtistSearchResult(BaseModel):
    name: str
    discogs_id: int
    url: str
    similarity: Optional[float] = Field(default=None, exclude=True)
