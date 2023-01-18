from typing import Any, List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate, ArtistDBListItem
from schemas.release import ReleaseCreate
from api.dependencies import get_db

from services.traverser import start_traversing

router = APIRouter()


@router.get("/name/{artist_name}", response_model=List[ArtistDBListItem])
def get_artist_by_name(
    artist_name: str,
    db: Session = Depends(get_db)
) -> Any:
    """
    Retrieve artist by name.
    """
    artists = artist_crud.search_by_name(db=db, artist_name=artist_name)
    return artists


@router.post("/")
def fetch_artist(
    discogs_id: str,
    db: Session = Depends(get_db)
) -> Any:
    """
    Retrieve artist by name.
    """
    start_traversing(discogs_id=discogs_id, db=db)
    return {'message': f'Fetching data for {discogs_id} artist'}


@router.post("/manual/")
def insert_artist_manually(
    # artist: schemas.artist.ArtistCreate,
    db: Session = Depends(get_db)
):
    artist = ArtistCreate(
        name="meow",
        discogs_id="123",
        page_url="12222",
        image_url="222",
        releases=[
            ReleaseCreate(
                name="drugi",
                discogs_id="7373",
                page_url="url",
                artists=[]
            )
        ]
    )

    artist_created = artist_crud.create_with_releases(db=db, artist_in=artist)
    return artist_created
