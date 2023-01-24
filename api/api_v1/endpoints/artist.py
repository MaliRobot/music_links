from typing import Any, List, Union
from collections import OrderedDict

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate, ArtistDBListItem, ArtistSearchResult
from schemas.release import ReleaseCreate
from api.dependencies import get_db

from services.traverser import start_traversing
from services.disco_ops import artist_sorted_search

router = APIRouter()


@router.get("/links/{artist_name}")
def get_artist_links(
    artist_discogs_id: str,
    db: Session = Depends(get_db)
):
    artist = artist_crud.get_by_discogs_id(db=db, discogs_id=artist_discogs_id)

    linked, _ = artist.get_connected_artists()
    for l in linked.keys():
        for a in linked[l]:
            print(f"{l}, {a.previous[1]} => {a.previous[0]} => {a.name}")
    return 'done'


@router.get("/search/{artist_name}", response_model=List[ArtistDBListItem])
def get_artist_by_name(
    artist_name: str,
    db: Session = Depends(get_db)
):
    """
    Retrieve artist by name.
    """
    artists = artist_crud.search_by_name(db=db, artist_name=artist_name)
    return artists


@router.post("/")
def fetch_artist(
    discogs_id: Union[str, None] = None,
    name: Union[str, None] = None,
    db: Session = Depends(get_db)
) -> Any:
    """
    Retrieve artist by name.
    """
    if name:
        return artist_sorted_search(name)

    if not discogs_id:
        return {}

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
                title="drugi",
                discogs_id="7373",
                page_url="url",
                year="1984",
                artists=[]
            )
        ]
    )

    artist_created = artist_crud.create_with_releases(db=db, artist_in=artist)
    return artist_created
