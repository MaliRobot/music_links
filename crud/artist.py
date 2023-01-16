from typing import List, Optional, Any

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from crud.base import CRUDBase, ModelType
from crud.release import release_crud
from models.artist import Artist
from schemas.artist import ArtistCreate, ArtistUpdate
from schemas.release import ReleaseCreate


class CRUDItem(CRUDBase[Artist, ArtistCreate, ArtistUpdate]):
    def search_by_name(self, db: Session, artist_name: str) -> Optional[ModelType]:
        return db.query(self.model).filter(Artist.name.ilike(f'{artist_name}%')).all()

    def get_by_discogs_id(self, db: Session, discogs_id: str) -> Optional[ModelType]:
        return db.query(self.model).filter(Artist.discogs_id == discogs_id).first()

    def create_with_releases(self, db: Session, artist_in: ArtistCreate):
        releases_db = []
        for release in artist_in.releases:
            db_rel = release_crud.create(db=db, obj_in=release)
            db_rel.artists = []
            releases_db.append(db_rel)

        artist_in.releases = []
        obj_in_data = jsonable_encoder(artist_in)
        db_obj = self.model(**obj_in_data)

        db_obj.releases.extend(releases_db)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj


artist_crud = CRUDItem(Artist)
