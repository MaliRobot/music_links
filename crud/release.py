from typing import Optional

from sqlalchemy.orm import Session

from crud.base import CRUDBase, ModelType
from models.release import Release
from schemas.release import ReleaseCreate, ReleaseUpdate


class CRUDItem(CRUDBase[Release, ReleaseCreate, ReleaseUpdate]):
    def get_by_discogs_id(self, db: Session, discogs_id: str) -> Optional[ModelType]:
        return db.query(self.model).filter(Release.discogs_id == discogs_id).first()


release_crud = CRUDItem(Release)
