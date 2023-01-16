from typing import List, Optional
from dataclasses import dataclass

from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship

from db.base_class import Base
from models.release import Release

artist_release = Table(
                    'artist_release',
                    Base.metadata,
                    Column('artist_id', Integer, ForeignKey('artist.id')),
                    Column('release_id', Integer, ForeignKey('release.id'))
                 )


class Artist(Base):
    __tablename__ = 'artist'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(256), nullable=False)
    discogs_id = Column(Integer, nullable=False, index=True, unique=True)
    page_url = Column(String(256), nullable=False)
    image_url = Column(String(256), nullable=True)
    releases = relationship(Release, secondary=artist_release, back_populates='artists')

    name: str
    discogs_id: int
    page_url: str
    releases: List
    image_url: Optional[str]
    releases: Optional[List[Release]]
