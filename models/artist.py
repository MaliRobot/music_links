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
    limit: int = 10

    def get_connected_artists(self):
        visited = {self.discogs_id}
        collected = []
        for release in self.releases:
            for artist in release.artists:
                if artist.discogs_id not in visited:
                    visited.add(artist.discogs_id)
                    collected.append(artist)
        return collected
