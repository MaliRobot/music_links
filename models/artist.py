from typing import List, Tuple, Optional
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
    previous: Tuple = None
    limit: int = 10

    def get_connected_artists(self, step: int = 0, collected=None, visited=None):
        if step >= self.limit:
            return collected, visited

        step += 1

        if collected is None:
            collected = {step: set()}

        if step not in collected.keys():
            collected[step] = set()

        if visited is None:
            visited = {self.discogs_id}

        visited.add(self.discogs_id)

        for release in self.releases:
            for artist in release.artists:
                if artist.discogs_id != self.discogs_id and artist.discogs_id not in visited:
                    artist.previous = (release.title, self.name)
                    collected[step].add(artist)
                    visited.add(artist)
                    new_collected, new_visited = artist.get_connected_artists(step, collected, visited)
                    visited.update(new_visited)
                    collected.update(new_collected)

        return collected, visited
