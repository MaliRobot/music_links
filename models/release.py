from sqlalchemy import Column, Integer, String, SmallInteger
from sqlalchemy.orm import relationship

from db.base_class import Base


class Release(Base):
    __tablename__ = 'release'
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(256), nullable=False)
    discogs_id = Column(Integer, nullable=False, index=True, unique=True)
    page_url = Column(String(256), nullable=False)
    image_url = Column(String(256), nullable=True)
    year = Column(SmallInteger, nullable=True)
    artists = relationship('Artist', secondary='artist_release', back_populates='releases')
