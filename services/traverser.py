from typing import Dict, Set, List
from dataclasses import dataclass, field
import discogs_client
import discogs_client.exceptions

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from config.settings import settings

from crud.artist import artist_crud
from crud.release import release_crud
from models.artist import Artist
from models.release import Release
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_fetch import DiscoConnector


MAX_STEPS = 20


@dataclass
class StepTraverser:
    discogs_id: str
    client: DiscoConnector
    db: Session
    artists: Set = field(default_factory=set)
    artist: Artist = None

    def get_or_create_artist(self):
        artist = artist_crud.get_by_discogs_id(self.db, self.discogs_id)
        if not artist:
            artist_discogs = self.client.fetch_artist_by_discogs_id(self.discogs_id)

            artist_in = ArtistCreate(
                name=artist_discogs.name,
                discogs_id=artist_discogs.id,
                page_url=artist_discogs.url,
            )

            artist_in.releases = [
                ReleaseCreate(
                    title=x.title,
                    discogs_id=x.id,
                    page_url=x.url,
                    year=x.year,
                ) for x in artist_discogs.releases
            ]

            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)

        self.artist = artist
        return self.artist

    def check_artist_releases(self):
        if self.artist:
            for release in self.artist.releases:
                release_discogs = self.client.get_release(release_id=release.discogs_id)
                try:
                    for artist in release_discogs.artists:
                        if artist.id != self.artist.discogs_id and artist.name != 'Various':
                            self.artists.add(artist.id)

                    if 'extraartists' in dir(release_discogs):
                        for ex_artist in release_discogs.extraartists:
                            if ex_artist.id != self.artist.discogs_id and ex_artist.name != 'Various':
                                self.artists.add(ex_artist.id)
                except discogs_client.exceptions.HTTPError as e:
                    print('err: ', str(e))
        print(self.artists)
        return self.artists


@dataclass
class Traverser:
    discogs_id: str
    client: DiscoConnector
    db: Session
    checked: Set = field(default_factory=set)
    count: int = 0
    max_artists: int = 100
    artists: Set = field(default_factory=set)

    def begin_traverse(self):
        self.checked = set()
        first_step = StepTraverser(
            discogs_id=self.discogs_id,
            client=self.client,
            db=self.db
        )
        artist = first_step.get_or_create_artist()
        self.checked.add(artist)
        first_step.check_artist_releases()
        self.artists = first_step.artists
        return self.traverse_loop()

    def traverse_loop(self):
        while True:
            artist = self.artists.pop()
            step = StepTraverser(
                discogs_id=artist,
                client=self.client,
                db=self.db
            )

            artist = step.get_or_create_artist()
            self.checked.add(artist)
            step.check_artist_releases()
            ids_to_check = step.artists
            ids_to_check = set([x for x in ids_to_check if x not in self.checked])
            self.artists.update(ids_to_check)

            if self.artists is None or self.count is self.max_artists:
                break
            del step
            self.count += 1


def start_traversing(discogs_id: str, db: Session, max_artists: int = 20):
    discogs_client = DiscoConnector(
        key=settings.discogs_key,
        secret=settings.discogs_secret
    )
    discogs_client.set_token(settings.token, settings.secret)

    traverser = Traverser(
        discogs_id=discogs_id,
        client=discogs_client,
        max_artists=max_artists,
        db=db,
    )
    traverser.begin_traverse()
