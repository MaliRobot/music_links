from typing import Set
from dataclasses import dataclass, field
import discogs_client
import discogs_client.exceptions

from sqlalchemy.orm import Session

from crud.artist import artist_crud
from crud.release import release_crud
from models.artist import Artist
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher


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
            if not artist_discogs:
                return None

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

    def get_artist_releases(self):
        if self.artist:
            if 'page_url' in dir(self.artist):
                artist = self.client.get_artist(artist_id=self.artist.discogs_id)
                return artist.releases
            return self.artist.releases
        return []

    def check_artist_releases(self):
        for release in self.get_artist_releases():
            if 'main_release' in dir(release):
                release = release.main_release
            try:
                for artist in release.artists:
                    if artist.id != self.artist.discogs_id and artist.name != 'Various':
                        self.artists.add(artist.id)
                        self.add_release_to_artist(artist, release)
                if 'extraartists' in dir(release):
                    for ex_artist in release.extraartists:
                        if ex_artist.id != self.artist.discogs_id and ex_artist.name != 'Various':
                            self.artists.add(ex_artist.id)
                            self.add_release_to_artist(ex_artist, release)
                for artist in release.credits:
                    if artist.id != self.artist.discogs_id and artist.name != 'Various':
                        self.artists.add(artist.id)
                        self.add_release_to_artist(artist, release)

            except discogs_client.exceptions.HTTPError as e:
                print('err: ', str(e))
            except AttributeError:
                print('master', dir(release), release.title)

        print(self.artists)
        return self.artists

    def add_release_to_artist(self, artist, release_discogs):
        release_in = ReleaseCreate(
            title=release_discogs.title,
            discogs_id=release_discogs.id,
            page_url=release_discogs.url,
            year=release_discogs.year,
        )
        db_artist = artist_crud.get_by_discogs_id(db=self.db, discogs_id=artist.id)
        if db_artist:
            release = release_crud.get_by_discogs_id(db=self.db, discogs_id=release_discogs.id)
            if not release:
                release = release_crud.create(db=self.db, obj_in=release_in)
            artist_crud.add_artist_release(db=self.db, artist_id=db_artist.id,
                                           release=release)
        else:
            artist_crud.create_with_releases(
                db=self.db,
                artist_in=ArtistCreate(
                    name=artist.name,
                    discogs_id=artist.id,
                    page_url=artist.url,
                    releases=[
                        release_in
                    ]
                )
            )


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
            if len(self.artists) == 0:
                break

            artist = self.artists.pop()
            step = StepTraverser(
                discogs_id=artist,
                client=self.client,
                db=self.db
            )

            artist = step.get_or_create_artist()
            if not artist:
                continue

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
    discogs_client = init_disco_fetcher()
    traverser = Traverser(
        discogs_id=discogs_id,
        client=discogs_client,
        max_artists=max_artists,
        db=db,
    )
    traverser.begin_traverse()
