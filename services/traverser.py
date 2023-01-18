from typing import Dict, Set
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
class ArtistFetcher:
    discogs_id: str
    client: DiscoConnector
    db: Session
    count: int = 0
    steps: int = 10
    artists: Dict = field(default_factory=dict)
    artist: ArtistCreate = None

    def get_artist_by_discogs_id(self, discogs_id: str):
        artist = artist_crud.get_by_discogs_id(self.db, discogs_id=discogs_id)
        if not artist:
            artist = self.client.get_artist(discogs_id)
        return artist

    def get_artist_by_name(self, term: str):
        artist = artist_crud.search_by_name(db=self.db, artist_name=term)
        if artist:
            artist = artist[0]
        if not artist:
            artist = self.client.search_artist(term)
        return artist

    def fetch_artist(self, discogs_id: str):
        artist = self.get_artist_by_discogs_id(discogs_id)
        if artist:
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist)
            return artist
        return None

    def fetch_release_artists(self, discogs_id):
        release = self.client.get_release(discogs_id)
        if release:
            try:
                return release.artists
            except discogs_client.exceptions.HTTPError:
                pass
        return []

    def add_release_to_artist(self, release, artist):
        release_from_db = release_crud.get_by_discogs_id(db=self.db, discogs_id=release.id)
        if not release_from_db:
            try:
                release_in = Release(
                    title=release.title,
                    discogs_id=release.id,
                    page_url=release.url,
                )
                release = release_crud.create(db=self.db, obj_in=release_in)
            except AttributeError:
                pass

        else:
            release = release_from_db
        artist_crud.add_artist_release(db=self.db, artist_id=artist.id, release=release)
        return artist

    def add_album_to_non_db_artist(self, release, artist):
        artist = self.get_artist_by_discogs_id(discogs_id=artist.id)
        if artist.id is None:
            artist_in = Artist(
                name=artist.name,
                discogs_id=artist.discogs_id,
                page_url=artist.page_url,
            )
            artist = artist_crud.create(db=self.db, obj_in=artist_in)

        self.add_release_to_artist(release, artist)
        self.add_artist_to_check(artist)

    def get_release_artists(self, release):
        release_artists = self.fetch_release_artists(release.discogs_id)
        try:
            for artist in release_artists:
                if artist.name not in ["Various", self.artist.name] or artist.id != self.discogs_id:
                    self.add_album_to_non_db_artist(release, artist)
                    if release.__class__.__name__ != "Master":
                        for other_artist in release.artists:
                            self.add_album_to_non_db_artist(release, other_artist)

                if self.count >= self.steps:
                    break

            self.increase_count()
        except discogs_client.exceptions.HTTPError as e:
            print(e)

    def add_artist_to_check(self, artist):
        if artist.name not in self.artists.keys() and artist.name != self.artist.name:
            self.artists.update({artist.name: artist})

    def increase_count(self):
        self.count += 1

    def run(self):
        artist = self.fetch_artist(discogs_id=self.discogs_id)
        if not artist:
            return None, None
        self.artist = artist
        print(f'Found {len(artist.releases)} releases for artist {artist.name}')
        for release in artist.releases:
            self.get_release_artists(release)
            if self.count >= self.steps:
                break

        return self.artists, self.artist


@dataclass
class Traverser:
    discogs_id: str
    client: DiscoConnector
    db: Session
    checked: Dict = field(default_factory=dict)
    artist_collection: Set = None
    count: int = 0
    depth: int = 15
    max_artists: int = 100

    def go_traverse(self):
        self.artist_collection = set()
        self.checked = {}
        artists, _ = self.get_artist_related(self.discogs_id)
        if artists:
            self.artists_loop(artists)
            print("Results:\n")
            print(self.checked)

    def artists_loop(self, artists):
        artist_count = 0
        while True:
            temp_artists = {}
            print(f"Checking {len(artists)} artists...")
            for artist in artists:
                new_artists, new_artist = self.get_artist_related(artists[artist].discogs_id)
                print(f"Found new artists: {new_artists}")
                for na in new_artists:
                    if new_artists[na].name not in temp_artists.keys() and \
                            new_artists[na].name not in self.checked.keys() and \
                            new_artists[na].id != self.discogs_id:
                        temp_artists[new_artists[na].name] = new_artists[na]
                self.artist_collection.update(set([x for x in new_artists.keys()]))
                artist_count += 1
                if artist_count >= self.max_artists:
                    break

            self.increase_count()
            print(f"Count: {self.count}")
            if self.count > MAX_STEPS or not temp_artists:
                break

            artists = [x.discogs_id for x in temp_artists]

    def increase_count(self):
        self.count += 1

    def get_artist_related(self, artist_discogs_id):
        fetcher = ArtistFetcher(
            artist_discogs_id,
            self.client,
            db=self.db,
            steps=self.depth,
        )
        new_artists, new_artist = fetcher.run()

        if not new_artist:
            return None, None

        self.checked[new_artist.name] = new_artist
        print(f"Found new artists: {new_artists}")
        del fetcher
        return new_artists, new_artist


def start_traversing(discogs_id: str, db: Session, depth: int = 10, max_artists: int = 20):
    discogs_client = DiscoConnector(
        key=settings.discogs_key,
        secret=settings.discogs_secret
    )
    discogs_client.set_token(settings.token, settings.secret)

    traverser = Traverser(
        discogs_id=discogs_id,
        client=discogs_client,
        max_artists=max_artists,
        depth=depth,
        db=db,
    )
    traverser.go_traverse()
