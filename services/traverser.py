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


@dataclass
class ArtistFetcher:
    term: str
    client: DiscoConnector
    db: Session
    count: int = 0
    depth: int = 10
    artists: Dict = field(default_factory=dict)
    artist: ArtistCreate = None

    def get_artist_from_by_discogs_id(self, discogs_id: str):
        artist = artist_crud.get_by_discogs_id(self.db, discogs_id=discogs_id)
        if not artist:
            artist_from_discogs = self.client.get_artist(discogs_id)
            if artist_from_discogs:
                artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_from_discogs)
                return artist
        return artist

    def get_artist_by_name(self, term: str):
        artist = artist_crud.search_by_name(db=self.db, artist_name=term)
        if artist:
            artist = artist[0]
        if not artist:
            artist = self.client.search_artist(term)
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist)
        return artist

    def fetch_artist(self, term: str = None, discogs_id: str = None):
        if discogs_id:
            return self.get_artist_from_by_discogs_id(discogs_id)
        if term:
            return self.get_artist_by_name(term)
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

    def get_release_artists(self, release):
        release_artists = self.fetch_release_artists(release.discogs_id)

        try:
            for artist in release_artists:
                if artist.name != self.term and \
                        artist.name != "Various" and \
                        artist != self.artist:
                    artist = self.fetch_artist(discogs_id=artist.id)
                    self.add_release_to_artist(release, artist)
                    if release.__class__.__name__ != "Master":
                        for other_artist in release.artists:
                            self.add_release_to_artist(release, other_artist)
                            self.add_artist_to_check(other_artist)

                    self.add_release_to_artist(release, artist)
                    self.add_artist_to_check(artist)
                if self.count >= self.depth:
                    break

            self.increase_count()
        except discogs_client.exceptions.HTTPError as e:
            print(e)

    def add_artist_to_check(self, artist):
        if artist.name not in self.artists.keys():
            self.artists.update({artist.name: artist})

    def increase_count(self):
        self.count += 1

    def run(self):
        artist = self.fetch_artist(self.term)
        if not artist:
            return None, None
        self.artist = artist

        for release in artist.releases:
            self.get_release_artists(release)
            if self.count >= self.depth:
                break

        return self.artists, self.artist


@dataclass
class Traverser:
    term: str
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
        artists, _ = self.get_artist_related(self.term)
        self.artists_loop(artists)
        print("Results:\n")

    def artists_loop(self, artists):
        artist_count = 0
        while True:
            temp_artists = {}
            print(f"Checking {len(artists)} artists...")
            for artist in artists:
                print('checking', artist)
                new_artists, new_artist = self.get_artist_related(artist)
                print(f"Found new artists: {new_artists}")
                for na in new_artists:
                    if new_artists[na].name not in temp_artists.keys() and \
                            new_artists[na].name not in self.checked.keys() and \
                            new_artists[na].name != self.term:
                        temp_artists[new_artists[na].name] = new_artists[na]
                self.artist_collection.update(set([x for x in new_artists.keys()]))
                artist_count += 1
                if artist_count >= self.max_artists:
                    break

            self.increase_count()
            print(f"Count: {self.count}")
            if self.count > 65 or not temp_artists:
                break

            artists = [x.name for x in temp_artists]

    def increase_count(self):
        self.count += 1

    def get_artist_related(self, artist):
        fetcher = ArtistFetcher(
            artist,
            self.client,
            db=self.db,
            depth=self.depth,
        )

        new_artists, new_artist = fetcher.run()
        print(new_artist, '89898')
        self.checked[new_artist.name] = new_artist
        print(f"Found new artists: {new_artists}")
        del fetcher
        return new_artists, new_artist


def start_traversing(term: str, db: Session, depth: int = 10, max_artists: int = 20):
    discogs_client = DiscoConnector(
        key=settings.discogs_key,
        secret=settings.discogs_secret
    )
    discogs_client.set_token(settings.token, settings.secret)

    traverser = Traverser(
        term=term,
        client=discogs_client,
        max_artists=max_artists,
        depth=depth,
        db=db,
    )
    traverser.go_traverse()
