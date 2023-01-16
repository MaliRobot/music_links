from typing import Dict, Set
from dataclasses import dataclass, field
import discogs_client
import discogs_client.exceptions

from sqlalchemy.orm import Session

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

    def fetch_artist(self, term: str = None, discogs_id: str = None):
        if discogs_id:
            artist_from_db = artist_crud.get_by_discogs_id(self.db, discogs_id=discogs_id)
        else:
            artist_from_db = artist_crud.search_by_name(self.db, term)
            if artist_from_db:
                artist_from_db = artist_from_db[0] # TODO need to filter this additionally and return match
        if artist_from_db:
            print('id issue?', artist_from_db, 'tsk tsk')
            artist_node = ArtistCreate(
                id=artist_from_db.id,
                name=artist_from_db.name,
                discogs_id=artist_from_db.discogs_id,
                page_url=artist_from_db.page_url,
                releases=[]
            )
            self.artist = artist_node
            return artist_from_db
        else:
            artist_from_discogs = self.client.search(self.term, 'artist')
            artist_from_discogs = artist_from_discogs[0]  # TODO it has to be verified by user
            print(f"Fetching {artist_from_discogs.name} from Discogs (original search term was {self.term})")
            if artist_from_discogs:
                artist_node = ArtistCreate(
                    name=artist_from_discogs.name,
                    discogs_id=artist_from_discogs.id,
                    page_url=artist_from_discogs.url,
                    releases=[]
                )
                self.artist = artist_node
                return artist_from_discogs
        return None

    def check_release_in_db(self, release):
        release_from_db = release_crud.get_by_discogs_id(self.db, discogs_id=release.discogs_id)
        if release_from_db:
            return ReleaseCreate(
                       id=release.id,
                       name=release_from_db.title,
                       discogs_id=release_from_db.id,
                       page_url=release_from_db.url
                   )

        return ReleaseCreate(
            name=release.title,
            discogs_id=release.id,
            page_url=release.url
        )

    def fetch_release_artists(self, discogs_id):
        release = release_crud.get_by_discogs_id(self.db, discogs_id=discogs_id)

        if not release:
            release = self.client.get_release(discogs_id)
            if release:
                try:
                    release_node = Release(
                        name=release.title,
                        discogs_id=release.id,
                        page_url=release.url,
                    )
                    self.artist.releases.append(release_node)
                    return release.artists
                except discogs_client.exceptions.HTTPError:
                    pass
        return []

    def get_release_artists(self, release):
        release_artists = self.fetch_release_artists(release.id)

        try:
            for artist in release_artists:
                if artist.name != self.term and \
                        artist.name != "Various" and \
                        artist.name not in self.artists.keys():
                    self.artists.update({artist.name: artist})
                if self.count >= self.depth:
                    break

            self.increase_count()
        except discogs_client.exceptions.HTTPError as e:
            print(e)

    def increase_count(self):
        self.count += 1

    def run(self):
        artist = self.fetch_artist(self.term)
        if not artist:
            return None, None

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
    depth: int = 5
    max_artists: int = 10

    def go_traverse(self):
        self.artist_collection = set()
        self.checked = {}
        artists, _ = self.get_artist_related(self.term)
        self.artists_loop(artists)
        print("Results:\n")
        for c in self.checked:
            try:
                print(self.checked[c])
            except discogs_client.exceptions.HTTPError:
                print(c, ' 404 error')

    def artists_loop(self, artists):
        artist_count = 0
        while True:
            temp_artists = {}
            print(f"Checking {len(artists)} artists...")
            for artist in artists:
                print('checking', artist)
                new_artists, new_artist = self.get_artist_related(artist)
                print('done checking...')
                self.checked[new_artist.name] = new_artist
                print(f"Found new artists: {new_artists}")
                for na in new_artists:
                    if new_artists[na].name not in temp_artists.keys():
                        temp_artists[new_artists[na].name] = new_artists[na]
                self.artist_collection.update(set([x for x in new_artists.keys()]))
                artist_count += 1
                if artist_count >= self.max_artists:
                    break

            self.increase_count()
            print(f"Count: {self.count}")
            if self.count > 5 or not temp_artists:
                break

            artists = temp_artists

    def increase_count(self):
        self.count += 1

    def get_artist_related(self, artist):
        traverser = ArtistFetcher(
            artist,
            self.client,
            db=self.db,
            depth=self.depth,
        )

        new_artists, new_artist = traverser.run()
        self.checked[new_artist.name] = new_artist
        print(f"Found new artists: {new_artists}")
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
    print('Artists checked: ', traverser.checked)
    for artist in traverser.checked:
        artist_crud.create_with_releases(db=db, artist_in=traverser.checked[artist])

