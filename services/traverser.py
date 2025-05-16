from typing import Set, Optional
from dataclasses import dataclass, field
from sqlalchemy.orm import Session
import discogs_client.exceptions

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher


@dataclass
class StepTraverser:
    discogs_id: str
    client: DiscoConnector
    db: Session
    artists: Set[str] = field(default_factory=set)
    artist: Optional[ArtistCreate] = None

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
                releases=[
                    ReleaseCreate(
                        title=r.title,
                        discogs_id=r.id,
                        page_url=r.url,
                        year=r.year,
                    ) for r in artist_discogs.releases
                ]
            )
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)

        self.artist = artist
        return artist

    def get_artist_releases(self):
        if not self.artist:
            return []

        try:
            # Prefer fetching from API if possible
            artist = self.client.get_artist(artist_id=self.artist.discogs_id)
            return artist.releases
        except Exception:
            return self.artist.releases

    def check_artist_releases(self):
        for release in self.get_artist_releases():
            try:
                if hasattr(release, 'main_release'):
                    release = release.main_release
                self._process_artists(release, getattr(release, 'artists', []))
                self._process_artists(release, getattr(release, 'extraartists', []))
                self._process_artists(release, getattr(release, 'credits', []))
            except discogs_client.exceptions.HTTPError as e:
                print(f'HTTPError: {e}')
            except AttributeError:
                print('AttributeError:', dir(release), getattr(release, 'title', ''))

        return self.artists

    def _process_artists(self, release, artists):
        for artist in artists:
            if artist.id != self.artist.discogs_id and artist.name != 'Various':
                self.artists.add(artist.id)
                self.add_release_to_artist(artist, release)

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
            artist_crud.add_artist_release(db=self.db, artist_id=db_artist.id, release=release)
        else:
            artist_crud.create_with_releases(
                db=self.db,
                artist_in=ArtistCreate(
                    name=artist.name,
                    discogs_id=artist.id,
                    page_url=artist.url,
                    releases=[release_in]
                )
            )


@dataclass
class Traverser:
    discogs_id: str
    client: DiscoConnector
    db: Session
    checked: Set[str] = field(default_factory=set)
    count: int = 0
    max_artists: int = 100
    artists: Set[str] = field(default_factory=set)

    def begin_traverse(self):
        first_step = StepTraverser(
            discogs_id=self.discogs_id,
            client=self.client,
            db=self.db
        )
        artist = first_step.get_or_create_artist()
        if artist:
            self.checked.add(artist.discogs_id)
            self.artists.update(first_step.check_artist_releases())
        return self.traverse_loop()

    def traverse_loop(self):
        while self.artists and self.count < self.max_artists:
            artist_id = self.artists.pop()
            if artist_id in self.checked:
                continue

            step = StepTraverser(
                discogs_id=artist_id,
                client=self.client,
                db=self.db
            )

            artist = step.get_or_create_artist()
            if not artist:
                continue

            self.checked.add(artist.discogs_id)
            new_ids = step.check_artist_releases()
            self.artists.update(new_ids - self.checked)

            self.count += 1


def start_traversing(discogs_id: str, db: Session, max_artists: int = 20):
    client = init_disco_fetcher()
    traverser = Traverser(
        discogs_id=discogs_id,
        client=client,
        max_artists=max_artists,
        db=db,
    )
    traverser.begin_traverse()
