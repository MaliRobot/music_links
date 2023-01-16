from typing import List, Optional, Dict, Set
import dotenv
from dataclasses import dataclass, field
import os
import time

import discogs_client
import requests.exceptions
from discogs_client.exceptions import HTTPError


@dataclass
class Node:
    name: str
    discogs_id: int
    page_url: str = None


@dataclass
class ReleaseNode(Node):
    year: int = None


@dataclass
class ArtistNode(Node):
    image_url: str = None
    releases: List[ReleaseNode] = field(default_factory=list)


@dataclass
class DiscoSimpleConnector:
    token: str = None

    def __init__(self, token):
        self.client = discogs_client.Client(
            "MaliRobot/1.0",
            user_token=token
        )

    def search(self, term: str):
        return self.client.search(term)


@dataclass
class DiscoConnector:
    client: discogs_client.Client = None
    auth_url: str = None
    request_token: str = None
    request_secret: str = None
    token: str = None
    secret: str = None

    def __init__(self, key, secret):
        self.client = discogs_client.Client(
            "MaliRobot/1.0",
            consumer_key=key,
            consumer_secret=secret
        )

    def get_new_token(self):
        self.request_token, self.request_secret, self.auth_url = self.client.get_authorize_url()

        accepted = 'n'
        while accepted.lower() == 'n':
            print("\n")
            accepted = input(f'Have you authorized me at {self.auth_url} [y/n] :')

        oauth_verifier = input('Verification code : ')
        token, secret = self.client.get_access_token(oauth_verifier)
        self.set_token(token, secret)

    def search(self, term: str, type: Optional[str]):
        if self.token is None:
            self.get_new_token()
        try:
            return self.client.search(term, type=type)
        except discogs_client.exceptions.HTTPError:
            self.get_new_token()
            self.search(term, type)

    def set_token(self, token, secret):
        self.token = token
        self.secret = secret
        self.client.set_token(self.token, self.secret)

    def get_release(self, release_id):
        return self.client.release(release_id)

    def get_artist(self, artist_id):
        return self.client.artist(artist_id)

    def search_artist(self, artist: str):
        results = self.search(artist, type="artist")
        for r in results:
            if r.name == artist:
                artist_node = ArtistNode(
                    name=r.name,
                    discogs_id=r.id,
                    page_url=r.url,
                )

                if r.images and len(r.images) > 0:
                    artist_node.image_url = r.images[0]['uri']

                for rel in r.releases:
                    release = ReleaseNode(
                        name=rel.title,
                        discogs_id=rel.id,
                        page_url=rel.url
                    )
                    artist_node.releases.append(release)
                return artist_node
        return None


@dataclass
class ArtistFetcher:
    term: str
    client: DiscoConnector
    depth: int
    count: int = 0
    artists: Dict = field(default_factory=dict)
    artist: ArtistNode = None

    def fetch_artist(self, term):
        artist = self.client.search(term, 'artist')
        if artist:
            artist = artist[0]
            artist_node = ArtistNode(
                name=artist.name,
                discogs_id=artist.id,
                page_url=artist.url,
            )

            self.artist = artist_node
            return artist
        return None

    @staticmethod
    def check_release_in_db(release):
        return ReleaseNode(
            name=release.title,
            discogs_id=release.id,
            page_url=release.url
        )

    def fetch_release_artists(self, discogs_id):
        release = self.client.get_release(discogs_id)
        self.artist.releases.append(release)

        try:
            return release.artists
        except discogs_client.exceptions.HTTPError:
            pass
        except requests.exceptions.SSLError:
            pass
        except ValueError:
            pass
        return []

    def get_release_artists(self, release):
        release_artists = self.fetch_release_artists(release.id)

        try:
            for artist in release_artists:
                self.increase_count()

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
    checked: Dict = field(default_factory=dict)
    artist_collection: Set = None
    count: int = 0
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
                new_artists, new_artist = self.get_artist_related(artist)
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
        traverser = ArtistFetcher(artist, self.client, depth=10)

        new_artists, new_artist = traverser.run()
        self.checked[new_artist.name] = new_artist
        print(f"Found new artists: {new_artists}")
        return new_artists, new_artist


def fetch():
    dotenv.load_dotenv()
    term = "Inside Riot"

    client = DiscoConnector(
        key=os.getenv("DISCOGS_KEY"),
        secret=os.getenv("DISCOGS_SECRET")
    )
    client.set_token(os.getenv("TOKEN"), os.getenv("SECRET"))

    traverser = Traverser(term=term, client=client)
    traverser.go_traverse()


if __name__ == '__main__':
    fetch()
