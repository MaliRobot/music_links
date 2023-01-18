from typing import Optional
from dataclasses import dataclass

import discogs_client
from discogs_client.exceptions import HTTPError

from models.artist import Artist
from models.release import Release


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
        # TODO - what to do about this propmt?
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
        artist = self.client.artist(artist_id)
        if artist:
            return self.prepare_artist_schema(artist)
        return artist

    @staticmethod
    def prepare_artist_schema(artist):
        artist_node = Artist(
            name=artist.name,
            discogs_id=artist.id,
            page_url=artist.url,
        )

        if artist.images and len(artist.images) > 0:
            artist_node.image_url = artist.images[0]['uri']

        for rel in artist.releases:
            release = Release(
                title=rel.title,
                discogs_id=rel.id,
                page_url=rel.url
            )
            artist_node.releases.append(release)
        return artist_node

    def search_artist(self, artist: str) -> Optional[Artist]:
        results = self.search(artist, type="artist")
        for result in results:
            if result.name == artist:
                return self.prepare_artist_schema(result)
        return None
