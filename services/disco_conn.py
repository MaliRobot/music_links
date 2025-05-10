from typing import Optional
from dataclasses import dataclass

import discogs_client
from discogs_client.exceptions import HTTPError

from config.settings import settings


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

        self.get_new_token()

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

    def fetch_artist_by_discogs_id(self, discogs_id):
        return self.client.artist(discogs_id)

    def set_token(self, token, secret):
        self.token = token
        self.secret = secret
        self.client.set_token(self.token, self.secret)

    def get_release(self, release_id):
        return self.client.release(release_id)

    def get_artist(self, artist_id):
        return self.client.artist(artist_id)


def init_disco_fetcher(oauth: bool = False):
    if oauth:
        discogs_conn = DiscoConnector(
            key=settings.discogs_key,
            secret=settings.discogs_secret
        )
        return discogs_conn.client

    discogs_conn = discogs_client.Client("MaliRobot/1.0", user_token="jBxVkTMkZIwMDhRMybSBtugUjsYXOAUwSpqgoWVb")
    return discogs_conn
