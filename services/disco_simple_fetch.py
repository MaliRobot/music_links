from dataclasses import dataclass

import discogs_client


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
