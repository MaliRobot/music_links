from typing import Dict, Any

from schemas.artist import ArtistSearchResult

from services.disco_conn import init_disco_fetcher

from utils.strings import string_similarity_ratio


def artist_sorted_search(name: str) -> Dict[str, ArtistSearchResult]:
    search_results: dict[Any, ArtistSearchResult] = {}

    discogs_conn = init_disco_fetcher()
    artists = discogs_conn.search(term=name, type='artist')
    for i in range(artists.pages):
        for artist in artists.page(i):
            if artist.id not in search_results.keys():
                artist_result = ArtistSearchResult(
                    name=artist.name,
                    discogs_id=artist.id,
                    url=artist.url,
                )
                search_results[artist.name] = artist_result

    search_results = dict(
        sorted(search_results.items(), key=lambda sub: string_similarity_ratio(sub[1].name, name), reverse=True)
    )
    return search_results
