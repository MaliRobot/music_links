import asyncio
from typing import Dict, Optional, List, Tuple

from fastapi.concurrency import run_in_threadpool

from schemas.artist import ArtistSearchResult
from services.disco_conn import get_disco_connection
from utils.strings import string_similarity_ratio


def _fetch_page_sync(disco_conn, name: str, page_number: int) -> List[Tuple[str, int, str]]:
    """
    Fetch only one page from the Discogs API.
    """
    search_result = disco_conn.search(term=name, type='artist')
    artists_page = search_result.page(page_number)
    return [(artist.name, artist.id, artist.url) for artist in artists_page]


async def fetch_page_threaded(disco_conn, name: str, page_number: int):
    """
    Run fetch in a threadpool.
    """
    return await run_in_threadpool(_fetch_page_sync, disco_conn, name, page_number)


async def artist_search(name: str, page_limit: int):
    """
    Get the number of pages with results and pass it to the fetcher.
    """
    disco_conn = get_disco_connection()

    initial_result = disco_conn.search(name, type='artist')
    max_pages = min(initial_result.pages, page_limit)

    tasks = [
        fetch_page_threaded(disco_conn, name, page_number)
        for page_number in range(1, max_pages + 1)
    ]
    page_results = await asyncio.gather(*tasks)
    return [item for page in page_results for item in page]


async def artist_sorted_search(
        name: str,
        page_limit: int = 3,
        similarity_threshold: Optional[float] = None
) -> Dict[str, ArtistSearchResult]:
    """
    Search for artists and sort by similarity to the search term.
    """
    search_results: Dict[str, ArtistSearchResult] = {}

    # Get cached search results
    artist_tuples = await artist_search(name, page_limit)

    for artist_name, discogs_id, url in artist_tuples:
        if artist_name not in search_results:
            artist_result = ArtistSearchResult(
                name=artist_name,
                discogs_id=discogs_id,
                url=url,
            )

            # Calculate similarity score
            similarity = string_similarity_ratio(artist_name, name)

            # Apply threshold filter if specified
            if similarity_threshold is None or similarity >= similarity_threshold:
                # Store the similarity score with the result for sorting
                artist_result.similarity = similarity
                search_results[artist_name] = artist_result

    # Sort results by similarity score
    search_results = dict(
        sorted(search_results.items(), key=lambda sub: getattr(sub[1], 'similarity', 0), reverse=True)
    )

    return search_results
