"""
Async version of the traverser module that uses asyncio for concurrent API calls.

Key improvements:
1. Concurrent fetching of artist and release data
2. Batch processing of releases to minimize sequential waits
3. Configurable concurrency limits to respect API rate limits
4. Better error handling with exponential backoff for rate limit errors
"""

import asyncio
import time
from typing import Set, Optional, List, Dict
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
import discogs_client.exceptions
from functools import wraps

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher

# Rate limiting configuration
RATE_LIMIT = 60  # requests per minute (Discogs default)
CONCURRENT_REQUESTS = 5  # max concurrent API requests
RETRY_ATTEMPTS = 3
INITIAL_BACKOFF = 1.0  # seconds


def rate_limited(max_per_second=1):
    """Decorator to rate limit function calls."""
    min_interval = 1.0 / float(max_per_second)

    def decorate(func):
        last_time_called = [0.0]

        @wraps(func)
        async def rate_limited_function(*args, **kwargs):
            elapsed = time.perf_counter() - last_time_called[0]
            left_to_wait = min_interval - elapsed
            if left_to_wait > 0:
                await asyncio.sleep(left_to_wait)
            ret = await func(*args, **kwargs)
            last_time_called[0] = time.perf_counter()
            return ret

        return rate_limited_function

    return decorate


class AsyncDiscoConnector:
    """Async wrapper for DiscoConnector with rate limiting and retry logic."""

    def __init__(self, client: DiscoConnector):
        self.client = client
        self._semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        self._rate_limiter = asyncio.Semaphore(RATE_LIMIT)

    async def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff on rate limit errors."""
        backoff = INITIAL_BACKOFF
        for attempt in range(RETRY_ATTEMPTS):
            try:
                return await func(*args, **kwargs)
            except discogs_client.exceptions.HTTPError as e:
                if e.status_code == 429:  # Rate limit exceeded
                    if attempt < RETRY_ATTEMPTS - 1:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    else:
                        raise
                else:
                    raise

    @asynccontextmanager
    async def _api_limit(self):
        """Context manager for API rate limiting."""
        async with self._semaphore:
            async with self._rate_limiter:
                yield
                # Add small delay to avoid hitting rate limits
                await asyncio.sleep(60.0 / RATE_LIMIT)

    async def fetch_artist_by_discogs_id(self, discogs_id: str):
        """Fetch artist data asynchronously."""
        async with self._api_limit():
            # Run synchronous API call in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self.client.fetch_artist_by_discogs_id,
                discogs_id
            )

    async def get_artist(self, artist_id: str):
        """Get artist data asynchronously."""
        async with self._api_limit():
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self.client.get_artist,
                artist_id
            )

    async def get_release(self, release_id: str):
        """Get release data asynchronously."""
        async with self._api_limit():
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self.client.get_release,
                release_id
            )


@dataclass
class AsyncStepTraverser:
    discogs_id: str
    client: AsyncDiscoConnector
    db: Session  # Note: Consider using AsyncSession for async DB operations
    artists: Set[str] = field(default_factory=set)
    artist: Optional[ArtistCreate] = None

    async def get_or_create_artist(self):
        """Fetch or create artist asynchronously."""
        # DB operations are still sync - consider using async SQLAlchemy
        artist = artist_crud.get_by_discogs_id(self.db, self.discogs_id)
        print(artist, 'artist')
        if not artist:
            artist_discogs = await self.client.fetch_artist_by_discogs_id(self.discogs_id)
            print(artist_discogs, 'artist')
            if not artist_discogs:
                return None

            # Fetch all releases concurrently
            releases = await self._fetch_releases_data(artist_discogs.releases)
            print(releases, 'releases')
            artist_in = ArtistCreate(
                name=artist_discogs.name,
                discogs_id=artist_discogs.id,
                page_url=artist_discogs.url,
                releases=releases
            )
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
        print(artist.discogs_id, 'artist', artist.name)
        self.artist = artist
        return artist

    async def _fetch_releases_data(self, releases) -> List[ReleaseCreate]:
        """Fetch release data concurrently."""

        async def fetch_release(r):
            try:
                return ReleaseCreate(
                    title=r.title,
                    discogs_id=r.id,
                    page_url=r.url,
                    year=r.year,
                )
            except Exception as e:
                print(f"Error fetching release {getattr(r, 'id', 'unknown')}: {e}")
                return None
        print(releases, 'releases')
        tasks = [fetch_release(r) for r in releases]
        print(tasks, 'tasks')
        tasks = tasks[:4]
        # results = await asyncio.gather(*tasks, return_exceptions=True)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                print(f"Error during concurrent task: {r}")
        return [r for r in results if r and not isinstance(r, Exception)]

    async def get_artist_releases(self):
        """Get artist releases asynchronously."""
        if not self.artist:
            return []

        try:
            # Prefer fetching from API if possible
            artist = await self.client.get_artist(artist_id=self.artist.discogs_id)
            return artist.releases
        except Exception:
            print(f"Error fetching artist releases")
            return []

    async def check_artist_releases(self):
        """Check all artist releases concurrently."""
        releases = await self.get_artist_releases()

        # Process releases in batches to avoid overwhelming the API
        batch_size = 10
        for i in range(1, releases.pages + 1):
            print(i, 'iii')
            print(releases[i])
            # batch = releases.pages(i,i + batch_size)
            batch = releases.page(i)
            print(batch)
            tasks = [self._process_release(release) for release in batch]
            await asyncio.gather(*tasks, return_exceptions=True)

        return self.artists

    async def _process_release(self, release):
        """Process a single release asynchronously."""
        try:
            if hasattr(release, 'main_release'):
                release = release.main_release

            # Fetch all artist data concurrently
            tasks = []
            if hasattr(release, 'artists'):
                tasks.append(self._process_artists_batch(release, release.artists))
            if hasattr(release, 'extraartists'):
                tasks.append(self._process_artists_batch(release, release.extraartists))
            if hasattr(release, 'credits'):
                tasks.append(self._process_artists_batch(release, release.credits))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except discogs_client.exceptions.HTTPError as e:
            print(f'HTTPError processing release: {e}')
        except AttributeError as e:
            print(f'AttributeError: {e}, release: {getattr(release, "title", "unknown")}')
        except Exception as e:
            print(f'Unexpected error processing release: {e}')

    async def _process_artists_batch(self, release, artists):
        """Process a batch of artists concurrently."""
        tasks = []
        for artist in artists:
            if artist.id != self.artist.discogs_id and artist.name != 'Various':
                self.artists.add(artist.id)
                tasks.append(self._add_release_to_artist(artist, release))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _add_release_to_artist(self, artist, release_discogs):
        """Add release to artist (async wrapper for sync DB operations)."""
        # This is still synchronous DB operation
        # Consider using async SQLAlchemy for full async benefits
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.add_release_to_artist_sync,
            artist,
            release_discogs
        )

    def add_release_to_artist_sync(self, artist, release_discogs):
        """Synchronous version of add_release_to_artist."""
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
class AsyncTraverser:
    discogs_id: str
    client: AsyncDiscoConnector
    db: Session
    checked: Set[str] = field(default_factory=set)
    count: int = 0
    max_artists: int = 100
    artists: Set[str] = field(default_factory=set)
    batch_size: int = 5  # Process artists in batches

    async def begin_traverse(self):
        """Begin traversal process asynchronously."""
        print('in loop')
        first_step = AsyncStepTraverser(
            discogs_id=self.discogs_id,
            client=self.client,
            db=self.db
        )
        artist = await first_step.get_or_create_artist()
        print(f'Artist: {artist.name}')
        if artist:
            self.checked.add(artist.discogs_id)
            new_artists = await first_step.check_artist_releases()
            print(f'New artists: {new_artists}')
            self.artists.update(new_artists)
        return await self.traverse_loop()

    async def traverse_loop(self):
        """Main traversal loop with concurrent processing."""
        print(self.artists, 'traverse started.')
        while self.artists and self.count < self.max_artists:
            # Process artists in batches for better concurrency
            batch = []
            for _ in range(min(self.batch_size, len(self.artists), self.max_artists - self.count)):
                if self.artists:
                    artist_id = self.artists.pop()
                    if artist_id not in self.checked:
                        batch.append(artist_id)

            if not batch:
                await asyncio.sleep(0.1)
                continue

            # Process batch concurrently
            tasks = [self._process_artist(artist_id) for artist_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Update count and collect new artists
            for result in results:
                if result and not isinstance(result, Exception):
                    self.count += 1
                    if isinstance(result, set):
                        self.artists.update(result - self.checked)

    async def _process_artist(self, artist_id: str) -> Optional[Set[str]]:
        """Process a single artist asynchronously."""
        try:
            step = AsyncStepTraverser(
                discogs_id=artist_id,
                client=self.client,
                db=self.db
            )

            artist = await step.get_or_create_artist()
            if not artist:
                return None
            print(artist, "artist")
            self.checked.add(artist.discogs_id)
            new_ids = await step.check_artist_releases()
            return new_ids

        except Exception as e:
            print(f"Error processing artist {artist_id}: {e}")
            return None


async def start_traversing_async(
        discogs_id: str,
        db: Session,
        max_artists: int = 20,
        batch_size: int = 5
):
    """Async version of start_traversing with improved performance."""
    sync_client = init_disco_fetcher()
    async_client = AsyncDiscoConnector(sync_client)
    print(f'Connected to discogs ID: {discogs_id}')
    traverser = AsyncTraverser(
        discogs_id=discogs_id,
        client=async_client,
        max_artists=max_artists,
        db=db,
        batch_size=batch_size
    )
    print(f'Starting traversal: {traverser.count}')
    start_time = time.perf_counter()
    await traverser.begin_traverse()
    elapsed = time.perf_counter() - start_time

    print(f"Traversal completed in {elapsed:.2f} seconds")
    print(f"Processed {traverser.count} artists")
    return traverser


# Synchronous wrapper for backwards compatibility
def start_traversing_async_sync(discogs_id: str, db: Session, max_artists: int = 20):
    """Synchronous wrapper for the async traverser."""
    return asyncio.run(start_traversing_async(discogs_id, db, max_artists))


if __name__ == "__main__":
    # Example usage
    import asyncio
    from db.session import SessionLocal


    async def main():
        db = SessionLocal()
        try:
            # Example: traverse starting from a specific artist
            await start_traversing_async("17199", db, max_artists=50)
        finally:
            db.close()


    asyncio.run(main())
