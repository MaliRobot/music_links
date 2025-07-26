"""
Optimized async version of the traverser module with improved performance and reliability.

Key improvements:
1. Proper batch processing to avoid overwhelming asyncio.gather
2. Simplified rate limiting with configurable throttling
3. Better error handling and recovery
4. Memory-efficient processing of large datasets
5. Progress tracking and logging
"""

import asyncio
import time
import logging
from typing import Set, Optional, List, Dict, Any
from dataclasses import dataclass, field
from sqlalchemy.orm import Session
import discogs_client.exceptions
from concurrent.futures import ThreadPoolExecutor

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiting configuration
RATE_LIMIT = 60  # requests per minute (Discogs default for authenticated requests)
BATCH_SIZE = 10  # Process items in batches of this size
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds
REQUEST_INTERVAL = 60.0 / RATE_LIMIT  # Minimum time between requests


class RateLimiter:
    """Simple rate limiter to prevent API throttling."""

    def __init__(self, requests_per_minute: int = 60):
        self.min_interval = 60.0 / requests_per_minute
        self.last_request = 0
        self._lock = asyncio.Lock()

    async def wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        async with self._lock:
            now = time.time()
            time_since_last = now - self.last_request
            if time_since_last < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last)
            self.last_request = time.time()


class AsyncDiscoConnector:
    """
    Async wrapper for DiscoConnector with improved rate limiting and error handling.
    Uses ThreadPoolExecutor for efficient handling of synchronous API calls.
    """

    def __init__(self, client: DiscoConnector, max_workers: int = 5):
        self.client = client
        self.rate_limiter = RateLimiter(RATE_LIMIT)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    async def _execute_with_retry(self, func, *args, **kwargs):
        """Execute function with retry logic for transient errors."""
        for attempt in range(MAX_RETRIES):
            try:
                # Rate limit before making request
                await self.rate_limiter.wait_if_needed()

                # Run synchronous function in thread pool
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    self.executor, func, *args
                )
                return result

            except discogs_client.exceptions.HTTPError as e:
                if e.status_code == 429:  # Rate limit exceeded
                    logger.warning(f"Rate limit hit, waiting {RETRY_DELAY * (attempt + 1)} seconds...")
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                elif e.status_code >= 500:  # Server error, retry
                    logger.warning(f"Server error {e.status_code}, retrying...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"HTTP error {e.status_code}: {e}")
                    raise
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise
                await asyncio.sleep(RETRY_DELAY)

        return None

    async def fetch_artist_by_discogs_id(self, discogs_id: str):
        """Fetch artist data asynchronously."""
        return await self._execute_with_retry(
            self.client.fetch_artist_by_discogs_id, discogs_id
        )

    async def get_artist(self, artist_id: str):
        """Get artist data asynchronously."""
        return await self._execute_with_retry(
            self.client.get_artist, artist_id
        )

    async def get_release(self, release_id: str):
        """Get release data asynchronously."""
        return await self._execute_with_retry(
            self.client.get_release, release_id
        )

    def cleanup(self):
        """Clean up executor resources."""
        self.executor.shutdown(wait=False)


@dataclass
class AsyncStepTraverser:
    """Handles processing of a single artist and their releases."""

    discogs_id: str
    client: AsyncDiscoConnector
    db: Session
    artists: Set[str] = field(default_factory=set)
    artist: Optional[Any] = None

    async def get_or_create_artist(self):
        """Fetch or create artist asynchronously."""
        # Check if artist exists in DB
        artist = artist_crud.get_by_discogs_id(self.db, self.discogs_id)

        if not artist:
            logger.info(f"Fetching new artist {self.discogs_id} from Discogs API")

            # Fetch artist from Discogs API
            artist_discogs = await self.client.fetch_artist_by_discogs_id(self.discogs_id)
            if not artist_discogs:
                logger.warning(f"Artist {self.discogs_id} not found in Discogs")
                return None

            # Fetch releases data in batches
            releases = await self._fetch_releases_data_batched(artist_discogs.releases)

            # Create artist with releases
            artist_in = ArtistCreate(
                name=artist_discogs.name,
                discogs_id=artist_discogs.id,
                page_url=artist_discogs.url,
                releases=releases
            )
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
            logger.info(f"Created artist {artist.name} with {len(releases)} releases")
        else:
            logger.info(f"Artist {artist.name} already exists in database")

        self.artist = artist
        return artist

    async def _fetch_releases_data_batched(self, releases_paginator) -> List[ReleaseCreate]:
        """
        Fetch release data in batches to avoid overwhelming the API and asyncio.
        Processes releases page by page from the paginator.
        """
        all_releases = []

        try:
            # Get total number of pages
            total_pages = releases_paginator.pages if hasattr(releases_paginator, 'pages') else 1
            logger.info(f"Processing {total_pages} pages of releases")

            # Process each page
            for page_num in range(1, min(total_pages + 1, 10)):  # Limit to 10 pages for safety
                try:
                    logger.info(f"Processing releases page {page_num}/{total_pages}")

                    # Get releases for current page
                    page_releases = releases_paginator.page(page_num)

                    # Process releases in batches
                    batch = []
                    for release in page_releases:
                        batch.append(self._create_release_schema(release))

                        # Process batch when it reaches BATCH_SIZE
                        if len(batch) >= BATCH_SIZE:
                            batch_results = await self._process_release_batch(batch)
                            all_releases.extend(batch_results)
                            batch = []

                    # Process remaining releases in batch
                    if batch:
                        batch_results = await self._process_release_batch(batch)
                        all_releases.extend(batch_results)

                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error accessing releases paginator: {e}")

        logger.info(f"Successfully fetched {len(all_releases)} releases")
        return all_releases

    async def _process_release_batch(self, batch: List) -> List[ReleaseCreate]:
        """Process a batch of releases concurrently."""
        tasks = []
        for release_coro in batch:
            tasks.append(release_coro)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_releases = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error processing release: {result}")
            elif result:
                valid_releases.append(result)

        return valid_releases

    async def _create_release_schema(self, release):
        """Create ReleaseCreate schema from Discogs release object."""
        try:
            # This is a coroutine that will be awaited in batch processing
            return ReleaseCreate(
                title=getattr(release, 'title', 'Unknown'),
                discogs_id=getattr(release, 'id', None),
                page_url=getattr(release, 'url', ''),
                year=getattr(release, 'year', None),
            )
        except Exception as e:
            logger.error(f"Error creating release schema: {e}")
            return None

    async def get_artist_releases(self):
        """Get artist releases asynchronously."""
        if not self.artist:
            return []

        try:
            artist = await self.client.get_artist(artist_id=self.artist.discogs_id)
            return artist.releases if artist else []
        except Exception as e:
            logger.error(f"Error fetching artist releases: {e}")
            return []

    async def check_artist_releases(self):
        """Check all artist releases and extract collaborating artists."""
        releases_paginator = await self.get_artist_releases()

        if not releases_paginator:
            return self.artists

        try:
            total_pages = releases_paginator.pages if hasattr(releases_paginator, 'pages') else 1
            logger.info(f"Checking {total_pages} pages of releases for collaborators")

            # Process releases page by page
            for page_num in range(1, min(total_pages + 1, 5)):  # Limit pages for performance
                try:
                    logger.info(f"Processing collaborators from page {page_num}/{total_pages}")
                    page_releases = releases_paginator.page(page_num)

                    # Process releases in batches
                    batch = []
                    for release in page_releases:
                        batch.append(release)

                        if len(batch) >= BATCH_SIZE:
                            await self._process_release_batch_for_artists(batch)
                            batch = []

                    # Process remaining releases
                    if batch:
                        await self._process_release_batch_for_artists(batch)

                except Exception as e:
                    logger.error(f"Error processing page {page_num}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error checking artist releases: {e}")

        return self.artists

    async def _process_release_batch_for_artists(self, releases: List):
        """Process a batch of releases to extract collaborating artists."""
        tasks = []
        for release in releases:
            tasks.append(self._process_release(release))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_release(self, release):
        """Process a single release to extract collaborating artists."""
        try:
            # Handle main release if this is a master release
            if hasattr(release, 'main_release'):
                release = release.main_release

            # Extract artists from different fields
            artist_sets = []
            if hasattr(release, 'artists'):
                artist_sets.append(release.artists)
            if hasattr(release, 'extraartists'):
                artist_sets.append(release.extraartists)
            if hasattr(release, 'credits'):
                artist_sets.append(release.credits)

            # Process all artists
            for artist_set in artist_sets:
                for artist in artist_set:
                    if (hasattr(artist, 'id') and
                        artist.id != self.artist.discogs_id and
                        getattr(artist, 'name', '') != 'Various'):
                        self.artists.add(artist.id)
                        # Note: We're not adding releases to other artists here
                        # to avoid excessive DB operations

        except Exception as e:
            logger.error(f"Error processing release: {e}")


@dataclass
class AsyncTraverser:
    """Main traverser class for exploring artist networks."""

    discogs_id: str
    client: AsyncDiscoConnector
    db: Session
    checked: Set[str] = field(default_factory=set)
    count: int = 0
    max_artists: int = 100
    artists: Set[str] = field(default_factory=set)
    concurrent_artists: int = 3  # Process this many artists concurrently

    async def begin_traverse(self):
        """Begin traversal process asynchronously."""
        logger.info(f"Starting traversal from artist {self.discogs_id}")

        # Process first artist
        first_step = AsyncStepTraverser(
            discogs_id=self.discogs_id,
            client=self.client,
            db=self.db
        )

        artist = await first_step.get_or_create_artist()
        if artist:
            self.checked.add(artist.discogs_id)
            new_artists = await first_step.check_artist_releases()
            self.artists.update(new_artists)
            logger.info(f"Found {len(new_artists)} collaborating artists")

        return await self.traverse_loop()

    async def traverse_loop(self):
        """Main traversal loop with concurrent processing."""
        logger.info(f"Starting traversal loop with {len(self.artists)} artists to process")

        while self.artists and self.count < self.max_artists:
            # Create batch of artists to process
            batch = []
            for _ in range(min(self.concurrent_artists, len(self.artists), self.max_artists - self.count)):
                if self.artists:
                    artist_id = self.artists.pop()
                    if artist_id not in self.checked:
                        batch.append(artist_id)

            if not batch:
                break

            logger.info(f"Processing batch of {len(batch)} artists. Progress: {self.count}/{self.max_artists}")

            # Process batch concurrently
            tasks = [self._process_artist(artist_id) for artist_id in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Update count and collect new artists
            for result in results:
                if result and not isinstance(result, Exception):
                    self.count += 1
                    if isinstance(result, set):
                        new_unchecked = result - self.checked
                        self.artists.update(new_unchecked)
                        logger.info(f"Added {len(new_unchecked)} new artists to queue")

        logger.info(f"Traversal complete. Processed {self.count} artists")

    async def _process_artist(self, artist_id: str) -> Optional[Set[str]]:
        """Process a single artist asynchronously."""
        try:
            logger.info(f"Processing artist {artist_id}")

            step = AsyncStepTraverser(
                discogs_id=artist_id,
                client=self.client,
                db=self.db
            )

            artist = await step.get_or_create_artist()
            if not artist:
                return None

            self.checked.add(artist.discogs_id)
            new_ids = await step.check_artist_releases()

            logger.info(f"Artist {artist.name} has {len(new_ids)} collaborators")
            return new_ids

        except Exception as e:
            logger.error(f"Error processing artist {artist_id}: {e}")
            return None


async def start_traversing_async(
    discogs_id: str,
    db: Session,
    max_artists: int = 20,
    concurrent_artists: int = 3
):
    """
    Async version of start_traversing with improved performance.

    Args:
        discogs_id: Starting artist's Discogs ID
        db: Database session
        max_artists: Maximum number of artists to process
        concurrent_artists: Number of artists to process concurrently
    """
    sync_client = init_disco_fetcher()
    async_client = AsyncDiscoConnector(sync_client, max_workers=concurrent_artists)

    logger.info(f'Connected to Discogs. Starting from artist ID: {discogs_id}')

    traverser = AsyncTraverser(
        discogs_id=discogs_id,
        client=async_client,
        max_artists=max_artists,
        db=db,
        concurrent_artists=concurrent_artists
    )

    start_time = time.perf_counter()

    try:
        await traverser.begin_traverse()
    finally:
        # Clean up resources
        async_client.cleanup()

    elapsed = time.perf_counter() - start_time

    logger.info(f"Traversal completed in {elapsed:.2f} seconds")
    logger.info(f"Processed {traverser.count} artists")
    logger.info(f"Discovered {len(traverser.checked)} unique artists")

    return traverser


def start_traversing(discogs_id: str, db: Session, max_artists: int = 20):
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
            # Using smaller batch sizes for better performance
            await start_traversing_async(
                "17199",
                db,
                max_artists=10,
                concurrent_artists=2
            )
        finally:
            db.close()

    asyncio.run(main())