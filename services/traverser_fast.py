"""
Fast and simple traverser that prioritizes speed over complex parallelization.

Key principles:
1. Simple depth-limited breadth-first traversal
2. Minimal async overhead - just enough to prevent blocking
3. Bulk database operations
4. Fast fail on errors - don't retry excessively
5. Process complete artists before moving to next level
"""

import asyncio
import time
from typing import Set, List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import logging
from sqlalchemy.orm import Session
import discogs_client.exceptions

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Simple configuration - no complex rate limiting
MAX_DEPTH = 2  # Maximum degrees of separation from starting artist
SLEEP_BETWEEN_REQUESTS = 0.2  # Simple delay between API calls (adjust based on your API limits)
MAX_RETRIES = 1  # Don't waste time retrying


class FastDiscoClient:
    """Simplified async wrapper for Discogs API calls."""
    
    def __init__(self, client: DiscoConnector):
        self.client = client
        self.request_count = 0
        self.last_request_time = 0
    
    async def _throttle(self):
        """Simple throttling to avoid rate limits."""
        # Just enforce minimum time between requests
        elapsed = time.time() - self.last_request_time
        if elapsed < SLEEP_BETWEEN_REQUESTS:
            await asyncio.sleep(SLEEP_BETWEEN_REQUESTS - elapsed)
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 50 == 0:
            logger.info(f"API requests made: {self.request_count}")
    
    async def get_artist(self, artist_id: str):
        """Get artist data with simple throttling."""
        await self._throttle()
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.client.get_artist, artist_id)
        except Exception as e:
            logger.error(f"Error fetching artist {artist_id}: {e}")
            return None
    
    async def get_release(self, release_id: str):
        """Get release data with simple throttling."""
        await self._throttle()
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self.client.get_release, release_id)
        except Exception as e:
            logger.error(f"Error fetching release {release_id}: {e}")
            return None


@dataclass
class ArtistNode:
    """Simple data structure for tracking artists in traversal."""
    discogs_id: str
    name: str = ""
    depth: int = 0
    processed: bool = False


class FastTraverser:
    """Fast traverser with depth-limited BFS approach."""
    
    def __init__(self, start_artist_id: str, db: Session, max_depth: int = MAX_DEPTH):
        self.db = db
        self.client = FastDiscoClient(init_disco_fetcher())
        self.max_depth = max_depth
        self.start_artist_id = start_artist_id
        
        # Track artists by depth level
        self.artists_by_depth: Dict[int, List[ArtistNode]] = {i: [] for i in range(max_depth + 1)}
        self.processed_artists: Set[str] = set()
        self.artist_queue = deque()
        
        # Statistics
        self.stats = {
            'artists_processed': 0,
            'releases_saved': 0,
            'start_time': time.time()
        }
    
    async def traverse(self):
        """Main traversal method."""
        logger.info(f"Starting traversal from artist {self.start_artist_id} with max depth {self.max_depth}")
        
        # Start with the initial artist
        start_node = ArtistNode(discogs_id=self.start_artist_id, depth=0)
        self.artist_queue.append(start_node)
        
        # Process level by level
        for current_depth in range(self.max_depth + 1):
            logger.info(f"\n{'='*50}")
            logger.info(f"Processing depth level {current_depth}")
            logger.info(f"{'='*50}")
            
            # Get all artists at this depth
            artists_at_depth = [a for a in self.artist_queue if a.depth == current_depth]
            
            if not artists_at_depth:
                logger.info(f"No artists to process at depth {current_depth}")
                continue
            
            logger.info(f"Found {len(artists_at_depth)} artists to process at depth {current_depth}")
            
            # Process each artist at this depth
            for artist_node in artists_at_depth:
                if artist_node.discogs_id in self.processed_artists:
                    continue
                
                # Process this artist and get connected artists
                connected_artists = await self._process_artist_complete(artist_node)
                
                # Add connected artists to queue for next depth level
                if current_depth < self.max_depth:
                    for connected_id in connected_artists:
                        if connected_id not in self.processed_artists:
                            new_node = ArtistNode(
                                discogs_id=connected_id,
                                depth=current_depth + 1
                            )
                            self.artist_queue.append(new_node)
                
                # Mark as processed
                self.processed_artists.add(artist_node.discogs_id)
                self.stats['artists_processed'] += 1
                
                # Progress update
                if self.stats['artists_processed'] % 5 == 0:
                    self._print_progress()
        
        # Final statistics
        self._print_final_stats()
    
    async def _process_artist_complete(self, artist_node: ArtistNode) -> Set[str]:
        """
        Process a single artist completely:
        1. Get or create artist
        2. Fetch all releases
        3. Save everything to DB
        4. Extract and return connected artists
        """
        logger.info(f"\nProcessing artist: {artist_node.discogs_id} (depth: {artist_node.depth})")
        
        # Check if artist exists in DB
        db_artist = artist_crud.get_by_discogs_id(self.db, artist_node.discogs_id)
        
        if db_artist:
            logger.info(f"  Artist already in DB: {db_artist.name}")
            # Still need to get connected artists
            return await self._get_connected_artists_from_db(db_artist)
        
        # Fetch artist from Discogs
        artist_data = await self.client.get_artist(artist_node.discogs_id)
        if not artist_data:
            logger.warning(f"  Could not fetch artist {artist_node.discogs_id}")
            return set()
        
        artist_node.name = artist_data.name
        logger.info(f"  Fetched artist: {artist_data.name}")
        
        # Fetch all releases for this artist
        releases_data, connected_artists = await self._fetch_all_releases_fast(artist_data)
        
        logger.info(f"  Found {len(releases_data)} releases and {len(connected_artists)} connected artists")
        
        # Save artist with all releases to database in one go
        if releases_data:
            artist_in = ArtistCreate(
                name=artist_data.name,
                discogs_id=str(artist_data.id),
                page_url=getattr(artist_data, 'url', ''),
                releases=releases_data
            )
            
            try:
                db_artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
                self.stats['releases_saved'] += len(releases_data)
                logger.info(f"  Saved artist with {len(releases_data)} releases to DB")
            except Exception as e:
                logger.error(f"  Error saving artist to DB: {e}")
                self.db.rollback()
        
        return connected_artists
    
    async def _fetch_all_releases_fast(self, artist_data) -> Tuple[List[ReleaseCreate], Set[str]]:
        """
        Fetch all releases for an artist and extract connected artists.
        Returns: (list of releases, set of connected artist IDs)
        """
        releases_data = []
        connected_artists = set()
        
        try:
            releases = artist_data.releases
            
            if not releases:
                return releases_data, connected_artists
            
            # Handle pagination if present
            if hasattr(releases, 'pages'):
                total_pages = releases.pages
                logger.info(f"    Fetching {total_pages} pages of releases")
                
                for page_num in range(1, min(total_pages + 1, 5)):  # Limit pages to avoid too many releases
                    try:
                        page = releases.page(page_num)
                        
                        for release in page:
                            # Create release data
                            release_data = self._create_release_data(release)
                            if release_data:
                                releases_data.append(release_data)
                            
                            # Extract connected artists
                            artists_from_release = self._extract_artists_from_release(release, artist_data.id)
                            connected_artists.update(artists_from_release)
                        
                        # Small delay between pages
                        if page_num < total_pages:
                            await asyncio.sleep(0.1)
                            
                    except Exception as e:
                        logger.error(f"    Error processing page {page_num}: {e}")
                        break
            else:
                # Non-paginated releases
                for release in releases:
                    release_data = self._create_release_data(release)
                    if release_data:
                        releases_data.append(release_data)
                    
                    artists_from_release = self._extract_artists_from_release(release, artist_data.id)
                    connected_artists.update(artists_from_release)
        
        except Exception as e:
            logger.error(f"    Error fetching releases: {e}")
        
        return releases_data, connected_artists
    
    def _create_release_data(self, release) -> Optional[ReleaseCreate]:
        """Create a ReleaseCreate object from Discogs release data."""
        try:
            return ReleaseCreate(
                title=getattr(release, 'title', 'Unknown'),
                discogs_id=str(release.id),
                page_url=getattr(release, 'url', ''),
                year=getattr(release, 'year', 0)
            )
        except Exception as e:
            logger.debug(f"Could not create release data: {e}")
            return None
    
    def _extract_artists_from_release(self, release, current_artist_id) -> Set[str]:
        """Extract all connected artists from a release."""
        connected = set()
        
        try:
            # Check main release if it's a master
            if hasattr(release, 'main_release') and release.main_release:
                release = release.main_release
            
            # Extract from various artist fields
            for attr in ['artists', 'extraartists', 'credits']:
                if hasattr(release, attr):
                    artists_list = getattr(release, attr)
                    if artists_list:
                        for artist in artists_list:
                            if hasattr(artist, 'id') and hasattr(artist, 'name'):
                                # Skip self, Various, and unknown artists
                                if (str(artist.id) != str(current_artist_id) and 
                                    artist.name not in ['Various', 'Unknown']):
                                    connected.add(str(artist.id))
        except Exception as e:
            logger.debug(f"Error extracting artists from release: {e}")
        
        return connected
    
    async def _get_connected_artists_from_db(self, db_artist) -> Set[str]:
        """Get connected artists for an artist already in database."""
        connected = set()
        
        try:
            # Get artist's releases from DB
            if hasattr(db_artist, 'releases'):
                for release in db_artist.releases:
                    # Get other artists associated with this release
                    if hasattr(release, 'artists'):
                        for artist in release.artists:
                            if artist.discogs_id != db_artist.discogs_id:
                                connected.add(artist.discogs_id)
        except Exception as e:
            logger.error(f"Error getting connected artists from DB: {e}")
        
        # If no connections in DB, try fetching from API
        if not connected:
            artist_data = await self.client.get_artist(db_artist.discogs_id)
            if artist_data:
                _, connected = await self._fetch_all_releases_fast(artist_data)
        
        return connected
    
    def _print_progress(self):
        """Print progress statistics."""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['artists_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"\n--- Progress Update ---")
        logger.info(f"  Artists processed: {self.stats['artists_processed']}")
        logger.info(f"  Releases saved: {self.stats['releases_saved']}")
        logger.info(f"  Time elapsed: {elapsed:.1f}s")
        logger.info(f"  Processing rate: {rate:.2f} artists/second")
        logger.info(f"  Queue size: {len(self.artist_queue)}")
    
    def _print_final_stats(self):
        """Print final statistics."""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"\n{'='*50}")
        logger.info(f"TRAVERSAL COMPLETE")
        logger.info(f"{'='*50}")
        logger.info(f"Total artists processed: {self.stats['artists_processed']}")
        logger.info(f"Total releases saved: {self.stats['releases_saved']}")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Average rate: {self.stats['artists_processed']/elapsed:.2f} artists/second")
        logger.info(f"API requests: {self.client.request_count}")
        
        # Show distribution by depth
        for depth in range(self.max_depth + 1):
            count = sum(1 for a in self.artist_queue if a.depth == depth and a.discogs_id in self.processed_artists)
            if count > 0:
                logger.info(f"  Depth {depth}: {count} artists")


async def start_fast_traversal(
    discogs_id: str,
    db: Session,
    max_depth: int = 2
):
    """
    Start a fast traversal with depth limitation.
    
    Args:
        discogs_id: Starting artist's Discogs ID
        db: Database session
        max_depth: Maximum degrees of separation (default 2)
    """
    traverser = FastTraverser(
        start_artist_id=discogs_id,
        db=db,
        max_depth=max_depth
    )
    
    await traverser.traverse()
    return traverser


# Synchronous wrapper
def traverse_fast_sync(discogs_id: str, db: Session, max_depth: int = 2):
    """Synchronous wrapper for the fast traverser."""
    return asyncio.run(start_fast_traversal(discogs_id, db, max_depth))


if __name__ == "__main__":
    import asyncio
    from db.session import SessionLocal
    
    async def main():
        db = SessionLocal()
        try:
            # Example: traverse with max depth of 2
            await start_fast_traversal(
                discogs_id="17199",  # Starting artist
                db=db,
                max_depth=2  # Stop at 2 degrees of separation
            )
        finally:
            db.close()
    
    asyncio.run(main())
