"""
Fast and simple traverser with depth-based exploration.
Focuses on speed over complex async patterns.

Strategy:
1. Process one artist completely before moving to next
2. Bulk save operations to minimize DB overhead  
3. Track depth/levels instead of artist count
4. Minimal async - only for API calls
5. Simple queue with depth tracking
"""

import asyncio
import time
from typing import Set, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import logging
from sqlalchemy.orm import Session

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple configuration
MAX_DEPTH = 2  # How many levels deep to go (2 = artist -> collaborator -> their collaborators)
API_DELAY = 0.1  # Small delay between API calls to avoid rate limits


class FastDiscoClient:
    """Simple wrapper for Discogs API calls with minimal overhead."""
    
    def __init__(self, client: DiscoConnector):
        self.client = client
        self.api_calls = 0
        self.last_call = 0
    
    async def _api_call(self, func, *args, **kwargs):
        """Execute API call with simple rate limiting."""
        # Simple rate limiting - ensure minimum time between calls
        elapsed = time.time() - self.last_call
        if elapsed < API_DELAY:
            await asyncio.sleep(API_DELAY - elapsed)
        
        # Execute in thread pool (since discogs_client is sync)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, func, *args, **kwargs)
        
        self.last_call = time.time()
        self.api_calls += 1
        
        if self.api_calls % 50 == 0:
            logger.info(f"API calls made: {self.api_calls}")
        
        return result
    
    async def get_artist(self, artist_id: str):
        """Get artist data."""
        try:
            return await self._api_call(self.client.fetch_artist_by_discogs_id, artist_id)
        except Exception as e:
            logger.error(f"Error fetching artist {artist_id}: {e}")
            return None
    
    async def get_release(self, release_id: str):
        """Get release data."""
        try:
            return await self._api_call(self.client.get_release, release_id)
        except Exception as e:
            logger.error(f"Error fetching release {release_id}: {e}")
            return None


@dataclass
class FastTraverser:
    """Fast traverser with depth-based exploration."""
    
    starting_artist_id: str
    client: FastDiscoClient
    db: Session
    max_depth: int = MAX_DEPTH
    
    # Tracking
    processed: Set[str] = field(default_factory=set)
    queue: deque = field(default_factory=deque)  # (artist_id, depth)
    artist_cache: Dict = field(default_factory=dict)  # Cache artist data
    stats: Dict = field(default_factory=lambda: {
        'artists_processed': 0,
        'releases_saved': 0,
        'relationships_created': 0,
        'start_time': time.time()
    })
    
    async def traverse(self):
        """Main traversal method."""
        logger.info(f"Starting traversal from artist {self.starting_artist_id} with max depth {self.max_depth}")
        
        # Add starting artist to queue
        self.queue.append((self.starting_artist_id, 0))
        
        while self.queue:
            artist_id, depth = self.queue.popleft()
            
            # Skip if already processed or too deep
            if artist_id in self.processed or depth > self.max_depth:
                continue
            
            logger.info(f"Processing artist {artist_id} at depth {depth}")
            
            # Process this artist
            collaborators = await self._process_artist(artist_id, depth)
            
            # Add collaborators to queue if not at max depth
            if depth < self.max_depth and collaborators:
                for collab_id in collaborators:
                    if collab_id not in self.processed:
                        self.queue.append((collab_id, depth + 1))
                logger.info(f"Added {len(collaborators)} collaborators to queue for depth {depth + 1}")
            
            # Mark as processed
            self.processed.add(artist_id)
            self.stats['artists_processed'] += 1
            
            # Log progress
            if self.stats['artists_processed'] % 10 == 0:
                self._log_progress()
        
        self._log_final_stats()
        return self.stats
    
    async def _process_artist(self, artist_id: str, depth: int) -> Set[str]:
        """Process a single artist completely."""
        
        # Check if artist exists in DB
        db_artist = artist_crud.get_by_discogs_id(self.db, artist_id)
        
        if db_artist:
            logger.info(f"Artist {artist_id} already in database")
            # Get collaborators from existing releases
            return await self._get_collaborators_from_db(db_artist)
        
        # Fetch from Discogs API
        artist_data = await self.client.get_artist(artist_id)
        if not artist_data:
            return set()
        
        logger.info(f"Fetched artist: {artist_data.name}")
        
        # Get all releases efficiently
        releases_data, collaborators = await self._fetch_all_releases_fast(artist_data)
        
        # Create artist with all releases in one go
        if releases_data:
            artist_in = ArtistCreate(
                name=artist_data.name,
                discogs_id=artist_data.id,
                page_url=artist_data.url,
                releases=releases_data
            )
            
            # Bulk save to database
            db_artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
            self.stats['releases_saved'] += len(releases_data)
            
            logger.info(f"Saved artist {artist_data.name} with {len(releases_data)} releases")
        
        return collaborators
    
    async def _fetch_all_releases_fast(self, artist_data) -> Tuple[List[ReleaseCreate], Set[str]]:
        """Fetch all releases and extract collaborators in one pass."""
        releases_data = []
        collaborators = set()
        
        try:
            releases_obj = artist_data.releases
            
            # Handle non-paginated results
            if isinstance(releases_obj, list):
                releases_list = releases_obj
            else:
                # Get all pages quickly
                releases_list = []
                total_pages = getattr(releases_obj, 'pages', 1)
                
                for page_num in range(1, total_pages + 1):
                    try:
                        page_releases = releases_obj.page(page_num)
                        releases_list.extend(page_releases)
                        
                        # Very small delay between pages
                        if page_num < total_pages:
                            await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.error(f"Error fetching page {page_num}: {e}")
                        continue
            
            logger.info(f"Processing {len(releases_list)} releases")
            
            # Process releases in chunks for memory efficiency
            chunk_size = 50
            for i in range(0, len(releases_list), chunk_size):
                chunk = releases_list[i:i + chunk_size]
                logger.info(f"Fetched {len(chunk)} releases")
                for release in chunk:
                    try:
                        # Create release data
                        release_data = ReleaseCreate(
                            title=getattr(release, 'title', 'Unknown'),
                            discogs_id=release.id,
                            page_url=getattr(release, 'url', ''),
                            year=getattr(release, 'year', 0)
                        )
                        releases_data.append(release_data)
                        logger.debug(f"Fetched {len(releases_data)} releases")
                        # Extract collaborators from this release
                        collab_ids = self._extract_collaborators(release, artist_data.id)
                        collaborators.update(collab_ids)
                        
                    except Exception as e:
                        logger.debug(f"Error processing release: {e}")
                        continue
                
                # Tiny delay between chunks
                if i + chunk_size < len(releases_list):
                    logger.debug(f"Processed {len(releases_data)} releases")
                    await asyncio.sleep(0.02)
            
        except Exception as e:
            logger.error(f"Error fetching releases: {e}")
        
        return releases_data, collaborators
    
    def _extract_collaborators(self, release, current_artist_id: str) -> Set[str]:
        """Extract collaborator IDs from a release."""
        collaborators = set()
        logger.info(f"Extract collaborators from release {release}")
        try:
            # Handle master releases
            if hasattr(release, 'main_release') and release.main_release:
                release = release.main_release

            # Check all artist fields
            for attr in ['artists', 'extraartists', 'credits']:
                logger.info(f"Extract {attr} from {release}")
                if hasattr(release, attr):
                    artists = getattr(release, attr)
                    if artists:
                        logger.debug(f"Found {len(artists)} artists")
                        for artist in artists:
                            if hasattr(artist, 'id') and hasattr(artist, 'name'):
                                # Skip self and generic artists
                                if (artist.id != current_artist_id and 
                                    artist.name not in ['Various', 'Various Artists', 'Unknown Artist']):
                                    collaborators.add(artist.id)
                                    logger.debug(f"Found {len(collaborators)} collaborators")
        except Exception as e:
            logger.debug(f"Error extracting collaborators: {e}")
        
        return collaborators
    
    async def _get_collaborators_from_db(self, db_artist) -> Set[str]:
        """Get collaborators from existing artist in database."""
        collaborators = set()
        
        # Get all releases for this artist
        for release in db_artist.releases:
            # Get all artists for this release
            for artist in release.artists:
                if artist.discogs_id != db_artist.discogs_id:
                    collaborators.add(artist.discogs_id)
        
        return collaborators
    
    def _log_progress(self):
        """Log current progress."""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['artists_processed'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"Progress: {self.stats['artists_processed']} artists, "
                   f"{self.stats['releases_saved']} releases, "
                   f"Queue: {len(self.queue)}, "
                   f"Rate: {rate:.1f} artists/sec")
    
    def _log_final_stats(self):
        """Log final statistics."""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info("="*60)
        logger.info("Traversal Complete!")
        logger.info(f"  Time: {elapsed:.1f} seconds")
        logger.info(f"  Artists processed: {self.stats['artists_processed']}")
        logger.info(f"  Releases saved: {self.stats['releases_saved']}")
        logger.info(f"  API calls: {self.client.api_calls}")
        logger.info(f"  Rate: {self.stats['artists_processed']/elapsed:.2f} artists/sec")
        logger.info("="*60)


async def traverse_fast(
    artist_id: str,
    db: Session,
    max_depth: int = 2
):
    """Fast traversal with depth limit."""
    
    # Initialize client
    sync_client = init_disco_fetcher()
    fast_client = FastDiscoClient(sync_client)
    
    # Create traverser
    traverser = FastTraverser(
        starting_artist_id=artist_id,
        client=fast_client,
        db=db,
        max_depth=max_depth
    )
    
    # Run traversal
    stats = await traverser.traverse()
    return stats


def traverse_fast_sync(artist_id: str, db: Session, max_depth: int = 2):
    """Synchronous wrapper."""
    return asyncio.run(traverse_fast(artist_id, db, max_depth))


# ============================================================================
# ALTERNATIVE: Even simpler synchronous version if async is causing issues
# ============================================================================

class SimpleSyncTraverser:
    """Dead simple synchronous traverser - no async complexity."""
    
    def __init__(self, artist_id: str, db: Session, max_depth: int = 2):
        self.start_id = artist_id
        self.db = db
        self.max_depth = max_depth
        self.client = init_disco_fetcher()
        self.processed = set()
        self.queue = deque([(artist_id, 0)])
        self.stats = {
            'artists': 0,
            'releases': 0,
            'start_time': time.time()
        }
    
    def traverse(self):
        """Simple BFS traversal."""
        while self.queue:
            artist_id, depth = self.queue.popleft()
            
            if artist_id in self.processed or depth > self.max_depth:
                continue
            
            print(f"Processing {artist_id} at depth {depth}")
            
            # Check DB first
            db_artist = artist_crud.get_by_discogs_id(self.db, artist_id)
            
            if not db_artist:
                # Fetch from API
                try:
                    artist = self.client.fetch_artist_by_discogs_id(artist_id)
                    if not artist:
                        continue
                    
                    # Get releases
                    releases_data = []
                    collaborators = set()
                    
                    for release in self._get_all_releases(artist):
                        # Save release
                        releases_data.append(ReleaseCreate(
                            title=release.title,
                            discogs_id=release.id,
                            page_url=release.url,
                            year=getattr(release, 'year', 0)
                        ))
                        
                        # Get collaborators
                        collaborators.update(self._get_release_artists(release, artist_id))
                    
                    # Save to DB
                    if releases_data:
                        artist_in = ArtistCreate(
                            name=artist.name,
                            discogs_id=artist.id,
                            page_url=artist.url,
                            releases=releases_data
                        )
                        db_artist = artist_crud.create_with_releases(self.db, artist_in=artist_in)
                        self.stats['releases'] += len(releases_data)
                        print(f"Saved {artist.name} with {len(releases_data)} releases")
                    
                    # Add collaborators to queue
                    if depth < self.max_depth:
                        for collab_id in collaborators:
                            if collab_id not in self.processed:
                                self.queue.append((collab_id, depth + 1))
                    
                    time.sleep(0.1)  # Simple rate limiting
                    
                except Exception as e:
                    print(f"Error processing {artist_id}: {e}")
            
            self.processed.add(artist_id)
            self.stats['artists'] += 1
        
        elapsed = time.time() - self.stats['start_time']
        print(f"\nComplete! Processed {self.stats['artists']} artists in {elapsed:.1f}s")
        return self.stats
    
    def _get_all_releases(self, artist):
        """Get all releases from artist."""
        releases = []
        try:
            releases_obj = artist.releases
            if hasattr(releases_obj, 'page'):
                for i in range(1, releases_obj.pages + 1):
                    releases.extend(releases_obj.page(i))
                    time.sleep(0.05)  # Small delay between pages
            else:
                releases = list(releases_obj)
        except Exception as e:
            print(f"Error getting releases: {e}")
        return releases
    
    def _get_release_artists(self, release, skip_id):
        """Extract artist IDs from release."""
        artist_ids = set()
        
        try:
            if hasattr(release, 'main_release') and release.main_release:
                release = release.main_release
            
            for attr in ['artists', 'extraartists']:
                if hasattr(release, attr):
                    for artist in getattr(release, attr):
                        if hasattr(artist, 'id') and artist.id != skip_id:
                            if artist.name not in ['Various', 'Various Artists']:
                                artist_ids.add(artist.id)
        except:
            pass
        
        return artist_ids


def traverse_simple(artist_id: str, db: Session, max_depth: int = 2):
    """Super simple synchronous traversal."""
    traverser = SimpleSyncTraverser(artist_id, db, max_depth)
    return traverser.traverse()


if __name__ == "__main__":
    import asyncio
    from db.session import SessionLocal
    
    # Example usage
    db = SessionLocal()
    
    try:
        # Option 1: Fast async version
        print("Running fast async traverser...")
        asyncio.run(traverse_fast("540828", db, max_depth=2))
        
        # Option 2: Simple sync version (if async is problematic)
        # print("Running simple sync traverser...")
        # traverse_simple("17199", db, max_depth=2)
        
    finally:
        db.close()
