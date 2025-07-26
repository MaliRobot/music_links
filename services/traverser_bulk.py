"""
Ultra-fast bulk traverser with aggressive optimization.
Focuses on bulk operations and minimal API calls.
"""

import time
from typing import Set, Dict, List, Optional
from collections import deque, defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import insert
import logging

from crud.artist import artist_crud
from crud.release import release_crud
from models.artist import Artist
from models.release import Release
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import init_disco_fetcher

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class BulkTraverser:
    """
    Optimized traverser using bulk operations and caching.
    Key optimizations:
    1. Bulk DB checks and inserts
    2. Caching to avoid redundant API calls
    3. Batch processing of releases
    4. Single-pass extraction of all data
    """
    
    def __init__(self, start_id: str, db: Session, max_depth: int = 2):
        self.db = db
        self.client = init_disco_fetcher()
        self.max_depth = max_depth
        
        # Queue management
        self.queue = deque([(start_id, 0)])
        self.processed = set()
        self.pending_artists = {}  # artist_id -> (name, url)
        self.pending_releases = {}  # release_id -> release_data
        self.artist_releases = defaultdict(list)  # artist_id -> [release_ids]
        
        # Stats
        self.stats = {
            'artists': 0,
            'releases': 0,
            'api_calls': 0,
            'db_hits': 0,
            'start_time': time.time()
        }
    
    def traverse(self):
        """Main traversal with bulk operations."""
        
        while self.queue:
            # Process queue in batches for efficiency
            batch = self._get_batch_from_queue(size=5)
            
            if not batch:
                break
            
            # Check which artists exist in DB (bulk check)
            existing = self._bulk_check_artists([aid for aid, _ in batch])
            
            # Process new artists
            for artist_id, depth in batch:
                if artist_id in existing:
                    self.stats['db_hits'] += 1
                    logger.info(f"[DB HIT] Artist {artist_id} exists")
                    # Get collaborators from DB if needed
                    if depth < self.max_depth:
                        self._queue_collaborators_from_db(artist_id, depth)
                else:
                    # Fetch from API
                    self._process_new_artist(artist_id, depth)
                
                self.processed.add(artist_id)
                self.stats['artists'] += 1
            
            # Bulk save accumulated data periodically
            if len(self.pending_artists) >= 10:
                self._bulk_save()
        
        # Final save
        if self.pending_artists:
            self._bulk_save()
        
        self._print_stats()
        return self.stats
    
    def _get_batch_from_queue(self, size: int) -> List:
        """Get a batch of items from queue."""
        batch = []
        while self.queue and len(batch) < size:
            artist_id, depth = self.queue.popleft()
            if artist_id not in self.processed and depth <= self.max_depth:
                batch.append((artist_id, depth))
        return batch
    
    def _bulk_check_artists(self, artist_ids: List[str]) -> Set[str]:
        """Check which artists exist in DB (bulk operation)."""
        existing = self.db.query(Artist.discogs_id).filter(
            Artist.discogs_id.in_(artist_ids)
        ).all()
        return {a[0] for a in existing}
    
    def _process_new_artist(self, artist_id: str, depth: int):
        """Process a new artist from API."""
        logger.info(f"[API] Fetching artist {artist_id} (depth={depth})")
        
        try:
            # Fetch artist
            artist = self.client.fetch_artist_by_discogs_id(artist_id)
            self.stats['api_calls'] += 1
            
            if not artist:
                return
            
            # Store artist data
            self.pending_artists[artist_id] = {
                'name': artist.name,
                'discogs_id': artist_id,
                'page_url': artist.url
            }
            
            # Process all releases efficiently
            collaborators = self._process_releases_bulk(artist, artist_id)
            
            # Queue collaborators if not at max depth
            if depth < self.max_depth:
                for collab_id in collaborators:
                    if collab_id not in self.processed:
                        self.queue.append((collab_id, depth + 1))
                logger.info(f"  -> Found {len(collaborators)} collaborators")
            
            # Small delay for rate limiting
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error processing {artist_id}: {e}")
    
    def _process_releases_bulk(self, artist, artist_id: str) -> Set[str]:
        """Process all releases in bulk, extracting all data in one pass."""
        collaborators = set()
        release_count = 0
        
        try:
            # Get all releases efficiently
            releases_obj = artist.releases
            
            # Determine total pages
            if hasattr(releases_obj, 'pages'):
                total_pages = releases_obj.pages
                logger.info(f"  -> Processing {total_pages} pages of releases")
                
                # Process all pages
                for page in range(1, total_pages + 1):
                    page_releases = releases_obj.page(page)
                    
                    for release in page_releases:
                        # Store release data
                        if release.id not in self.pending_releases:
                            self.pending_releases[release.id] = {
                                'title': release.title,
                                'discogs_id': release.id,
                                'page_url': release.url,
                                'year': getattr(release, 'year', None)
                            }
                            release_count += 1
                        
                        # Link to artist
                        self.artist_releases[artist_id].append(release.id)
                        
                        # Extract collaborators (single pass)
                        collaborators.update(self._extract_all_artists(release, artist_id))
                    
                    # Tiny delay between pages
                    if page < total_pages:
                        time.sleep(0.05)
                    
                    self.stats['api_calls'] += 1
            else:
                # Non-paginated
                for release in releases_obj:
                    if release.id not in self.pending_releases:
                        self.pending_releases[release.id] = {
                            'title': release.title,
                            'discogs_id': release.id,
                            'page_url': release.url,
                            'year': getattr(release, 'year', None)
                        }
                        release_count += 1
                    
                    self.artist_releases[artist_id].append(release.id)
                    collaborators.update(self._extract_all_artists(release, artist_id))
            
            logger.info(f"  -> Processed {release_count} releases")
            self.stats['releases'] += release_count
            
        except Exception as e:
            logger.error(f"Error processing releases: {e}")
        
        return collaborators
    
    def _extract_all_artists(self, release, skip_id: str) -> Set[str]:
        """Extract all artist IDs from a release in one pass."""
        artist_ids = set()
        
        try:
            # Check for main release
            if hasattr(release, 'main_release') and release.main_release:
                release = release.main_release
            
            # Check all artist fields at once
            for field in ['artists', 'extraartists', 'credits']:
                if hasattr(release, field):
                    artists = getattr(release, field)
                    if artists:
                        for a in artists:
                            if (hasattr(a, 'id') and 
                                a.id != skip_id and 
                                getattr(a, 'name', '') not in ['Various', 'Various Artists', '']):
                                artist_ids.add(a.id)
        except:
            pass
        
        return artist_ids
    
    def _bulk_save(self):
        """Perform bulk save of all pending data."""
        if not self.pending_artists:
            return
        
        logger.info(f"[BULK SAVE] Saving {len(self.pending_artists)} artists and {len(self.pending_releases)} releases...")
        
        try:
            # First, save all artists
            for artist_id, artist_data in self.pending_artists.items():
                # Get releases for this artist
                release_ids = self.artist_releases.get(artist_id, [])
                releases_data = []
                
                for rid in release_ids:
                    if rid in self.pending_releases:
                        release_data = self.pending_releases[rid]
                        releases_data.append(ReleaseCreate(**release_data))
                
                # Create artist with releases
                if releases_data:
                    artist_in = ArtistCreate(
                        name=artist_data['name'],
                        discogs_id=artist_data['discogs_id'],
                        page_url=artist_data['page_url'],
                        releases=releases_data
                    )
                    artist_crud.create_with_releases(self.db, artist_in=artist_in)
            
            # Clear pending data
            self.pending_artists.clear()
            self.pending_releases.clear()
            self.artist_releases.clear()
            
            logger.info("[BULK SAVE] Complete")
            
        except Exception as e:
            logger.error(f"Bulk save error: {e}")
            self.db.rollback()
    
    def _queue_collaborators_from_db(self, artist_id: str, depth: int):
        """Queue collaborators for an existing artist."""
        try:
            artist = artist_crud.get_by_discogs_id(self.db, artist_id)
            if artist and artist.releases:
                collaborators = set()
                for release in artist.releases:
                    for a in release.artists:
                        if a.discogs_id != artist_id:
                            collaborators.add(a.discogs_id)
                
                for collab_id in collaborators:
                    if collab_id not in self.processed:
                        self.queue.append((collab_id, depth + 1))
        except:
            pass
    
    def _print_stats(self):
        """Print final statistics."""
        elapsed = time.time() - self.stats['start_time']
        
        print("\n" + "="*60)
        print("TRAVERSAL COMPLETE")
        print("="*60)
        print(f"Time:            {elapsed:.1f} seconds")
        print(f"Artists:         {self.stats['artists']}")
        print(f"Releases:        {self.stats['releases']}")
        print(f"API calls:       {self.stats['api_calls']}")
        print(f"DB cache hits:   {self.stats['db_hits']}")
        print(f"Artists/sec:     {self.stats['artists']/elapsed:.2f}")
        print(f"API calls/sec:   {self.stats['api_calls']/elapsed:.2f}")
        print("="*60)


def fast_traverse(artist_id: str, db: Session, max_depth: int = 2):
    """
    Fast traversal with bulk operations.
    
    Args:
        artist_id: Starting artist's Discogs ID
        db: Database session
        max_depth: How many levels deep to traverse (default: 2)
    
    Returns:
        Statistics dictionary
    """
    traverser = BulkTraverser(artist_id, db, max_depth)
    return traverser.traverse()


if __name__ == "__main__":
    from db.session import SessionLocal
    
    # Example usage
    db = SessionLocal()
    
    try:
        # Run fast traversal
        stats = fast_traverse(
            artist_id="540828",  # Starting artist
            db=db,
            max_depth=2  # Only go 2 levels deep
        )
        
        print(f"\nTraversal returned: {stats}")
        
    finally:
        db.close()
