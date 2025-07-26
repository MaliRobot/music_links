"""
Simple synchronous traverser - no async complexity, just straight processing.
Often faster for APIs with rate limits since there's no overhead.
"""

import time
from typing import Set, List, Dict, Optional
from dataclasses import dataclass
from collections import deque
import logging
from sqlalchemy.orm import Session
import discogs_client.exceptions

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import init_disco_fetcher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
MAX_DEPTH = 2  # Maximum degrees of separation
SLEEP_BETWEEN_REQUESTS = 0.15  # Adjust based on your API rate limit
MAX_RELEASES_PER_ARTIST = 100  # Limit releases to fetch per artist
MAX_PAGES = 3  # Max pages of releases to fetch


@dataclass
class ArtistNode:
    """Track artist with depth level."""
    discogs_id: str
    name: str = ""
    depth: int = 0


class SimpleTraverser:
    """Simple, fast, synchronous traverser."""
    
    def __init__(self, start_artist_id: str, db: Session, max_depth: int = MAX_DEPTH):
        self.db = db
        self.client = init_disco_fetcher()
        self.max_depth = max_depth
        self.start_artist_id = start_artist_id
        
        # Tracking
        self.processed = set()
        self.queue = deque([ArtistNode(start_artist_id, depth=0)])
        
        # Stats
        self.stats = {
            'artists': 0,
            'releases': 0,
            'api_calls': 0,
            'start_time': time.time()
        }
    
    def traverse(self):
        """Main traversal loop - simple BFS with depth limit."""
        logger.info(f"Starting traversal from {self.start_artist_id}, max depth: {self.max_depth}")
        
        current_depth = 0
        
        while self.queue:
            # Get next artist
            artist_node = self.queue.popleft()
            
            # Check depth limit
            if artist_node.depth > self.max_depth:
                continue
            
            # Skip if already processed
            if artist_node.discogs_id in self.processed:
                continue
            
            # Update depth tracking
            if artist_node.depth > current_depth:
                current_depth = artist_node.depth
                logger.info(f"\n{'='*50}")
                logger.info(f"Now processing depth level {current_depth}")
                logger.info(f"Queue size: {len(self.queue)}, Processed: {len(self.processed)}")
                logger.info(f"{'='*50}")
            
            # Process this artist
            logger.info(f"\nProcessing artist {artist_node.discogs_id} (depth {artist_node.depth})")
            connected_artists = self.process_artist(artist_node)
            
            # Add connected artists to queue (if not at max depth)
            if artist_node.depth < self.max_depth:
                for artist_id in connected_artists:
                    if artist_id not in self.processed:
                        self.queue.append(ArtistNode(artist_id, depth=artist_node.depth + 1))
            
            # Mark as processed
            self.processed.add(artist_node.discogs_id)
            self.stats['artists'] += 1
            
            # Progress update
            if self.stats['artists'] % 5 == 0:
                self.print_progress()
        
        self.print_final_stats()
    
    def process_artist(self, artist_node: ArtistNode) -> Set[str]:
        """
        Process single artist - fetch, save, return connected artists.
        Simple and fast - no unnecessary complexity.
        """
        connected_artists = set()
        
        # Check if in database
        db_artist = artist_crud.get_by_discogs_id(self.db, artist_node.discogs_id)
        if db_artist:
            logger.info(f"  Found in DB: {db_artist.name}")
            # Get connected artists from existing releases
            return self.get_connected_from_db_artist(db_artist)
        
        # Fetch from Discogs
        try:
            time.sleep(SLEEP_BETWEEN_REQUESTS)  # Simple rate limiting
            artist = self.client.get_artist(artist_node.discogs_id)
            self.stats['api_calls'] += 1
        except Exception as e:
            logger.error(f"  Error fetching artist: {e}")
            return connected_artists
        
        if not artist:
            return connected_artists
        
        logger.info(f"  Fetched: {artist.name}")
        
        # Get releases and connected artists
        releases_data = []
        
        try:
            if hasattr(artist, 'releases') and artist.releases:
                releases = artist.releases
                
                # Handle pagination
                if hasattr(releases, 'pages'):
                    pages_to_fetch = min(releases.pages, MAX_PAGES)
                    logger.info(f"  Fetching {pages_to_fetch} pages of releases")
                    
                    for page_num in range(1, pages_to_fetch + 1):
                        time.sleep(SLEEP_BETWEEN_REQUESTS)
                        try:
                            page = releases.page(page_num)
                            self.stats['api_calls'] += 1
                            
                            for release in page[:MAX_RELEASES_PER_ARTIST]:
                                # Create release data
                                release_data = self.create_release_data(release)
                                if release_data:
                                    releases_data.append(release_data)
                                
                                # Get connected artists
                                connected = self.extract_artists_from_release(release, artist.id)
                                connected_artists.update(connected)
                                
                                if len(releases_data) >= MAX_RELEASES_PER_ARTIST:
                                    break
                        
                        except Exception as e:
                            logger.error(f"    Error on page {page_num}: {e}")
                            break
                        
                        if len(releases_data) >= MAX_RELEASES_PER_ARTIST:
                            break
                else:
                    # Non-paginated
                    for release in list(releases)[:MAX_RELEASES_PER_ARTIST]:
                        release_data = self.create_release_data(release)
                        if release_data:
                            releases_data.append(release_data)
                        
                        connected = self.extract_artists_from_release(release, artist.id)
                        connected_artists.update(connected)
        
        except Exception as e:
            logger.error(f"  Error processing releases: {e}")
        
        # Save to database
        if releases_data:
            try:
                artist_in = ArtistCreate(
                    name=artist.name,
                    discogs_id=str(artist.id),
                    page_url=getattr(artist, 'url', ''),
                    releases=releases_data
                )
                
                db_artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
                self.stats['releases'] += len(releases_data)
                logger.info(f"  Saved {len(releases_data)} releases, found {len(connected_artists)} connections")
                
            except Exception as e:
                logger.error(f"  DB error: {e}")
                self.db.rollback()
        
        return connected_artists
    
    def create_release_data(self, release) -> Optional[ReleaseCreate]:
        """Create release data object."""
        try:
            return ReleaseCreate(
                title=getattr(release, 'title', 'Unknown'),
                discogs_id=str(release.id),
                page_url=getattr(release, 'url', ''),
                year=getattr(release, 'year', 0)
            )
        except:
            return None
    
    def extract_artists_from_release(self, release, current_artist_id) -> Set[str]:
        """Extract connected artists from release."""
        connected = set()
        
        try:
            # Handle master releases
            if hasattr(release, 'main_release') and release.main_release:
                release = release.main_release
            
            # Check various artist fields
            for attr in ['artists', 'extraartists']:
                if hasattr(release, attr):
                    artists = getattr(release, attr)
                    if artists:
                        for artist in artists:
                            if (hasattr(artist, 'id') and 
                                str(artist.id) != str(current_artist_id) and
                                getattr(artist, 'name', '') not in ['Various', 'Unknown', '']):
                                connected.add(str(artist.id))
        except:
            pass
        
        return connected
    
    def get_connected_from_db_artist(self, db_artist) -> Set[str]:
        """Get connected artists from database artist."""
        connected = set()
        
        try:
            if hasattr(db_artist, 'releases'):
                for release in db_artist.releases:
                    if hasattr(release, 'artists'):
                        for artist in release.artists:
                            if artist.discogs_id != db_artist.discogs_id:
                                connected.add(artist.discogs_id)
        except:
            pass
        
        # If no connections in DB, fetch from API
        if not connected:
            try:
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                artist = self.client.get_artist(db_artist.discogs_id)
                self.stats['api_calls'] += 1
                
                if artist and hasattr(artist, 'releases'):
                    # Just check first few releases for connections
                    releases = list(artist.releases)[:10]
                    for release in releases:
                        connected.update(self.extract_artists_from_release(release, db_artist.discogs_id))
            except:
                pass
        
        return connected
    
    def print_progress(self):
        """Print progress stats."""
        elapsed = time.time() - self.stats['start_time']
        rate = self.stats['artists'] / elapsed if elapsed > 0 else 0
        
        logger.info(f"  Progress: {self.stats['artists']} artists, "
                   f"{self.stats['releases']} releases, "
                   f"{elapsed:.1f}s ({rate:.2f} artists/sec)")
    
    def print_final_stats(self):
        """Print final statistics."""
        elapsed = time.time() - self.stats['start_time']
        
        logger.info(f"\n{'='*50}")
        logger.info(f"TRAVERSAL COMPLETE")
        logger.info(f"{'='*50}")
        logger.info(f"Artists processed: {self.stats['artists']}")
        logger.info(f"Releases saved: {self.stats['releases']}")
        logger.info(f"API calls: {self.stats['api_calls']}")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Rate: {self.stats['artists']/elapsed:.2f} artists/sec")
        logger.info(f"{'='*50}")


def traverse_simple(discogs_id: str, db: Session, max_depth: int = 2):
    """
    Simple traversal function.
    
    Args:
        discogs_id: Starting artist ID
        db: Database session
        max_depth: Maximum degrees of separation (default 2)
    """
    traverser = SimpleTraverser(discogs_id, db, max_depth)
    traverser.traverse()
    return traverser


if __name__ == "__main__":
    from db.session import SessionLocal
    
    db = SessionLocal()
    try:
        # Example usage
        traverse_simple(
            discogs_id="17199",  # Starting artist
            db=db,
            max_depth=2  # 2 degrees of separation
        )
    finally:
        db.close()
