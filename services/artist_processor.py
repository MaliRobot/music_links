"""
Enhanced artist processor module with accurate tracking.

This module handles the processing and persistence of artist data
with improved tracking and statistics.
"""

import logging
from typing import Optional, Any, List, Dict, Set
from dataclasses import dataclass, field
from sqlalchemy.orm import Session

from crud.artist import artist_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.api_client import DiscogsAPIClient
from services.release_processor import ReleaseProcessor

logger = logging.getLogger(__name__)


@dataclass
class ArtistProcessingStats:
    """Statistics for artist processing."""
    total_processed: int = 0
    created_new: int = 0
    found_existing: int = 0
    api_fetches: int = 0
    database_hits: int = 0
    errors: int = 0
    releases_fetched: Dict[str, int] = field(default_factory=dict)  # artist_id -> release_count

    @property
    def creation_rate(self) -> float:
        """Get the rate of new artist creation."""
        if self.total_processed == 0:
            return 0.0
        return self.created_new / self.total_processed

    @property
    def cache_hit_rate(self) -> float:
        """Get the database cache hit rate."""
        total_lookups = self.api_fetches + self.database_hits
        if total_lookups == 0:
            return 0.0
        return self.database_hits / total_lookups

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'total_processed': self.total_processed,
            'created_new': self.created_new,
            'found_existing': self.found_existing,
            'api_fetches': self.api_fetches,
            'database_hits': self.database_hits,
            'errors': self.errors,
            'total_releases_fetched': sum(self.releases_fetched.values()),
            'creation_rate': self.creation_rate,
            'cache_hit_rate': self.cache_hit_rate
        }


class ArtistProcessor:
    """
    Enhanced artist processor with accurate tracking.
    
    This processor ensures accurate counting of artists throughout
    the traversal process.
    """
    
    def __init__(self, db: Session, api_client: DiscogsAPIClient):
        """
        Initialize the artist processor.
        
        Args:
            db: Database session
            api_client: API client for fetching data
        """
        self.db = db
        self.api_client = api_client
        self.release_processor = ReleaseProcessor(db)
        self.stats = ArtistProcessingStats()
        
        # Cache for this session to avoid repeated API calls
        self._session_cache: Dict[str, Any] = {}
        
        logger.info("Initialized ArtistProcessor with session cache")
        
    def get_or_create_artist(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist from database or create if not exists.
        
        This method ensures accurate counting by:
        1. Checking the session cache first
        2. Checking the database
        3. Fetching from API only if necessary
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Artist object or None if not found/created
        """
        logger.debug(f"Processing artist with Discogs ID: {discogs_id}")
        
        # Check session cache first
        if discogs_id in self._session_cache:
            logger.debug(f"Artist {discogs_id} found in session cache")
            return self._session_cache[discogs_id]
        
        # Check if artist exists in database
        artist = artist_crud.get_by_discogs_id(self.db, discogs_id)
        
        if artist:
            logger.info(f"Artist found in database: {artist.name} (ID: {artist.discogs_id})")
            self.stats.found_existing += 1
            self.stats.database_hits += 1
            self.stats.total_processed += 1
            
            # Add to session cache
            self._session_cache[discogs_id] = artist
            return artist
        
        # Fetch from API and create
        artist = self._fetch_and_create_artist(discogs_id)
        if artist:
            self._session_cache[discogs_id] = artist
        
        return artist
    
    def _fetch_and_create_artist(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist from API and create in database.
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Created artist object or None if failed
        """
        logger.info(f"Artist {discogs_id} not in database, fetching from Discogs API")
        
        try:
            # Fetch from Discogs API
            artist_discogs = self.api_client.fetch_artist_by_discogs_id(discogs_id)
            self.stats.api_fetches += 1
            
            if not artist_discogs:
                logger.warning(f"Artist with ID {discogs_id} not found in Discogs")
                self.stats.errors += 1
                return None
            
            logger.info(f"Found artist in Discogs: {artist_discogs.name}")
            
            # Fetch all releases data
            logger.info(f"Fetching releases for artist {artist_discogs.name}")
            releases = self.release_processor.fetch_all_releases(artist_discogs.releases)
            
            release_count = len(releases)
            logger.info(f"Fetched {release_count} releases for artist {artist_discogs.name}")
            self.stats.releases_fetched[discogs_id] = release_count
            
            # Create artist object
            artist_in = ArtistCreate(
                name=artist_discogs.name,
                discogs_id=artist_discogs.id,
                page_url=artist_discogs.url,
                releases=releases
            )
            
            # Save to database
            logger.info(f"Creating artist {artist_discogs.name} in database with {release_count} releases")
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
            
            if artist:
                logger.info(f"Successfully created artist {artist_discogs.name} in database")
                self.stats.created_new += 1
                self.stats.total_processed += 1
                return artist
            else:
                logger.error(f"Failed to create artist {artist_discogs.name} in database")
                self.stats.errors += 1
                return None
                
        except Exception as e:
            logger.error(f"Error fetching/creating artist {discogs_id}: {e}")
            self.stats.errors += 1
            return None
    
    def fetch_artist_releases(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist releases from API.
        
        This method returns the raw releases object from the API,
        allowing for lazy evaluation and pagination handling.
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Releases object from API or None if failed
        """
        logger.debug(f"Fetching releases for artist {discogs_id}")
        
        try:
            # Get artist from API
            artist = self.api_client.get_artist(discogs_id)
            if not artist:
                logger.warning(f"Artist {discogs_id} not found in API")
                return None
            
            # Return the releases object (may be paginated)
            return artist.releases
            
        except Exception as e:
            logger.error(f"Error fetching releases for artist {discogs_id}: {e}")
            self.stats.errors += 1
            return None
    
    def get_artist_info(self, discogs_id: str) -> Optional[Dict[str, Any]]:
        """
        Get basic artist information without creating in database.
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Dictionary with artist info or None if not found
        """
        try:
            # Check cache first
            if discogs_id in self._session_cache:
                artist = self._session_cache[discogs_id]
                return {
                    'id': artist.discogs_id,
                    'name': artist.name,
                    'url': artist.page_url,
                    'in_database': True
                }
            
            # Check database
            artist = artist_crud.get_by_discogs_id(self.db, discogs_id)
            if artist:
                return {
                    'id': artist.discogs_id,
                    'name': artist.name,
                    'url': artist.page_url,
                    'in_database': True
                }
            
            # Fetch from API (without creating)
            artist_api = self.api_client.get_artist(discogs_id)
            if artist_api:
                return {
                    'id': artist_api.id,
                    'name': artist_api.name,
                    'url': artist_api.url,
                    'in_database': False
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting artist info for {discogs_id}: {e}")
            return None
    
    def bulk_check_existence(self, discogs_ids: Set[str]) -> Dict[str, bool]:
        """
        Check existence of multiple artists in database.
        
        Args:
            discogs_ids: Set of Discogs IDs to check
            
        Returns:
            Dictionary mapping discogs_id to existence boolean
        """
        result = {}
        
        for discogs_id in discogs_ids:
            # Check cache first
            if discogs_id in self._session_cache:
                result[discogs_id] = True
                continue
            
            # Check database
            artist = artist_crud.get_by_discogs_id(self.db, discogs_id)
            result[discogs_id] = artist is not None
            
            if artist:
                self._session_cache[discogs_id] = artist
        
        existing_count = sum(1 for exists in result.values() if exists)
        logger.info(f"Bulk checked {len(discogs_ids)} artists: {existing_count} exist in database")
        
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processor statistics.
        
        Returns:
            Dictionary with statistics
        """
        return self.stats.to_dict()
    
    def reset_statistics(self):
        """Reset processor statistics."""
        self.stats = ArtistProcessingStats()
        logger.info("Artist processor statistics reset")
    
    def clear_cache(self):
        """Clear the session cache."""
        cache_size = len(self._session_cache)
        self._session_cache.clear()
        logger.info(f"Cleared session cache ({cache_size} entries)")
