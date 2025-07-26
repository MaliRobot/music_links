"""
Artist processor module.

This module handles the processing and persistence of artist data.
"""

import logging
from typing import Optional, Any, List
from sqlalchemy.orm import Session

from crud.artist import artist_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.api_client import DiscogsAPIClient
from services.release_processor import ReleaseProcessor

logger = logging.getLogger(__name__)


class ArtistProcessor:
    """Handles processing and persistence of artist data."""
    
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
        self.processed_count = 0
        self.created_count = 0
        self.existing_count = 0
        
    def get_or_create_artist(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist from database or create if not exists.
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Artist object or None if not found/created
        """
        logger.info(f"Getting or creating artist with Discogs ID: {discogs_id}")
        
        # Check if artist exists in database
        artist = artist_crud.get_by_discogs_id(self.db, discogs_id)
        
        if artist:
            logger.info(f"Artist found in database: {artist.name} (ID: {artist.discogs_id})")
            self.existing_count += 1
            self.processed_count += 1
            return artist
        
        # Fetch from API and create
        return self._fetch_and_create_artist(discogs_id)
    
    def _fetch_and_create_artist(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist from API and create in database.
        
        Args:
            discogs_id: Discogs ID of the artist
            
        Returns:
            Created artist object or None if failed
        """
        logger.info(f"Artist not in database, fetching from Discogs API")
        
        try:
            # Fetch from Discogs API
            artist_discogs = self.api_client.fetch_artist_by_discogs_id(discogs_id)
            
            if not artist_discogs:
                logger.warning(f"Artist with ID {discogs_id} not found in Discogs")
                return None
            
            logger.info(f"Found artist in Discogs: {artist_discogs.name}")
            
            # Fetch all releases data
            logger.info(f"Fetching releases for artist {artist_discogs.name}")
            releases = self.release_processor.fetch_all_releases(artist_discogs.releases)
            logger.info(f"Fetched {len(releases)} releases for artist {artist_discogs.name}")
            
            # Create artist object
            artist_in = ArtistCreate(
                name=artist_discogs.name,
                discogs_id=artist_discogs.id,
                page_url=artist_discogs.url,
                releases=releases
            )
            
            # Save to database
            logger.info(f"Creating artist {artist_discogs.name} in database with {len(releases)} releases")
            artist = artist_crud.create_with_releases(db=self.db, artist_in=artist_in)
            
            logger.info(f"Successfully created artist {artist.name} in database")
            self.created_count += 1
            self.processed_count += 1
            
            return artist
            
        except Exception as e:
            logger.error(f"Error creating artist {discogs_id}: {e}")
            return None
    
    def fetch_artist_releases(self, artist_id: str) -> Optional[Any]:
        """
        Fetch fresh release data for an artist from the API.
        
        Args:
            artist_id: Discogs ID of the artist
            
        Returns:
            Releases collection or None if error
        """
        try:
            logger.info(f"Fetching releases for artist ID: {artist_id}")
            artist = self.api_client.get_artist(artist_id=artist_id)
            
            if artist:
                logger.info("Successfully fetched artist releases from API")
                return artist.releases
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching artist releases: {e}")
            return None
    
    def get_statistics(self) -> dict:
        """
        Get statistics about artist processing.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'processed_count': self.processed_count,
            'created_count': self.created_count,
            'existing_count': self.existing_count,
            'creation_rate': self.created_count / max(self.processed_count, 1)
        }
