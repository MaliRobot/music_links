"""
Release processor module.

This module handles the processing of releases and extraction of artist information.
"""

import logging
from typing import List, Optional, Set, Any
from dataclasses import dataclass

import discogs_client.exceptions
from sqlalchemy.orm import Session

from schemas.release import ReleaseCreate
from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate

logger = logging.getLogger(__name__)


@dataclass
class ProcessedRelease:
    """Container for processed release data."""
    release_data: ReleaseCreate
    artist_ids: Set[str]
    
    
class ReleaseProcessor:
    """Handles processing of releases and extraction of related artists."""
    
    def __init__(self, db: Session):
        """
        Initialize the release processor.
        
        Args:
            db: Database session
        """
        self.db = db
        self.processed_count = 0
        self.error_count = 0
        
    def create_release_data(self, release: Any) -> Optional[ReleaseCreate]:
        """
        Create ReleaseCreate object from release data.
        
        Args:
            release: Release data from API
            
        Returns:
            ReleaseCreate object or None if creation fails
        """
        try:
            logger.debug(f"Creating release data for: {release.title}")
            
            return ReleaseCreate(
                title=release.title,
                discogs_id=release.id,
                page_url=release.url,
                year=getattr(release, 'year', None),  # year might not always be present
            )
        except AttributeError as e:
            self.error_count += 1
            logger.error(f"Missing attribute in release data: {e}")
            return None
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error creating release data: {e}")
            return None
    
    def fetch_all_releases(self, releases: Any, page_callback=None) -> List[ReleaseCreate]:
        """
        Fetch release data for all releases, handling pagination.
        
        Args:
            releases: Releases collection from API (may be paginated)
            page_callback: Optional callback function called for each page (for progress tracking)
            
        Returns:
            List of ReleaseCreate objects
        """
        logger.info("Starting to fetch release data")
        release_list = []
        
        # Check if releases is paginated
        if hasattr(releases, 'pages'):
            total_pages = releases.pages
            logger.info(f"Releases are paginated with {total_pages} pages")
            
            # Iterate through all pages
            for page_num in range(1, total_pages + 1):
                if page_callback:
                    page_callback(page_num, total_pages)
                    
                logger.info(f"Fetching release page {page_num}/{total_pages}")
                try:
                    page = releases.page(page_num)
                    for release in page:
                        release_data = self.create_release_data(release)
                        if release_data:
                            release_list.append(release_data)
                            self.processed_count += 1
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Error fetching release page {page_num}: {e}")
        else:
            # Non-paginated releases
            logger.info("Processing non-paginated releases")
            for release in releases:
                release_data = self.create_release_data(release)
                if release_data:
                    release_list.append(release_data)
                    self.processed_count += 1
        
        logger.info(f"Successfully fetched {len(release_list)} releases")
        return release_list
    
    def extract_artists_from_release(
        self, 
        release: Any, 
        current_artist_id: str,
        include_extras: bool = True,
        include_credits: bool = True
    ) -> Set[str]:
        """
        Extract artist IDs from a release.
        
        Args:
            release: Release data from API
            current_artist_id: ID of the current artist (to exclude from results)
            include_extras: Whether to include extra artists (producers, remixers, etc.)
            include_credits: Whether to include credits
            
        Returns:
            Set of artist IDs found in the release
        """
        artist_ids = set()
        
        try:
            release_title = getattr(release, 'title', 'Unknown')
            logger.debug(f"Extracting artists from release: {release_title}")
            
            # Handle master releases
            if hasattr(release, 'main_release'):
                logger.debug(f"Release {release_title} is a master release, getting main release")
                release = release.main_release
            
            # Main artists
            if hasattr(release, 'artists'):
                logger.debug(f"Processing {len(release.artists)} main artists")
                for artist in release.artists:
                    if self._should_include_artist(artist, current_artist_id):
                        artist_ids.add(artist.id)
            
            # Extra artists (producers, remixers, etc.)
            if include_extras and hasattr(release, 'extraartists'):
                logger.debug(f"Processing {len(release.extraartists)} extra artists")
                for artist in release.extraartists:
                    if self._should_include_artist(artist, current_artist_id):
                        artist_ids.add(artist.id)
            
            # Credits
            if include_credits and hasattr(release, 'credits'):
                logger.debug(f"Processing {len(release.credits)} credits")
                for artist in release.credits:
                    if self._should_include_artist(artist, current_artist_id):
                        artist_ids.add(artist.id)
            
            if artist_ids:
                logger.debug(f"Found {len(artist_ids)} artists in release {release_title}")
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error extracting artists from release: {e}")
        
        return artist_ids
    
    def _should_include_artist(self, artist: Any, current_artist_id: str) -> bool:
        """
        Check if artist should be included in results.
        
        Args:
            artist: Artist data from API
            current_artist_id: ID of the current artist
            
        Returns:
            True if artist should be included
        """
        # Skip if it's the current artist
        if artist.id == current_artist_id:
            return False
        
        # Skip "Various" artists (compilations)
        if artist.name == 'Various':
            return False
        
        return True
    
    def save_release_to_artist(
        self, 
        artist: Any, 
        release_discogs: Any
    ) -> bool:
        """
        Save a release to an artist in the database.
        
        Args:
            artist: Artist data from API
            release_discogs: Release data from API
            
        Returns:
            True if successful, False otherwise
        """
        logger.debug(f"Saving release {release_discogs.title} to artist {artist.name}")
        
        try:
            # Create release object
            release_in = self.create_release_data(release_discogs)
            if not release_in:
                return False
            
            # Check if artist exists in database
            db_artist = artist_crud.get_by_discogs_id(db=self.db, discogs_id=artist.id)
            
            if db_artist:
                logger.debug(f"Artist {artist.name} exists in database, adding release")
                
                # Check if release exists
                release = release_crud.get_by_discogs_id(db=self.db, discogs_id=release_discogs.id)
                
                if not release:
                    logger.debug(f"Creating new release {release_discogs.title} in database")
                    release = release_crud.create(db=self.db, obj_in=release_in)
                
                # Add release to artist
                artist_crud.add_artist_release(db=self.db, artist_id=db_artist.id, release=release)
                logger.debug(f"Successfully added release to artist {artist.name}")
            else:
                logger.debug(f"Artist {artist.name} not in database, creating with release")
                
                # Create artist with release
                artist_crud.create_with_releases(
                    db=self.db,
                    artist_in=ArtistCreate(
                        name=artist.name,
                        discogs_id=artist.id,
                        page_url=artist.url,
                        releases=[release_in]
                    )
                )
                logger.debug(f"Successfully created artist {artist.name} with release")
            
            return True
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error saving release to artist {artist.name}: {e}")
            return False
    
    def get_statistics(self) -> dict:
        """
        Get statistics about release processing.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(self.processed_count, 1)
        }
