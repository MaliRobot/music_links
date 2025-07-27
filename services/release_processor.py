"""
Enhanced release processor module with accurate artist extraction.

This module handles the processing of releases and ensures accurate
extraction and counting of related artists.
"""

import logging
from typing import List, Optional, Set, Any, Dict, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

import discogs_client.exceptions
from sqlalchemy.orm import Session

from schemas.release import ReleaseCreate
from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate

logger = logging.getLogger(__name__)


@dataclass
class ReleaseProcessingStats:
    """Statistics for release processing."""
    total_processed: int = 0
    total_pages_fetched: int = 0
    artists_extracted: int = 0
    main_artists_found: int = 0
    extra_artists_found: int = 0
    credit_artists_found: int = 0
    errors: int = 0
    
    # Track which types of artists were found per release
    extraction_details: Dict[str, Dict[str, int]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'total_processed': self.total_processed,
            'total_pages_fetched': self.total_pages_fetched,
            'artists_extracted': self.artists_extracted,
            'main_artists_found': self.main_artists_found,
            'extra_artists_found': self.extra_artists_found,
            'credit_artists_found': self.credit_artists_found,
            'errors': self.errors,
            'average_artists_per_release': self.artists_extracted / max(self.total_processed, 1)
        }


@dataclass
class ExtractedArtist:
    """Information about an extracted artist."""
    discogs_id: str
    name: str
    role: str  # 'main', 'extra', 'credit'
    credit_type: Optional[str] = None  # For credits: 'Written-By', 'Producer', etc.


@dataclass  
class ProcessedRelease:
    """Container for processed release data."""
    release_data: ReleaseCreate
    artists: List[ExtractedArtist]
    
    @property
    def artist_ids(self) -> Set[str]:
        """Get unique artist IDs."""
        return {artist.discogs_id for artist in self.artists}


class ReleaseProcessor:
    """
    Enhanced release processor with accurate artist extraction.
    
    This processor ensures all artists are properly extracted and counted
    from releases, including main artists, extra artists, and credits.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the release processor.
        
        Args:
            db: Database session
        """
        self.db = db
        self.stats = ReleaseProcessingStats()
        
        # Cache for processed releases in this session
        self._release_cache: Dict[str, ProcessedRelease] = {}
        
        logger.info("Initialized ReleaseProcessor with session cache")
        
    def create_release_data(self, release: Any) -> Optional[ReleaseCreate]:
        """
        Create ReleaseCreate object from release data.
        
        Args:
            release: Release data from API
            
        Returns:
            ReleaseCreate object or None if creation fails
        """
        try:
            logger.debug(f"Creating release data for: {getattr(release, 'title', 'Unknown')}")
            
            return ReleaseCreate(
                title=release.title,
                discogs_id=release.id,
                page_url=release.url,
                year=getattr(release, 'year', None),
            )
        except AttributeError as e:
            self.stats.errors += 1
            logger.error(f"Missing attribute in release data: {e}")
            return None
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Error creating release data: {e}")
            return None
    
    def fetch_all_releases(self, releases: Any, page_callback=None) -> List[ReleaseCreate]:
        """
        Fetch release data for all releases, handling pagination.
        
        Args:
            releases: Releases collection from API (may be paginated)
            page_callback: Optional callback function called for each page
            
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
                    
                logger.debug(f"Fetching release page {page_num}/{total_pages}")
                try:
                    page = releases.page(page_num)
                    self.stats.total_pages_fetched += 1
                    
                    for release in page:
                        release_data = self.create_release_data(release)
                        if release_data:
                            release_list.append(release_data)
                            self.stats.total_processed += 1
                except Exception as e:
                    self.stats.errors += 1
                    logger.error(f"Error fetching release page {page_num}: {e}")
        else:
            # Non-paginated releases
            logger.info("Processing non-paginated releases")
            for release in releases:
                release_data = self.create_release_data(release)
                if release_data:
                    release_list.append(release_data)
                    self.stats.total_processed += 1
        
        logger.info(f"Successfully fetched {len(release_list)} releases")
        return release_list
    
    def extract_artists_from_release(
        self, 
        release: Any, 
        current_artist_id: str,
        include_extras: bool = True,
        include_credits: bool = True,
        detailed: bool = False
    ) -> Set[str]:
        """
        Extract all artist IDs from a release with accurate counting.
        
        This method ensures all artists are properly extracted by:
        1. Checking main artists
        2. Checking extra artists (if enabled)
        3. Checking credits (if enabled)
        4. Handling master releases properly
        5. Avoiding duplicates
        
        Args:
            release: Release data from API
            current_artist_id: ID of the current artist (to exclude)
            include_extras: Whether to include extra artists
            include_credits: Whether to include credits
            detailed: Whether to return detailed extraction info
            
        Returns:
            Set of unique artist IDs found in the release (excluding current)
        """
        # Check cache first
        release_id = str(getattr(release, 'id', ''))
        if release_id in self._release_cache:
            cached = self._release_cache[release_id]
            return cached.artist_ids - {current_artist_id}
        
        extracted_artists = []
        release_title = getattr(release, 'title', 'Unknown')
        
        try:
            logger.debug(f"Extracting artists from release: {release_title}")
            
            # Handle master releases
            actual_release = release
            if hasattr(release, 'main_release') and release.main_release:
                logger.debug(f"Release {release_title} is a master release, using main release")
                actual_release = release.main_release
            
            # 1. Extract main artists
            main_artists = self._extract_main_artists(actual_release)
            extracted_artists.extend(main_artists)
            self.stats.main_artists_found += len(main_artists)
            
            # 2. Extract extra artists (if enabled)
            if include_extras:
                extra_artists = self._extract_extra_artists(actual_release)
                extracted_artists.extend(extra_artists)
                self.stats.extra_artists_found += len(extra_artists)
            
            # 3. Extract credits (if enabled)
            if include_credits:
                credit_artists = self._extract_credit_artists(actual_release)
                extracted_artists.extend(credit_artists)
                self.stats.credit_artists_found += len(credit_artists)
            
            # Cache the processed release
            if release_id:
                processed = ProcessedRelease(
                    release_data=self.create_release_data(release),
                    artists=extracted_artists
                )
                self._release_cache[release_id] = processed
            
            # Get unique artist IDs, excluding current artist
            unique_ids = {artist.discogs_id for artist in extracted_artists} - {current_artist_id}
            
            self.stats.artists_extracted += len(unique_ids)
            logger.debug(f"Extracted {len(unique_ids)} unique artists from {release_title}")
            
            # Track extraction details
            if release_id:
                for artist in extracted_artists:
                    self.stats.extraction_details[release_id][artist.role] += 1
            
            return unique_ids
            
        except Exception as e:
            logger.error(f"Error extracting artists from release {release_title}: {e}")
            self.stats.errors += 1
            return set()
    
    def _extract_main_artists(self, release: Any) -> List[ExtractedArtist]:
        """
        Extract main artists from a release.
        
        Args:
            release: Release data from API
            
        Returns:
            List of extracted main artists
        """
        artists = []
        
        try:
            if hasattr(release, 'artists') and release.artists:
                logger.debug(f"Processing {len(release.artists)} main artists")
                for artist in release.artists:
                    if hasattr(artist, 'id'):
                        artists.append(ExtractedArtist(
                            discogs_id=str(artist.id),
                            name=getattr(artist, 'name', 'Unknown'),
                            role='main'
                        ))
        except Exception as e:
            logger.error(f"Error extracting main artists: {e}")
        
        return artists
    
    def _extract_extra_artists(self, release: Any) -> List[ExtractedArtist]:
        """
        Extract extra artists (featuring, remixers, etc.) from a release.
        
        Args:
            release: Release data from API
            
        Returns:
            List of extracted extra artists
        """
        artists = []
        
        try:
            if hasattr(release, 'extraartists') and release.extraartists:
                logger.debug(f"Processing {len(release.extraartists)} extra artists")
                for artist in release.extraartists:
                    if hasattr(artist, 'id'):
                        artists.append(ExtractedArtist(
                            discogs_id=str(artist.id),
                            name=getattr(artist, 'name', 'Unknown'),
                            role='extra',
                            credit_type=getattr(artist, 'role', None)
                        ))
        except Exception as e:
            logger.error(f"Error extracting extra artists: {e}")
        
        return artists
    
    def _extract_credit_artists(self, release: Any) -> List[ExtractedArtist]:
        """
        Extract artists from release credits.
        
        Args:
            release: Release data from API
            
        Returns:
            List of extracted credit artists
        """
        artists = []
        
        try:
            # Check for credits in tracklist
            if hasattr(release, 'tracklist') and release.tracklist:
                for track in release.tracklist:
                    # Track-level credits
                    if hasattr(track, 'extraartists') and track.extraartists:
                        for artist in track.extraartists:
                            if hasattr(artist, 'id'):
                                artists.append(ExtractedArtist(
                                    discogs_id=str(artist.id),
                                    name=getattr(artist, 'name', 'Unknown'),
                                    role='credit',
                                    credit_type=f"Track: {getattr(artist, 'role', 'Unknown')}"
                                ))
            
            # Check for release-level credits
            if hasattr(release, 'credits') and release.credits:
                for credit in release.credits:
                    if hasattr(credit, 'id'):
                        artists.append(ExtractedArtist(
                            discogs_id=str(credit.id),
                            name=getattr(credit, 'name', 'Unknown'),
                            role='credit',
                            credit_type=getattr(credit, 'role', None)
                        ))
        
        except Exception as e:
            logger.error(f"Error extracting credit artists: {e}")
        
        return artists
    
    def process_release_batch(
        self,
        releases: List[Any],
        current_artist_id: str,
        include_extras: bool = True,
        include_credits: bool = True
    ) -> Dict[str, Set[str]]:
        """
        Process a batch of releases and extract artists.
        
        Args:
            releases: List of release data from API
            current_artist_id: ID of the current artist
            include_extras: Whether to include extra artists
            include_credits: Whether to include credits
            
        Returns:
            Dictionary mapping release ID to set of artist IDs
        """
        result = {}
        
        for release in releases:
            release_id = str(getattr(release, 'id', ''))
            if release_id:
                artists = self.extract_artists_from_release(
                    release,
                    current_artist_id,
                    include_extras,
                    include_credits
                )
                result[release_id] = artists
        
        logger.info(f"Batch processed {len(releases)} releases, found {sum(len(a) for a in result.values())} total artist references")
        
        return result
    
    def save_release_to_artist(self, artist: Any, release: Any) -> bool:
        """
        Save a release to an artist in the database.
        
        Args:
            artist: Artist data
            release: Release data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            release_data = self.create_release_data(release)
            if not release_data:
                return False
            
            # Check if artist exists in database
            db_artist = artist_crud.get_by_discogs_id(self.db, str(artist.id))
            if not db_artist:
                # Create artist first
                artist_in = ArtistCreate(
                    name=artist.name,
                    discogs_id=str(artist.id),
                    page_url=artist.url,
                    releases=[]
                )
                db_artist = artist_crud.create(self.db, obj_in=artist_in)
            
            # Add release to artist
            if db_artist:
                # Check if release already exists
                existing = release_crud.get_by_discogs_id(self.db, release_data.discogs_id)
                if not existing:
                    release_crud.create_for_artist(self.db, obj_in=release_data, artist_id=db_artist.id)
                    logger.debug(f"Saved release {release_data.title} to artist {db_artist.name}")
                    return True
                else:
                    logger.debug(f"Release {release_data.title} already exists for artist {db_artist.name}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error saving release to artist: {e}")
            self.stats.errors += 1
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processor statistics.
        
        Returns:
            Dictionary with statistics
        """
        stats_dict = self.stats.to_dict()
        
        # Add extraction breakdown if available
        if self.stats.extraction_details:
            total_by_type = defaultdict(int)
            for release_details in self.stats.extraction_details.values():
                for role, count in release_details.items():
                    total_by_type[role] += count
            stats_dict['extraction_breakdown'] = dict(total_by_type)
        
        return stats_dict
    
    def reset_statistics(self):
        """Reset processor statistics."""
        self.stats = ReleaseProcessingStats()
        logger.info("Release processor statistics reset")
    
    def clear_cache(self):
        """Clear the session cache."""
        cache_size = len(self._release_cache)
        self._release_cache.clear()
        logger.info(f"Cleared release cache ({cache_size} entries)")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """
        Get information about the current cache.
        
        Returns:
            Dictionary with cache information
        """
        return {
            'size': len(self._release_cache),
            'release_ids': list(self._release_cache.keys()),
            'total_artists_cached': sum(
                len(r.artists) for r in self._release_cache.values()
            )
        }
