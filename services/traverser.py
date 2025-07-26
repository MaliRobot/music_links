"""
Single-threaded, maintainable version of the traverser module with comprehensive logging.

This module traverses artist relationships through the Discogs API, fetching artist
and release data, and storing them in the database.

Key features:
1. Single-threaded execution for simplicity and maintainability
2. Comprehensive logging at each step
3. Clear separation of concerns
4. Better error handling with detailed error messages
5. Rate limiting to respect API limits
6. Progress tracking
"""

import time
import logging
from typing import Set, Optional, List, Dict, Any
from dataclasses import dataclass, field
from contextlib import contextmanager

from sqlalchemy.orm import Session
import discogs_client.exceptions

from crud.artist import artist_crud
from crud.release import release_crud
from schemas.artist import ArtistCreate
from schemas.release import ReleaseCreate
from services.disco_conn import DiscoConnector, init_disco_fetcher


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create logger for this module
logger = logging.getLogger(__name__)

# Rate limiting configuration
RATE_LIMIT_REQUESTS_PER_MINUTE = 60
RATE_LIMIT_INTERVAL = 60.0 / RATE_LIMIT_REQUESTS_PER_MINUTE  # seconds between requests
RETRY_ATTEMPTS = 3
INITIAL_BACKOFF = 1.0  # seconds


class RateLimiter:
    """Simple rate limiter to respect API rate limits."""
    
    def __init__(self, requests_per_minute: int = RATE_LIMIT_REQUESTS_PER_MINUTE):
        self.interval = 60.0 / requests_per_minute
        self.last_request_time = 0
        logger.info(f"Initialized RateLimiter with {requests_per_minute} requests/minute")
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.interval:
            wait_time = self.interval - time_since_last_request
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()


class DiscoConnectorWithLogging:
    """Wrapper for DiscoConnector that adds logging and rate limiting."""
    
    def __init__(self, client: DiscoConnector):
        self.client = client
        self.rate_limiter = RateLimiter()
        self.request_count = 0
        logger.info("Initialized DiscoConnectorWithLogging wrapper")
    
    def _make_api_call(self, method_name: str, *args, **kwargs):
        """Make an API call with rate limiting, logging, and retry logic."""
        self.rate_limiter.wait_if_needed()
        self.request_count += 1
        
        logger.debug(f"API call #{self.request_count}: {method_name} with args={args}, kwargs={kwargs}")
        
        for attempt in range(RETRY_ATTEMPTS):
            try:
                method = getattr(self.client, method_name)
                result = method(*args, **kwargs)
                logger.debug(f"API call #{self.request_count} successful")
                return result
            
            except discogs_client.exceptions.HTTPError as e:
                if e.status_code == 429:  # Rate limit exceeded
                    if attempt < RETRY_ATTEMPTS - 1:
                        backoff = INITIAL_BACKOFF * (2 ** attempt)
                        logger.warning(f"Rate limit hit (attempt {attempt + 1}/{RETRY_ATTEMPTS}). "
                                     f"Waiting {backoff} seconds...")
                        time.sleep(backoff)
                    else:
                        logger.error(f"Rate limit exceeded after {RETRY_ATTEMPTS} attempts")
                        raise
                else:
                    logger.error(f"HTTP error in API call: {e}")
                    raise
            
            except Exception as e:
                logger.error(f"Unexpected error in API call {method_name}: {e}")
                raise
    
    def fetch_artist_by_discogs_id(self, discogs_id: str):
        """Fetch artist data by Discogs ID."""
        logger.info(f"Fetching artist with Discogs ID: {discogs_id}")
        return self._make_api_call('fetch_artist_by_discogs_id', discogs_id)
    
    def get_artist(self, artist_id: str):
        """Get artist data."""
        logger.info(f"Getting artist with ID: {artist_id}")
        return self._make_api_call('get_artist', artist_id)
    
    def get_release(self, release_id: str):
        """Get release data."""
        logger.debug(f"Getting release with ID: {release_id}")
        return self._make_api_call('get_release', release_id)


@dataclass
class StepTraverser:
    """Handles traversal for a single artist."""
    
    discogs_id: str
    client: DiscoConnectorWithLogging
    db: Session
    artists: Set[str] = field(default_factory=set)
    artist: Optional[Any] = None
    
    def __post_init__(self):
        logger.info(f"Created StepTraverser for artist ID: {self.discogs_id}")
    
    def get_or_create_artist(self):
        """Fetch artist from database or create if not exists."""
        logger.info(f"Getting or creating artist with Discogs ID: {self.discogs_id}")
        
        # Check if artist exists in database
        artist = artist_crud.get_by_discogs_id(self.db, self.discogs_id)
        
        if artist:
            logger.info(f"Artist found in database: {artist.name} (ID: {artist.discogs_id})")
        else:
            logger.info(f"Artist not in database, fetching from Discogs API")
            
            # Fetch from Discogs API
            artist_discogs = self.client.fetch_artist_by_discogs_id(self.discogs_id)
            
            if not artist_discogs:
                logger.warning(f"Artist with ID {self.discogs_id} not found in Discogs")
                return None
            
            logger.info(f"Found artist in Discogs: {artist_discogs.name}")
            logger.info(f"Fetching releases for artist {artist_discogs.name}")
            
            # Fetch all releases data
            releases = self._fetch_releases_data(artist_discogs.releases)
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
        
        self.artist = artist
        return artist
    
    def _fetch_releases_data(self, releases) -> List[ReleaseCreate]:
        """Fetch release data for all releases."""
        logger.info(f"Starting to fetch release data")
        
        release_list = []
        
        # First, check if releases is paginated
        if hasattr(releases, 'pages'):
            total_pages = releases.pages
            logger.info(f"Releases are paginated with {total_pages} pages")
            
            # Iterate through all pages
            for page_num in range(1, total_pages + 1):
                logger.info(f"Fetching release page {page_num}/{total_pages}")
                try:
                    page = releases.page(page_num)
                    for release in page:
                        release_data = self._create_release_data(release)
                        if release_data:
                            release_list.append(release_data)
                except Exception as e:
                    logger.error(f"Error fetching release page {page_num}: {e}")
        else:
            # Non-paginated releases
            logger.info(f"Processing non-paginated releases")
            for release in releases:
                release_data = self._create_release_data(release)
                if release_data:
                    release_list.append(release_data)
        
        logger.info(f"Successfully fetched {len(release_list)} releases")
        return release_list
    
    def _create_release_data(self, release) -> Optional[ReleaseCreate]:
        """Create ReleaseCreate object from release data."""
        try:
            logger.debug(f"Creating release data for: {release.title}")
            
            return ReleaseCreate(
                title=release.title,
                discogs_id=release.id,
                page_url=release.url,
                year=getattr(release, 'year', None),  # year might not always be present
            )
        except AttributeError as e:
            logger.error(f"Missing attribute in release data: {e}")
            return None
        except Exception as e:
            logger.error(f"Error creating release data: {e}")
            return None
    
    def get_artist_releases(self):
        """Get all releases for the current artist."""
        if not self.artist:
            logger.warning("No artist set, cannot get releases")
            return []
        
        logger.info(f"Getting releases for artist: {self.artist.name}")
        
        try:
            # Fetch fresh data from API
            artist = self.client.get_artist(artist_id=self.artist.discogs_id)
            logger.info(f"Successfully fetched artist releases from API")
            return artist.releases
        except Exception as e:
            logger.error(f"Error fetching artist releases: {e}")
            return []
    
    def check_artist_releases(self):
        """Check all releases for the artist and collect related artists."""
        logger.info(f"Checking releases for artist: {self.artist.name if self.artist else 'Unknown'}")
        
        releases = self.get_artist_releases()
        
        if hasattr(releases, 'pages'):
            total_pages = releases.pages
            logger.info(f"Processing {total_pages} pages of releases")
            
            # Process each page
            for page_num in range(1, total_pages + 1):
                logger.info(f"Processing release page {page_num}/{total_pages}")
                
                try:
                    page = releases.page(page_num)
                    for release in page:
                        self._process_release(release)
                except Exception as e:
                    logger.error(f"Error processing release page {page_num}: {e}")
        else:
            # Non-paginated releases
            logger.info(f"Processing non-paginated releases")
            for release in releases:
                self._process_release(release)
        
        logger.info(f"Found {len(self.artists)} related artists")
        return self.artists
    
    def _process_release(self, release):
        """Process a single release and extract artist information."""
        try:
            release_title = getattr(release, 'title', 'Unknown')
            logger.debug(f"Processing release: {release_title}")
            
            # Handle master releases
            if hasattr(release, 'main_release'):
                logger.debug(f"Release {release_title} is a master release, getting main release")
                release = release.main_release
            
            # Process different types of artist credits
            artists_found = 0
            
            # Main artists
            if hasattr(release, 'artists'):
                logger.debug(f"Processing {len(release.artists)} main artists for release {release_title}")
                for artist in release.artists:
                    if self._should_process_artist(artist):
                        self._process_artist_from_release(artist, release)
                        artists_found += 1
            
            # Extra artists (producers, remixers, etc.)
            if hasattr(release, 'extraartists'):
                logger.debug(f"Processing {len(release.extraartists)} extra artists for release {release_title}")
                for artist in release.extraartists:
                    if self._should_process_artist(artist):
                        self._process_artist_from_release(artist, release)
                        artists_found += 1
            
            # Credits
            if hasattr(release, 'credits'):
                logger.debug(f"Processing {len(release.credits)} credits for release {release_title}")
                for artist in release.credits:
                    if self._should_process_artist(artist):
                        self._process_artist_from_release(artist, release)
                        artists_found += 1
            
            if artists_found > 0:
                logger.debug(f"Found {artists_found} artists in release {release_title}")
        
        except discogs_client.exceptions.HTTPError as e:
            logger.error(f"HTTP error processing release: {e}")
        except AttributeError as e:
            logger.error(f"Attribute error processing release: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing release: {e}")
    
    def _should_process_artist(self, artist) -> bool:
        """Check if artist should be processed."""
        # Skip if it's the current artist
        if artist.id == self.artist.discogs_id:
            return False
        
        # Skip "Various" artists (compilations)
        if artist.name == 'Various':
            return False
        
        return True
    
    def _process_artist_from_release(self, artist, release):
        """Process an artist found in a release."""
        logger.debug(f"Processing artist {artist.name} (ID: {artist.id}) from release {release.title}")
        
        # Add to artists set for later traversal
        self.artists.add(artist.id)
        
        # Add release to artist in database
        self._add_release_to_artist(artist, release)
    
    def _add_release_to_artist(self, artist, release_discogs):
        """Add a release to an artist in the database."""
        logger.debug(f"Adding release {release_discogs.title} to artist {artist.name}")
        
        try:
            # Create release object
            release_in = ReleaseCreate(
                title=release_discogs.title,
                discogs_id=release_discogs.id,
                page_url=release_discogs.url,
                year=getattr(release_discogs, 'year', None),
            )
            
            # Check if artist exists in database
            print(artist.id, "->", artist.name, "->", release_discogs.title)
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
                
        except Exception as e:
            logger.error(f"Error adding release to artist {artist.name}: {e}")


@dataclass
class Traverser:
    """Main traverser class that orchestrates the traversal process."""
    
    discogs_id: str
    client: DiscoConnectorWithLogging
    db: Session
    checked: Set[str] = field(default_factory=set)
    count: int = 0
    max_artists: int = 100
    artists: Set[str] = field(default_factory=set)
    
    def __post_init__(self):
        logger.info(f"Initialized Traverser for artist ID {self.discogs_id} "
                   f"with max_artists={self.max_artists}")
    
    def begin_traverse(self):
        """Begin the traversal process."""
        logger.info("=" * 80)
        logger.info(f"Starting traversal from artist ID: {self.discogs_id}")
        logger.info("=" * 80)
        
        # Process the initial artist
        first_step = StepTraverser(
            discogs_id=self.discogs_id,
            client=self.client,
            db=self.db
        )
        
        artist = first_step.get_or_create_artist()
        
        if artist:
            logger.info(f"Initial artist: {artist.name}")
            self.checked.add(artist.discogs_id)
            
            # Get related artists from releases
            new_artists = first_step.check_artist_releases()
            self.artists.update(new_artists)
            
            logger.info(f"Found {len(new_artists)} related artists from initial artist")
            
            # Continue traversal
            return self.traverse_loop()
        else:
            logger.error(f"Could not find or create initial artist with ID {self.discogs_id}")
            return None
    
    def traverse_loop(self):
        """Main traversal loop."""
        logger.info(f"Starting traversal loop with {len(self.artists)} artists to process")
        
        while self.artists and self.count < self.max_artists:
            # Get next artist to process
            artist_id = self.artists.pop()
            
            # Skip if already checked
            if artist_id in self.checked:
                logger.debug(f"Skipping already checked artist ID: {artist_id}")
                continue
            
            self.count += 1
            progress_pct = (self.count / self.max_artists) * 100
            
            logger.info(f"\n--- Processing artist {self.count}/{self.max_artists} "
                       f"({progress_pct:.1f}%) ---")
            logger.info(f"Artist ID: {artist_id}")
            logger.info(f"Remaining artists in queue: {len(self.artists)}")
            
            try:
                # Process artist
                step = StepTraverser(
                    discogs_id=artist_id,
                    client=self.client,
                    db=self.db
                )
                
                artist = step.get_or_create_artist()
                
                if artist:
                    self.checked.add(artist.discogs_id)
                    
                    # Get related artists
                    new_artists = step.check_artist_releases()
                    
                    # Add new artists that haven't been checked
                    new_to_add = new_artists - self.checked
                    self.artists.update(new_to_add)
                    
                    logger.info(f"Added {len(new_to_add)} new artists to queue")
                else:
                    logger.warning(f"Could not process artist ID: {artist_id}")
                    
            except Exception as e:
                logger.error(f"Error processing artist {artist_id}: {e}")
                continue
        
        # Final summary
        logger.info("=" * 80)
        if self.count >= self.max_artists:
            logger.info(f"Traversal stopped: reached maximum of {self.max_artists} artists")
        elif not self.artists:
            logger.info("Traversal complete: no more artists to process")
        
        logger.info(f"Total artists processed: {self.count}")
        logger.info(f"Total artists checked: {len(self.checked)}")
        logger.info(f"Remaining in queue: {len(self.artists)}")
        logger.info("=" * 80)


def start_traversing(
    discogs_id: str,
    db: Session,
    max_artists: int = 20,
    log_level: str = "INFO"
) -> Traverser:
    """
    Start the traversal process.
    
    Args:
        discogs_id: Discogs ID of the starting artist
        db: Database session
        max_artists: Maximum number of artists to process
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Traverser object with traversal results
    """
    # Set logging level
    logger.setLevel(getattr(logging, log_level.upper()))
    
    logger.info("Initializing traversal system")
    
    # Initialize Discogs client
    sync_client = init_disco_fetcher()
    client = DiscoConnectorWithLogging(sync_client)
    
    logger.info(f"Connected to Discogs API")
    
    # Create traverser
    traverser = Traverser(
        discogs_id=discogs_id,
        client=client,
        max_artists=max_artists,
        db=db
    )
    
    # Start traversal and track time
    start_time = time.time()
    traverser.begin_traverse()
    elapsed = time.time() - start_time
    
    # Print summary
    logger.info(f"\nTraversal completed in {elapsed:.2f} seconds")
    logger.info(f"Average time per artist: {elapsed/max(traverser.count, 1):.2f} seconds")
    logger.info(f"Total API requests: {client.request_count}")
    
    return traverser


# Progress tracker context manager
@contextmanager
def progress_tracker(description: str):
    """Context manager for tracking progress of operations."""
    logger.info(f"Starting: {description}")
    start_time = time.time()
    
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Completed: {description} (took {elapsed:.2f} seconds)")


if __name__ == "__main__":
    # Example usage
    from db.session import SessionLocal
    
    # Configure detailed logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    
    db = SessionLocal()
    
    try:
        # Example: traverse starting from a specific artist
        # You can adjust the log_level to see more or less detail
        traverser = start_traversing(
            discogs_id="17199",
            db=db,
            max_artists=50,
            log_level="INFO"  # Change to DEBUG for even more detail
        )
        
        print(f"\nFinal Statistics:")
        print(f"  Artists processed: {traverser.count}")
        print(f"  Artists checked: {len(traverser.checked)}")
        print(f"  API requests made: {traverser.client.request_count}")
        
    finally:
        db.close()
