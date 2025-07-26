"""
Traversal manager module.

This module manages the overall traversal process and coordinates between components.
"""

import logging
import time
from typing import Set, Optional, Dict, Any
from dataclasses import dataclass, field
from contextlib import contextmanager

from sqlalchemy.orm import Session

from api_client import DiscogsAPIClient
from artist_processor import ArtistProcessor
from release_processor import ReleaseProcessor

logger = logging.getLogger(__name__)


@dataclass
class TraversalStatistics:
    """Container for traversal statistics."""
    artists_processed: int = 0
    artists_checked: int = 0
    releases_processed: int = 0
    api_requests: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    @property
    def elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def average_time_per_artist(self) -> float:
        """Get average time per artist in seconds."""
        if self.artists_processed == 0:
            return 0.0
        return self.elapsed_time / self.artists_processed
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary."""
        return {
            'artists_processed': self.artists_processed,
            'artists_checked': self.artists_checked,
            'releases_processed': self.releases_processed,
            'api_requests': self.api_requests,
            'errors': self.errors,
            'elapsed_time': self.elapsed_time,
            'average_time_per_artist': self.average_time_per_artist
        }


@dataclass
class TraversalConfig:
    """Configuration for traversal process."""
    max_artists: int = 100
    include_extra_artists: bool = True
    include_credits: bool = True
    log_progress_interval: int = 10  # Log progress every N artists


class TraversalQueue:
    """Manages the queue of artists to process."""
    
    def __init__(self, max_size: Optional[int] = None):
        """
        Initialize the queue.
        
        Args:
            max_size: Optional maximum queue size to prevent unbounded growth
        """
        self.pending: Set[str] = set()
        self.checked: Set[str] = set()
        self.max_size = max_size
        
    def add(self, artist_id: str) -> bool:
        """
        Add an artist to the queue if not already checked and queue has space.
        
        Returns:
            True if added, False if already checked or queue is full
        """
        if artist_id not in self.checked:
            if self.max_size is None or len(self.pending) < self.max_size:
                self.pending.add(artist_id)
                return True
            else:
                logger.debug(f"Queue is full (max_size={self.max_size}), not adding artist {artist_id}")
        return False
    
    def add_multiple(self, artist_ids: Set[str]) -> int:
        """
        Add multiple artists to the queue.
        
        Returns:
            Number of artists actually added
        """
        new_artists = artist_ids - self.checked
        
        if self.max_size is not None:
            # Limit additions to available space
            available_space = max(0, self.max_size - len(self.pending))
            if available_space < len(new_artists):
                logger.debug(f"Queue near capacity, adding only {available_space} of {len(new_artists)} artists")
                new_artists = set(list(new_artists)[:available_space])
        
        before_size = len(self.pending)
        self.pending.update(new_artists)
        return len(self.pending) - before_size
    
    def get_next(self) -> Optional[str]:
        """
        Get next artist to process.
        
        Returns:
            Artist ID or None if queue is empty
        """
        if self.pending:
            artist_id = self.pending.pop()
            self.checked.add(artist_id)
            return artist_id
        return None
    
    def mark_checked(self, artist_id: str):
        """Mark an artist as checked."""
        self.checked.add(artist_id)
        self.pending.discard(artist_id)
    
    @property
    def size(self) -> int:
        """Get number of pending artists."""
        return len(self.pending)
    
    @property
    def total_checked(self) -> int:
        """Get total number of checked artists."""
        return len(self.checked)


class SingleArtistTraverser:
    """Handles traversal for a single artist."""
    
    def __init__(
        self,
        artist_processor: ArtistProcessor,
        release_processor: ReleaseProcessor,
        api_client: DiscogsAPIClient,
        config: TraversalConfig
    ):
        """
        Initialize single artist traverser.
        
        Args:
            artist_processor: Processor for artist data
            release_processor: Processor for release data
            api_client: API client
            config: Traversal configuration
        """
        self.artist_processor = artist_processor
        self.release_processor = release_processor
        self.api_client = api_client
        self.config = config
        
    def process_artist(self, discogs_id: str) -> Set[str]:
        """
        Process a single artist and return related artist IDs.
        
        Args:
            discogs_id: Discogs ID of the artist to process
            
        Returns:
            Set of related artist IDs found
        """
        logger.info(f"Processing artist with ID: {discogs_id}")
        related_artists = set()
        
        # Get or create artist
        artist = self.artist_processor.get_or_create_artist(discogs_id)
        if not artist:
            logger.warning(f"Could not process artist ID: {discogs_id}")
            return related_artists
        
        # Fetch and process releases
        releases = self.artist_processor.fetch_artist_releases(artist.discogs_id)
        if not releases:
            logger.warning(f"No releases found for artist: {artist.name}")
            return related_artists
        
        # Extract related artists from releases
        related_artists = self._process_releases(releases, artist.discogs_id)
        logger.info(f"Found {len(related_artists)} related artists for {artist.name}")
        
        return related_artists
    
    def _process_releases(self, releases: Any, current_artist_id: str) -> Set[str]:
        """
        Process releases and extract related artists.
        
        Args:
            releases: Releases collection from API
            current_artist_id: ID of the current artist
            
        Returns:
            Set of related artist IDs
        """
        all_artists = set()
        
        # Handle pagination
        if hasattr(releases, 'pages'):
            total_pages = releases.pages
            logger.info(f"Processing {total_pages} pages of releases")
            
            for page_num in range(1, total_pages + 1):
                logger.debug(f"Processing release page {page_num}/{total_pages}")
                
                try:
                    page = releases.page(page_num)
                    for release in page:
                        artists = self.release_processor.extract_artists_from_release(
                            release,
                            current_artist_id,
                            include_extras=self.config.include_extra_artists,
                            include_credits=self.config.include_credits
                        )
                        all_artists.update(artists)
                        
                        # Also save releases to artists in database
                        self._save_release_to_artists(release, artists)
                        
                except Exception as e:
                    logger.error(f"Error processing release page {page_num}: {e}")
        else:
            # Non-paginated releases
            logger.info("Processing non-paginated releases")
            for release in releases:
                artists = self.release_processor.extract_artists_from_release(
                    release,
                    current_artist_id,
                    include_extras=self.config.include_extra_artists,
                    include_credits=self.config.include_credits
                )
                all_artists.update(artists)
                
                # Also save releases to artists in database
                self._save_release_to_artists(release, artists)
        
        return all_artists
    
    def _save_release_to_artists(self, release: Any, artist_ids: Set[str]):
        """
        Save release to artists in the database.
        
        Args:
            release: Release data from API
            artist_ids: Set of artist IDs to save the release to
        """
        for artist_id in artist_ids:
            try:
                # Get artist data from API
                artist = self.api_client.get_artist(artist_id)
                if artist:
                    self.release_processor.save_release_to_artist(artist, release)
            except Exception as e:
                logger.error(f"Error saving release to artist {artist_id}: {e}")


class TraversalManager:
    """Manages the overall traversal process."""
    
    def __init__(
        self,
        db: Session,
        api_client: DiscogsAPIClient,
        config: Optional[TraversalConfig] = None
    ):
        """
        Initialize traversal manager.
        
        Args:
            db: Database session
            api_client: API client
            config: Traversal configuration
        """
        self.db = db
        self.api_client = api_client
        self.config = config or TraversalConfig()
        
        # Initialize processors
        self.artist_processor = ArtistProcessor(db, api_client)
        self.release_processor = ReleaseProcessor(db)
        
        # Initialize traverser
        self.single_traverser = SingleArtistTraverser(
            self.artist_processor,
            self.release_processor,
            api_client,
            self.config
        )
        
        # Initialize queue and statistics
        # Set queue max size to be 2x max_artists to allow some buffer but prevent unbounded growth
        queue_max_size = self.config.max_artists * 2
        self.queue = TraversalQueue(max_size=queue_max_size)
        self.statistics = TraversalStatistics()
        
        logger.info(f"Initialized TraversalManager with config: max_artists={self.config.max_artists}, "
                   f"queue_max_size={queue_max_size}")
    
    def traverse(self, starting_artist_id: str) -> TraversalStatistics:
        """
        Start traversal from a given artist.
        
        Args:
            starting_artist_id: Discogs ID of the starting artist
            
        Returns:
            Traversal statistics
        """
        logger.info("=" * 80)
        logger.info(f"Starting traversal from artist ID: {starting_artist_id}")
        logger.info(f"Configuration: max_artists={self.config.max_artists}")
        logger.info("=" * 80)
        
        # Check if we should process the initial artist
        if self.statistics.artists_processed >= self.config.max_artists:
            logger.warning(f"Already at max_artists limit ({self.config.max_artists})")
            return self.statistics
        
        # Process initial artist
        related_artists = self.single_traverser.process_artist(starting_artist_id)
        self.queue.mark_checked(starting_artist_id)
        self.statistics.artists_processed += 1
        
        # Only add new artists if we haven't reached the limit
        if self.statistics.artists_processed < self.config.max_artists:
            # Calculate how many more we can process
            remaining_capacity = self.config.max_artists - self.statistics.artists_processed
            
            # Add only as many artists as we have capacity for
            artists_to_add = set(list(related_artists)[:remaining_capacity])
            added = self.queue.add_multiple(artists_to_add)
            
            logger.info(f"Initial artist processed. Found {len(related_artists)} related artists, "
                       f"added {added} to queue (capacity: {remaining_capacity})")
        else:
            logger.info(f"Initial artist processed. Reached max_artists limit, not adding related artists")
        
        # Main traversal loop
        self._traverse_loop()
        
        # Finalize statistics
        self.statistics.end_time = time.time()
        self.statistics.artists_checked = self.queue.total_checked
        self.statistics.api_requests = self.api_client.request_count
        
        # Log final summary
        self._log_summary()
        
        return self.statistics
    
    def _traverse_loop(self):
        """Main traversal loop."""
        logger.info(f"Starting traversal loop with {self.queue.size} artists to process")
        
        while self.queue.size > 0 and self.statistics.artists_processed < self.config.max_artists:
            # Get next artist
            artist_id = self.queue.get_next()
            if not artist_id:
                break
            
            self.statistics.artists_processed += 1
            
            # Log progress
            if self.statistics.artists_processed % self.config.log_progress_interval == 0:
                self._log_progress()
            
            try:
                # Process artist
                related_artists = self.single_traverser.process_artist(artist_id)
                
                # Only add new artists if we haven't reached the limit
                if self.statistics.artists_processed < self.config.max_artists:
                    # Calculate how many more we can process
                    remaining_capacity = self.config.max_artists - self.statistics.artists_processed
                    
                    # Only add as many artists as we have capacity for
                    if remaining_capacity > 0:
                        # Convert to list to slice, then back to set
                        artists_to_add = set(list(related_artists)[:remaining_capacity])
                        added = self.queue.add_multiple(artists_to_add)
                        
                        logger.info(f"Processed artist {artist_id}. Found {len(related_artists)} related artists, "
                                   f"added {added} to queue (remaining capacity: {remaining_capacity})")
                    else:
                        logger.info(f"Processed artist {artist_id}. At capacity, not adding {len(related_artists)} related artists")
                else:
                    logger.info(f"Processed artist {artist_id}. Reached max_artists limit, not adding related artists")
                
            except Exception as e:
                self.statistics.errors += 1
                logger.error(f"Error processing artist {artist_id}: {e}")
                continue
        
        # Log reason for stopping
        if self.statistics.artists_processed >= self.config.max_artists:
            logger.info(f"Traversal stopped: reached maximum of {self.config.max_artists} artists")
        elif self.queue.size == 0:
            logger.info("Traversal complete: no more artists to process")
    
    def _log_progress(self):
        """Log current progress."""
        progress_pct = (self.statistics.artists_processed / self.config.max_artists) * 100
        
        logger.info(
            f"\n--- Progress Update ---\n"
            f"Artists processed: {self.statistics.artists_processed}/{self.config.max_artists} "
            f"({progress_pct:.1f}%)\n"
            f"Queue size: {self.queue.size}\n"
            f"Total checked: {self.queue.total_checked}\n"
            f"API requests: {self.api_client.request_count}\n"
            f"Elapsed time: {self.statistics.elapsed_time:.1f}s\n"
            f"Average time per artist: {self.statistics.average_time_per_artist:.1f}s\n"
            f"----------------------"
        )
    
    def _log_summary(self):
        """Log final summary."""
        logger.info("=" * 80)
        logger.info("TRAVERSAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total artists processed: {self.statistics.artists_processed}")
        logger.info(f"Total artists checked: {self.statistics.artists_checked}")
        logger.info(f"Remaining in queue: {self.queue.size}")
        logger.info(f"Total API requests: {self.statistics.api_requests}")
        logger.info(f"Total errors: {self.statistics.errors}")
        logger.info(f"Total time: {self.statistics.elapsed_time:.2f} seconds")
        logger.info(f"Average time per artist: {self.statistics.average_time_per_artist:.2f} seconds")
        
        # Get component statistics
        artist_stats = self.artist_processor.get_statistics()
        release_stats = self.release_processor.get_statistics()
        api_stats = self.api_client.get_statistics()
        
        logger.info("\nArtist Processing Statistics:")
        logger.info(f"  - Artists created: {artist_stats['created_count']}")
        logger.info(f"  - Artists existing: {artist_stats['existing_count']}")
        logger.info(f"  - Creation rate: {artist_stats['creation_rate']:.1%}")
        
        logger.info("\nRelease Processing Statistics:")
        logger.info(f"  - Releases processed: {release_stats['processed_count']}")
        logger.info(f"  - Release errors: {release_stats['error_count']}")
        
        logger.info("\nAPI Statistics:")
        logger.info(f"  - Total requests: {api_stats['request_count']}")
        logger.info(f"  - API errors: {api_stats['error_count']}")
        logger.info(f"  - Error rate: {api_stats['error_rate']:.1%}")
        
        logger.info("=" * 80)


@contextmanager
def progress_tracker(description: str):
    """
    Context manager for tracking progress of operations.
    
    Args:
        description: Description of the operation
    """
    logger.info(f"Starting: {description}")
    start_time = time.time()
    
    try:
        yield
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Completed: {description} (took {elapsed:.2f} seconds)")
