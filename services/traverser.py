"""
Refactored traverser module with improved maintainability and single responsibility principle.

This module serves as the main entry point for the traversal system, coordinating
between all the specialized components.
"""

import logging
from typing import Optional
from sqlalchemy.orm import Session

from services.disco_conn import init_disco_fetcher
from services.rate_limiter import RateLimiter
from services.api_client import DiscogsAPIClient, RetryConfig
from services.traversal_manager import TraversalManager, TraversalConfig, TraversalStatistics

# Configure module logger
logger = logging.getLogger(__name__)


def configure_logging(level: str = "INFO", format_string: Optional[str] = None):
    """
    Configure logging for the traverser module.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        format_string: Custom format string for log messages
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string
    )
    
    logger.info(f"Logging configured with level: {level}")


def create_api_client(
    requests_per_minute: int = 60,
    retry_attempts: int = 3,
    initial_backoff: float = 1.0
) -> DiscogsAPIClient:
    """
    Create and configure the API client.
    
    Args:
        requests_per_minute: API rate limit
        retry_attempts: Number of retry attempts for failed requests
        initial_backoff: Initial backoff time for retries
        
    Returns:
        Configured API client
    """
    logger.info("Creating API client...")
    
    # Initialize base Discogs client
    base_client = init_disco_fetcher()
    
    # Configure rate limiter
    rate_limiter = RateLimiter(requests_per_minute=requests_per_minute)
    
    # Configure retry logic
    retry_config = RetryConfig(
        max_attempts=retry_attempts,
        initial_backoff=initial_backoff
    )
    
    # Create wrapped client
    api_client = DiscogsAPIClient(
        client=base_client,
        rate_limiter=rate_limiter,
        retry_config=retry_config
    )
    
    logger.info(f"API client created with {requests_per_minute} req/min rate limit")
    
    return api_client


def start_traversing(
    discogs_id: str,
    db: Session,
    max_artists: int = 20,
    include_extra_artists: bool = True,
    include_credits: bool = True,
    log_level: str = "INFO",
    requests_per_minute: int = 60
) -> TraversalStatistics:
    """
    Start the traversal process with improved architecture.
    
    This is the main entry point for the refactored traversal system.
    It creates all necessary components and starts the traversal.
    
    Args:
        discogs_id: Discogs ID of the starting artist
        db: Database session
        max_artists: Maximum number of artists to process
        include_extra_artists: Whether to include extra artists (producers, remixers, etc.)
        include_credits: Whether to include credits
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        requests_per_minute: API rate limit
        
    Returns:
        TraversalStatistics object with traversal results
        
    Example:
        >>> from db.session import SessionLocal
        >>> db = SessionLocal()
        >>> stats = start_traversing("12345", db, max_artists=50)
        >>> print(f"Processed {stats.artists_processed} artists")
    """
    # Configure logging
    configure_logging(level=log_level)
    
    logger.info("=" * 80)
    logger.info("STARTING TRAVERSAL SYSTEM")
    logger.info("=" * 80)
    
    # Create API client
    api_client = create_api_client(requests_per_minute=requests_per_minute)
    
    # Configure traversal
    config = TraversalConfig(
        max_artists=max_artists,
        include_extra_artists=include_extra_artists,
        include_credits=include_credits
    )
    
    # Create traversal manager
    manager = TraversalManager(
        db=db,
        api_client=api_client,
        config=config
    )
    
    # Start traversal
    logger.info(f"Starting traversal from artist ID: {discogs_id}")
    statistics = manager.traverse(discogs_id)
    
    return statistics


def print_statistics(stats: TraversalStatistics):
    """
    Print traversal statistics in a formatted way.
    
    Args:
        stats: TraversalStatistics object
    """
    print("\n" + "=" * 60)
    print("TRAVERSAL STATISTICS")
    print("=" * 60)
    print(f"Artists processed: {stats.artists_processed}")
    print(f"Artists checked: {stats.artists_checked}")
    print(f"Releases processed: {stats.releases_processed}")
    print(f"API requests: {stats.api_requests}")
    print(f"Errors: {stats.errors}")
    print(f"Total time: {stats.elapsed_time:.2f} seconds")
    print(f"Average time per artist: {stats.average_time_per_artist:.2f} seconds")
    print("=" * 60)


# Backward compatibility classes for existing code
class StepTraverser:
    """
    Backward compatibility wrapper for StepTraverser.
    
    This class maintains the same interface as the original implementation
    but delegates to the new architecture.
    """
    
    def __init__(self, discogs_id: str, client, db: Session, artists=None):
        from artist_processor import ArtistProcessor
        from release_processor import ReleaseProcessor
        
        self.discogs_id = discogs_id
        self.db = db
        self.artists = artists or set()
        
        # Create processors
        if not isinstance(client, DiscogsAPIClient):
            # Wrap old client
            self.client = DiscogsAPIClient(client)
        else:
            self.client = client
            
        self.artist_processor = ArtistProcessor(db, self.client)
        self.release_processor = ReleaseProcessor(db)
        self.artist = None
    
    def get_or_create_artist(self):
        """Get or create artist (backward compatibility)."""
        self.artist = self.artist_processor.get_or_create_artist(self.discogs_id)
        return self.artist
    
    def get_artist_releases(self):
        """Get artist releases (backward compatibility)."""
        if not self.artist:
            return []
        return self.artist_processor.fetch_artist_releases(self.artist.discogs_id)
    
    def check_artist_releases(self):
        """Check artist releases (backward compatibility)."""
        if not self.artist:
            return self.artists
            
        releases = self.get_artist_releases()
        if not releases:
            return self.artists
        
        # Extract artists from releases
        from traversal_manager import TraversalConfig, SingleArtistTraverser
        
        config = TraversalConfig()
        traverser = SingleArtistTraverser(
            self.artist_processor,
            self.release_processor,
            self.client,
            config
        )
        
        related = traverser._process_releases(releases, self.artist.discogs_id)
        self.artists.update(related)
        
        return self.artists


class Traverser:
    """
    Backward compatibility wrapper for Traverser.
    
    This class maintains the same interface as the original implementation
    but delegates to the new architecture.
    """
    
    def __init__(
        self,
        discogs_id: str,
        client,
        db: Session,
        checked=None,
        count: int = 0,
        max_artists: int = 100,
        artists=None
    ):
        self.discogs_id = discogs_id
        self.db = db
        self.checked = checked or set()
        self.count = count
        self.max_artists = max_artists
        self.artists = artists or set()
        
        # Wrap client if needed
        if not isinstance(client, DiscogsAPIClient):
            self.client = DiscogsAPIClient(client)
        else:
            self.client = client
    
    def begin_traverse(self):
        """Begin traversal (backward compatibility)."""
        from traversal_manager import TraversalManager, TraversalConfig
        
        config = TraversalConfig(max_artists=self.max_artists)
        manager = TraversalManager(self.db, self.client, config)
        
        stats = manager.traverse(self.discogs_id)
        
        # Update internal state for compatibility
        self.count = stats.artists_processed
        self.checked = manager.queue.checked
        
        return stats
    
    def traverse_loop(self):
        """Traverse loop (backward compatibility)."""
        # This is handled by begin_traverse in the new architecture
        pass


class DiscoConnectorWithLogging:
    """
    Backward compatibility wrapper for DiscoConnectorWithLogging.
    
    This class maintains the same interface as the original implementation
    but delegates to the new DiscogsAPIClient.
    """
    
    def __init__(self, client):
        if isinstance(client, DiscogsAPIClient):
            self.client = client
        else:
            self.client = DiscogsAPIClient(client)
        
        self.rate_limiter = self.client.rate_limiter
        self.request_count = 0
    
    def fetch_artist_by_discogs_id(self, discogs_id: str):
        """Fetch artist by Discogs ID (backward compatibility)."""
        result = self.client.fetch_artist_by_discogs_id(discogs_id)
        self.request_count = self.client.request_count
        return result
    
    def get_artist(self, artist_id: str):
        """Get artist (backward compatibility)."""
        result = self.client.get_artist(artist_id)
        self.request_count = self.client.request_count
        return result
    
    def get_release(self, release_id: str):
        """Get release (backward compatibility)."""
        result = self.client.get_release(release_id)
        self.request_count = self.client.request_count
        return result


# Main execution example
if __name__ == "__main__":
    from db.session import SessionLocal
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Start traversal with the new improved architecture
        stats = start_traversing(
            discogs_id="17199",  # Example artist ID
            db=db,
            max_artists=50,
            include_extra_artists=True,
            include_credits=True,
            log_level="DEBUG",  # Use DEBUG for more detailed logging
            requests_per_minute=60
        )
        
        # Print statistics
        print_statistics(stats)
        
    finally:
        db.close()
