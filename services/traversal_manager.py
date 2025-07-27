"""
Queue-centric traversal manager module.

This module emphasizes the queue as the central control structure for traversal,
with clear termination conditions and accurate artist counting throughout.
"""

import logging
import time
from typing import Set, Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

from sqlalchemy.orm import Session

from services.api_client import DiscogsAPIClient
from services.artist_processor import ArtistProcessor
from services.release_processor import ReleaseProcessor

logger = logging.getLogger(__name__)


class QueueStrategy(Enum):
    """Queue traversal strategy."""
    BFS = "bfs"  # Breadth-first search (FIFO)
    DFS = "dfs"  # Depth-first search (LIFO)
    PRIORITY = "priority"  # Priority-based (e.g., by popularity)


class TerminationReason(Enum):
    """Reasons for traversal termination."""
    MAX_ARTISTS_REACHED = "Maximum number of artists reached"
    QUEUE_EMPTY = "Queue is empty - no more artists to process"
    TIME_LIMIT_EXCEEDED = "Time limit exceeded"
    ERROR_THRESHOLD_EXCEEDED = "Error threshold exceeded"
    MANUAL_STOP = "Manual stop requested"


@dataclass
class ArtistQueueItem:
    """Item in the artist processing queue."""
    discogs_id: str
    depth: int = 0  # Distance from starting artist
    priority: float = 0.0  # For priority-based traversal
    parent_id: Optional[str] = None  # ID of the artist that led to this one
    discovery_time: float = field(default_factory=time.time)


@dataclass
class TraversalStatistics:
    """Enhanced container for traversal statistics."""
    artists_processed: int = 0
    artists_discovered: int = 0  # Total unique artists found
    artists_skipped: int = 0  # Artists already in DB
    releases_processed: int = 0
    api_requests: int = 0
    errors: int = 0
    queue_peak_size: int = 0  # Maximum queue size during traversal
    total_queue_additions: int = 0  # Total items added to queue
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    termination_reason: str = ""
    processed_artist_ids: List[str] = field(default_factory=list)

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
    
    @property
    def discovery_rate(self) -> float:
        """Get the rate of new artist discovery."""
        if self.artists_processed == 0:
            return 0.0
        return self.artists_discovered / self.artists_processed

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to dictionary."""
        return {
            'artists_processed': self.artists_processed,
            'artists_discovered': self.artists_discovered,
            'artists_skipped': self.artists_skipped,
            'releases_processed': self.releases_processed,
            'api_requests': self.api_requests,
            'errors': self.errors,
            'queue_peak_size': self.queue_peak_size,
            'total_queue_additions': self.total_queue_additions,
            'elapsed_time': self.elapsed_time,
            'average_time_per_artist': self.average_time_per_artist,
            'discovery_rate': self.discovery_rate,
            'termination_reason': self.termination_reason
        }


@dataclass
class TraversalConfig:
    """Enhanced configuration for traversal process."""
    max_artists: int = 100
    include_extra_artists: bool = True
    include_credits: bool = True
    log_progress_interval: int = 10
    queue_strategy: str = "bfs"
    max_queue_size: Optional[int] = None  # Limit queue size to prevent memory issues
    max_depth: Optional[int] = None  # Limit traversal depth
    time_limit_seconds: Optional[float] = None  # Time limit for traversal
    error_threshold: Optional[int] = None  # Stop if too many errors
    batch_size: int = 5  # Number of artists to process before checking conditions


class SmartTraversalQueue:
    """
    Enhanced queue that manages artist traversal with multiple strategies.
    
    This queue is the central control structure for the traversal process.
    """

    def __init__(self, strategy: QueueStrategy = QueueStrategy.BFS, max_size: Optional[int] = None):
        """
        Initialize the smart queue.

        Args:
            strategy: Queue traversal strategy
            max_size: Maximum queue size (None for unlimited)
        """
        self.strategy = strategy
        self.max_size = max_size
        
        # Different storage based on strategy
        if strategy == QueueStrategy.BFS:
            self._queue = deque()
        elif strategy == QueueStrategy.DFS:
            self._queue = []  # Use list as stack
        else:  # Priority queue
            import heapq
            self._queue = []
        
        # Track all seen artists to avoid duplicates
        self.seen: Set[str] = set()
        self.processed: Set[str] = set()
        
        # Statistics
        self.peak_size = 0
        self.total_additions = 0
        
        # Depth tracking
        self.depth_map: Dict[str, int] = {}

    def add(self, item: ArtistQueueItem) -> bool:
        """
        Add an artist to the queue if not seen before.
        
        Args:
            item: Artist queue item to add
            
        Returns:
            True if added, False if duplicate or queue full
        """
        # Check if already seen
        if item.discogs_id in self.seen:
            logger.debug(f"Artist {item.discogs_id} already in queue or processed")
            return False
        
        # Check queue size limit
        if self.max_size and len(self._queue) >= self.max_size:
            logger.warning(f"Queue size limit ({self.max_size}) reached")
            return False
        print(len(self._queue))
        # Add based on strategy
        if self.strategy == QueueStrategy.BFS or self.strategy == QueueStrategy.DFS:
            self._queue.append(item)
        else:  # Priority queue
            import heapq
            # Use negative priority for max-heap behavior
            heapq.heappush(self._queue, (-item.priority, item.discovery_time, item))
        
        # Update tracking
        self.seen.add(item.discogs_id)
        self.depth_map[item.discogs_id] = item.depth
        self.total_additions += 1
        
        # Update peak size
        current_size = len(self._queue)
        if current_size > self.peak_size:
            self.peak_size = current_size
        
        logger.debug(f"Added artist {item.discogs_id} to queue (depth={item.depth}, queue_size={current_size})")
        return True

    def add_multiple(self, items: List[ArtistQueueItem], limit: Optional[int] = None) -> int:
        """
        Add multiple artists to the queue.
        
        Args:
            items: List of artist queue items
            limit: Maximum number to add (None for all)
            
        Returns:
            Number of items actually added
        """
        added = 0
        for item in items:
            if limit and added >= limit:
                break
            if self.add(item):
                added += 1
        
        logger.info(f"Added {added} of {len(items)} artists to queue")
        return added

    def get_next(self) -> Optional[ArtistQueueItem]:
        """
        Get next artist to process based on strategy.
        
        Returns:
            Next artist item or None if queue is empty
        """
        if not self._queue:
            return None
        
        if self.strategy == QueueStrategy.BFS:
            item = self._queue.popleft()
        elif self.strategy == QueueStrategy.DFS:
            item = self._queue.pop()
        else:  # Priority queue
            import heapq
            _, _, item = heapq.heappop(self._queue)
        
        self.processed.add(item.discogs_id)
        return item

    def should_terminate(self, config: TraversalConfig, stats: TraversalStatistics) -> Tuple[bool, TerminationReason]:
        """
        Check if traversal should terminate based on current state.
        
        Args:
            config: Traversal configuration
            stats: Current statistics
            
        Returns:
            Tuple of (should_terminate, reason)
        """
        # Check max artists limit
        if stats.artists_processed >= config.max_artists:
            return True, TerminationReason.MAX_ARTISTS_REACHED
        
        # Check if queue is empty
        if self.is_empty():
            return True, TerminationReason.QUEUE_EMPTY
        
        # Check time limit
        if config.time_limit_seconds and stats.elapsed_time > config.time_limit_seconds:
            return True, TerminationReason.TIME_LIMIT_EXCEEDED
        
        # Check error threshold
        if config.error_threshold and stats.errors >= config.error_threshold:
            return True, TerminationReason.ERROR_THRESHOLD_EXCEEDED
        
        return False, None

    def can_accept_more(self, config: TraversalConfig, stats: TraversalStatistics) -> int:
        """
        Calculate how many more artists can be accepted.
        
        Args:
            config: Traversal configuration
            stats: Current statistics
            
        Returns:
            Number of artists that can still be added
        """
        # Calculate remaining capacity based on max_artists
        remaining_for_processing = config.max_artists - stats.artists_processed - len(self._queue)
        
        # Consider queue size limit
        remaining_for_queue = float('inf')
        if self.max_size:
            remaining_for_queue = self.max_size - len(self._queue)
        
        return max(0, min(remaining_for_processing, remaining_for_queue))

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0

    @property
    def size(self) -> int:
        """Get current queue size."""
        return len(self._queue)

    @property
    def total_seen(self) -> int:
        """Get total number of unique artists seen."""
        return len(self.seen)


class QueueBasedTraversalManager:
    """
    Traversal manager with queue as the central control structure.
    
    The queue drives the entire traversal process with clear termination conditions
    and accurate artist counting throughout the traversal.
    """

    def __init__(
        self,
        db: Session,
        api_client: DiscogsAPIClient,
        config: Optional[TraversalConfig] = None
    ):
        """
        Initialize the queue-based traversal manager.

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

        # Initialize smart queue
        strategy = QueueStrategy(self.config.queue_strategy)
        self.queue = SmartTraversalQueue(
            strategy=strategy,
            max_size=self.config.max_queue_size
        )
        
        # Initialize statistics
        self.statistics = TraversalStatistics()
        
        # Control flags
        self.stop_requested = False

        logger.info(
            f"Initialized QueueBasedTraversalManager:\n"
            f"  - Strategy: {strategy.value}\n"
            f"  - Max artists: {self.config.max_artists}\n"
            f"  - Max queue size: {self.config.max_queue_size or 'unlimited'}\n"
            f"  - Max depth: {self.config.max_depth or 'unlimited'}"
        )

    def traverse(self, starting_artist_id: str) -> TraversalStatistics:
        """
        Start traversal from a given artist using queue-based control.
        
        The queue is populated as we traverse and controls the entire process.
        Termination conditions are checked inside the main loop.

        Args:
            starting_artist_id: Discogs ID of the starting artist

        Returns:
            Traversal statistics
        """
        logger.info("=" * 80)
        logger.info(f"Starting queue-based traversal from artist ID: {starting_artist_id}")
        logger.info(f"Max artists to process: {self.config.max_artists}")
        logger.info("=" * 80)

        # Add starting artist to queue
        start_item = ArtistQueueItem(
            discogs_id=starting_artist_id,
            depth=0,
            priority=1.0  # Highest priority for starting artist
        )
        self.queue.add(start_item)

        # Main traversal loop - queue-driven with internal termination checks
        while True:
            # Check termination conditions
            should_stop, reason = self.queue.should_terminate(self.config, self.statistics)
            if should_stop:
                self.statistics.termination_reason = reason.value
                logger.info(f"Traversal terminated: {reason.value}")
                break

            # Check for manual stop
            if self.stop_requested:
                self.statistics.termination_reason = TerminationReason.MANUAL_STOP.value
                logger.info("Traversal stopped manually")
                break

            # Get next artist from queue
            queue_item = self.queue.get_next()
            if not queue_item:  # Should not happen due to termination check, but safety check
                self.statistics.termination_reason = TerminationReason.QUEUE_EMPTY.value
                break

            # Process artist
            try:
                self._process_artist_from_queue(queue_item)
            except Exception as e:
                self.statistics.errors += 1
                logger.error(f"Error processing artist {queue_item.discogs_id}: {e}")
                
                # Check if we've hit error threshold
                if self.config.error_threshold and self.statistics.errors >= self.config.error_threshold:
                    self.statistics.termination_reason = TerminationReason.ERROR_THRESHOLD_EXCEEDED.value
                    logger.error("Error threshold exceeded, terminating traversal")
                    break

            # Log progress at intervals
            if self.statistics.artists_processed % self.config.log_progress_interval == 0:
                self._log_progress()

        # Finalize statistics
        self.statistics.end_time = time.time()
        self.statistics.queue_peak_size = self.queue.peak_size
        self.statistics.total_queue_additions = self.queue.total_additions
        self.statistics.api_requests = self.api_client.request_count

        # Log final summary
        self._log_final_summary()

        return self.statistics

    def _process_artist_from_queue(self, queue_item: ArtistQueueItem):
        """
        Process a single artist from the queue.
        
        This method:
        1. Fetches/creates the artist
        2. Processes their releases
        3. Discovers related artists
        4. Adds discovered artists to the queue (with capacity checks)
        
        Args:
            queue_item: Artist queue item to process
        """
        artist_id = queue_item.discogs_id
        depth = queue_item.depth
        
        logger.info(f"Processing artist {artist_id} (depth={depth}, queue_size={self.queue.size})")

        # Get or create artist
        artist = self.artist_processor.get_or_create_artist(artist_id)
        if not artist:
            logger.warning(f"Could not process artist {artist_id}")
            return

        self.statistics.artists_processed += 1
        self.statistics.processed_artist_ids.append(artist_id)

        # Fetch releases
        logger.info(f"Fetching releases for {artist.name}")
        try:
            releases = self.artist_processor.fetch_artist_releases(artist.discogs_id)
        except Exception as e:
            logger.error(f"Error fetching releases for {artist.name}: {e}")
            return

        if not releases:
            logger.info(f"No releases found for {artist.name}")
            return

        # Calculate how many new artists we can still accept
        capacity = self.queue.can_accept_more(self.config, self.statistics)
        logger.debug(f"Queue can accept {capacity} more artists")

        # Process releases and discover artists
        discovered_artists = self._process_releases_with_queue(
            releases,
            artist_id,
            queue_item,
            capacity
        )

        logger.info(
            f"Completed processing {artist.name}: "
            f"discovered {len(discovered_artists)} new artists, "
            f"queue size now {self.queue.size}"
        )

    def _process_releases_with_queue(
        self,
        releases: Any,
        current_artist_id: str,
        parent_item: ArtistQueueItem,
        capacity: int
    ) -> Set[str]:
        """
        Process releases and populate queue with discovered artists.
        
        Args:
            releases: Releases collection from API
            current_artist_id: ID of the current artist
            parent_item: Parent queue item
            capacity: Maximum number of new artists to add to queue
            
        Returns:
            Set of discovered artist IDs
        """
        discovered = set()
        added_to_queue = 0
        releases_processed = 0

        # Check max depth
        if self.config.max_depth and parent_item.depth >= self.config.max_depth:
            logger.info(f"Max depth ({self.config.max_depth}) reached, not exploring further")
            return discovered

        # Handle pagination
        if hasattr(releases, 'pages'):
            total_pages = releases.pages
            logger.info(f"Processing {total_pages} pages of releases")

            for page_num in range(1, total_pages + 1):
                if added_to_queue >= capacity:
                    logger.info(f"Reached capacity ({capacity}), stopping release exploration")
                    break

                try:
                    page = releases.page(page_num)
                    for release in page:
                        if added_to_queue >= capacity:
                            break

                        # Extract artists from release
                        artists = self.release_processor.extract_artists_from_release(
                            release,
                            current_artist_id,
                            include_extras=self.config.include_extra_artists,
                            include_credits=self.config.include_credits
                        )

                        discovered.update(artists)
                        releases_processed += 1

                        # Add to queue with depth tracking
                        for artist_id in artists:
                            if added_to_queue >= capacity:
                                break
                            
                            item = ArtistQueueItem(
                                discogs_id=artist_id,
                                depth=parent_item.depth + 1,
                                priority=0.5,  # Default priority
                                parent_id=current_artist_id
                            )
                            
                            if self.queue.add(item):
                                added_to_queue += 1
                                self.statistics.artists_discovered += 1

                except Exception as e:
                    logger.error(f"Error processing release page {page_num}: {e}")
                    self.statistics.errors += 1
        else:
            # Non-paginated releases
            for release in releases:
                if added_to_queue >= capacity:
                    logger.info(f"Reached capacity ({capacity}), stopping release exploration")
                    break

                # Extract artists from release
                artists = self.release_processor.extract_artists_from_release(
                    release,
                    current_artist_id,
                    include_extras=self.config.include_extra_artists,
                    include_credits=self.config.include_credits
                )

                discovered.update(artists)
                releases_processed += 1

                # Add to queue with depth tracking
                for artist_id in artists:
                    if added_to_queue >= capacity:
                        break
                    
                    item = ArtistQueueItem(
                        discogs_id=artist_id,
                        depth=parent_item.depth + 1,
                        priority=0.5,  # Default priority
                        parent_id=current_artist_id
                    )
                    
                    if self.queue.add(item):
                        added_to_queue += 1
                        self.statistics.artists_discovered += 1

        self.statistics.releases_processed += releases_processed
        logger.info(f"Processed {releases_processed} releases, added {added_to_queue} artists to queue")

        return discovered

    def stop(self):
        """Request traversal to stop."""
        self.stop_requested = True
        logger.info("Stop requested for traversal")

    def _log_progress(self):
        """Log current traversal progress."""
        progress_pct = (self.statistics.artists_processed / self.config.max_artists) * 100

        logger.info(
            f"\n{'='*60}\n"
            f"PROGRESS UPDATE\n"
            f"{'='*60}\n"
            f"Artists processed: {self.statistics.artists_processed}/{self.config.max_artists} "
            f"({progress_pct:.1f}%)\n"
            f"Artists discovered: {self.statistics.artists_discovered}\n"
            f"Queue size: {self.queue.size} (peak: {self.queue.peak_size})\n"
            f"Total seen: {self.queue.total_seen}\n"
            f"Releases processed: {self.statistics.releases_processed}\n"
            f"API requests: {self.api_client.request_count}\n"
            f"Errors: {self.statistics.errors}\n"
            f"Elapsed time: {self.statistics.elapsed_time:.1f}s\n"
            f"Avg time/artist: {self.statistics.average_time_per_artist:.1f}s\n"
            f"Discovery rate: {self.statistics.discovery_rate:.1%}\n"
            f"{'='*60}"
        )
    
    def _log_final_summary(self):
        """Log final traversal summary."""
        logger.info(
            f"\n{'='*80}\n"
            f"TRAVERSAL COMPLETE\n"
            f"{'='*80}\n"
            f"Termination reason: {self.statistics.termination_reason}\n"
            f"Total artists processed: {self.statistics.artists_processed}\n"
            f"Total artists discovered: {self.statistics.artists_discovered}\n"
            f"Total unique artists seen: {self.queue.total_seen}\n"
            f"Queue peak size: {self.queue.peak_size}\n"
            f"Total queue additions: {self.queue.total_additions}\n"
            f"Total releases processed: {self.statistics.releases_processed}\n"
            f"Total API requests: {self.statistics.api_requests}\n"
            f"Total errors: {self.statistics.errors}\n"
            f"Total time: {self.statistics.elapsed_time:.2f} seconds\n"
            f"Average time per artist: {self.statistics.average_time_per_artist:.2f} seconds\n"
            f"Discovery rate: {self.statistics.discovery_rate:.1%}\n"
            f"{'='*80}"
        )

        # Log component statistics if available
        try:
            artist_stats = self.artist_processor.get_statistics()
            release_stats = self.release_processor.get_statistics()
            api_stats = self.api_client.get_statistics()
            
            logger.info("\nComponent Statistics:")
            logger.info(f"\nArtist Processor:")
            for key, value in artist_stats.items():
                logger.info(f"  - {key}: {value}")
            
            logger.info(f"\nRelease Processor:")
            for key, value in release_stats.items():
                logger.info(f"  - {key}: {value}")
            
            logger.info(f"\nAPI Client:")
            for key, value in api_stats.items():
                logger.info(f"  - {key}: {value}")
        except Exception as e:
            logger.debug(f"Could not get component statistics: {e}")


# Maintain backward compatibility
TraversalManager = QueueBasedTraversalManager
