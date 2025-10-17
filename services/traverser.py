"""
Refactored traverser module with queue-centric design.

This module emphasizes the queue as the central structure for traversal,
with proper termination conditions and accurate artist counting.
"""

import logging
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from services.disco_conn import init_disco_fetcher
from services.rate_limiter import RateLimiter
from services.api_client import DiscogsAPIClient, RetryConfig
from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig, TraversalStatistics

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


def start_queue_based_traversal(
    discogs_id: str,
    db: Session,
    max_artists: int = 20,
    include_extra_artists: bool = True,
    include_credits: bool = True,
    log_level: str = "INFO",
    requests_per_minute: int = 60,
    queue_strategy: str = "bfs"  # "bfs" for breadth-first, "dfs" for depth-first
) -> TraversalStatistics:
    """
    Start the queue-based traversal process.
    
    This is the main entry point for the queue-centric traversal system.
    The queue is the central structure that controls the entire traversal process.
    
    Args:
        discogs_id: Discogs ID of the starting artist
        db: Database session
        max_artists: Maximum number of artists to process
        include_extra_artists: Whether to include extra artists (producers, remixers, etc.)
        include_credits: Whether to include credits
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        requests_per_minute: API rate limit
        queue_strategy: Queue strategy ("bfs" or "dfs")
        
    Returns:
        TraversalStatistics object with traversal results
        
    Example:
        >>> from db.session import SessionLocal
        >>> db = SessionLocal()
        >>> stats = start_queue_based_traversal("12345", db, max_artists=50)
        >>> print(f"Processed {stats.artists_processed} artists")
    """
    # Configure logging
    configure_logging(level=log_level)
    
    logger.info("=" * 80)
    logger.info("STARTING QUEUE-BASED TRAVERSAL SYSTEM")
    logger.info(f"Strategy: {queue_strategy.upper()}")
    logger.info("=" * 80)
    
    # Create API client
    api_client = create_api_client(requests_per_minute=requests_per_minute)
    
    # Configure traversal
    config = TraversalConfig(
        max_artists=max_artists,
        include_extra_artists=include_extra_artists,
        include_credits=include_credits,
        queue_strategy=queue_strategy
    )
    
    # Create queue-based traversal manager
    manager = QueueBasedTraversalManager(
        db=db,
        api_client=api_client,
        config=config
    )
    
    # Start traversal
    logger.info(f"Starting traversal from artist ID: {discogs_id}")
    statistics = manager.traverse(discogs_id)
    
    # Print detailed statistics
    print_detailed_statistics(statistics)
    
    return statistics


def print_detailed_statistics(stats: TraversalStatistics):
    """
    Print detailed traversal statistics.
    
    Args:
        stats: TraversalStatistics object
    """
    print("\n" + "=" * 60)
    print("TRAVERSAL STATISTICS")
    print("=" * 60)
    print(f"Artists processed: {stats.artists_processed}")
    print(f"Artists discovered: {stats.artists_discovered}")
    print(f"Artists skipped (already in DB): {stats.artists_skipped}")
    print(f"Queue peak size: {stats.queue_peak_size}")
    print(f"Total queue additions: {stats.total_queue_additions}")
    print(f"Releases processed: {stats.releases_processed}")
    print(f"API requests: {stats.api_requests}")
    print(f"Errors: {stats.errors}")
    print(f"Total time: {stats.elapsed_time:.2f} seconds")
    
    if stats.artists_processed > 0:
        print(f"Average time per artist: {stats.average_time_per_artist:.2f} seconds")
        print(f"Discovery rate: {stats.discovery_rate:.1%}")
    
    print("\nTermination reason: " + stats.termination_reason)
    print("=" * 60)


def analyze_traversal_efficiency(stats: TraversalStatistics) -> Dict[str, Any]:
    """
    Analyze traversal efficiency and provide recommendations.
    
    Args:
        stats: TraversalStatistics object
        
    Returns:
        Dictionary with analysis results
    """
    analysis = {
        "efficiency_score": 0.0,
        "recommendations": [],
        "metrics": {}
    }
    
    # Calculate efficiency metrics
    if stats.artists_processed > 0:
        # Discovery efficiency
        discovery_rate = stats.artists_discovered / stats.artists_processed
        analysis["metrics"]["discovery_rate"] = discovery_rate
        
        # Queue efficiency
        queue_efficiency = stats.artists_processed / max(stats.total_queue_additions, 1)
        analysis["metrics"]["queue_efficiency"] = queue_efficiency
        
        # API efficiency
        api_efficiency = stats.artists_processed / max(stats.api_requests, 1)
        analysis["metrics"]["api_efficiency"] = api_efficiency
        
        # Error rate
        error_rate = stats.errors / max(stats.api_requests, 1)
        analysis["metrics"]["error_rate"] = error_rate
        
        # Calculate overall efficiency score (0-100)
        efficiency_score = (
            (discovery_rate * 30) +  # 30% weight for discovery
            (queue_efficiency * 30) +  # 30% weight for queue usage
            (api_efficiency * 30) +  # 30% weight for API usage
            ((1 - error_rate) * 10)  # 10% weight for error-free operation
        )
        analysis["efficiency_score"] = min(100, efficiency_score * 100)
        
        # Generate recommendations
        if discovery_rate < 0.5:
            analysis["recommendations"].append(
                "Low discovery rate. Consider starting from a less popular artist or enabling more artist types."
            )
        
        if queue_efficiency < 0.5:
            analysis["recommendations"].append(
                "Low queue efficiency. Many discovered artists are duplicates. Consider adjusting traversal strategy."
            )
        
        if api_efficiency < 0.1:
            analysis["recommendations"].append(
                "Low API efficiency. Consider caching or batch processing to reduce API calls."
            )
        
        if error_rate > 0.1:
            analysis["recommendations"].append(
                "High error rate. Check API rate limits and network connectivity."
            )
    
    return analysis


# Backward compatibility classes
class Traverser:
    """
    Backward compatibility wrapper for Traverser.
    
    This class maintains the same interface as the original implementation
    but delegates to the new queue-based architecture.
    """
    
    def __init__(
        self,
        discogs_id: str,
        db: Session,
        checked=None,
        client=None,
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
        from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig
        
        config = TraversalConfig(max_artists=self.max_artists)
        manager = QueueBasedTraversalManager(self.db, self.client, config)
        
        stats = manager.traverse(self.discogs_id)
        
        # Update internal state for compatibility
        self.count = stats.artists_processed
        self.checked = set(stats.processed_artist_ids)
        
        return stats


# Main execution example
if __name__ == "__main__":
    from db.session import SessionLocal
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Run traversal with queue-based architecture
        stats = start_queue_based_traversal(
            discogs_id="88637",  # Example artist ID
            db=db,
            max_artists=50,
            include_extra_artists=True,
            include_credits=True,
            log_level="INFO",
            requests_per_minute=60,
            queue_strategy="bfs"  # Use breadth-first search
        )
        
        # Analyze efficiency
        analysis = analyze_traversal_efficiency(stats)
        print("\n" + "=" * 60)
        print("EFFICIENCY ANALYSIS")
        print("=" * 60)
        print(f"Efficiency Score: {analysis['efficiency_score']:.1f}/100")
        print("\nMetrics:")
        for metric, value in analysis['metrics'].items():
            print(f"  - {metric}: {value:.2%}")
        
        if analysis['recommendations']:
            print("\nRecommendations:")
            for rec in analysis['recommendations']:
                print(f"  • {rec}")
        
    finally:
        db.close()
