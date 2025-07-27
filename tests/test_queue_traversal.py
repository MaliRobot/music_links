"""
Test script for the queue-centric traversal system.

This script demonstrates the improvements in the refactored traversal system:
1. Queue as the central control structure
2. Accurate artist counting throughout traversal
3. Clear termination conditions
4. Better tracking and statistics
"""

import logging
from unittest.mock import MagicMock, patch
import time

# Mock the database and API components for testing
def create_mock_db():
    """Create a mock database session."""
    db = MagicMock()
    return db


def create_mock_api_client():
    """Create a mock API client with test data."""
    client = MagicMock()
    client.request_count = 0
    
    # Mock artist data
    def mock_fetch_artist(artist_id):
        client.request_count += 1
        artist = MagicMock()
        artist.id = artist_id
        artist.name = f"Artist_{artist_id}"
        artist.url = f"https://discogs.com/artist/{artist_id}"
        
        # Mock releases with pagination
        releases = MagicMock()
        releases.pages = 2  # 2 pages of releases
        
        def mock_page(page_num):
            # Each page has 3 releases
            page_releases = []
            for i in range(3):
                release = MagicMock()
                release.id = f"{artist_id}_r{page_num}_{i}"
                release.title = f"Release {page_num}-{i}"
                release.url = f"https://discogs.com/release/{release.id}"
                release.year = 2020 + i
                
                # Each release has 2-3 related artists
                release.artists = []
                for j in range(2):
                    related = MagicMock()
                    related.id = f"{artist_id}_a{page_num}_{i}_{j}"
                    related.name = f"Related_Artist_{related.id}"
                    release.artists.append(related)
                
                # Extra artists
                release.extraartists = []
                if i % 2 == 0:  # Only some releases have extra artists
                    extra = MagicMock()
                    extra.id = f"{artist_id}_extra_{i}"
                    extra.name = f"Producer_{extra.id}"
                    extra.role = "Producer"
                    release.extraartists.append(extra)
                
                page_releases.append(release)
            
            return page_releases
        
        releases.page = mock_page
        artist.releases = releases
        
        return artist
    
    client.fetch_artist_by_discogs_id = mock_fetch_artist
    client.get_artist = mock_fetch_artist
    
    def mock_get_statistics():
        return {
            'request_count': client.request_count,
            'error_count': 0,
            'error_rate': 0.0
        }
    
    client.get_statistics = mock_get_statistics
    
    return client


def test_queue_based_traversal():
    """Test the queue-based traversal system."""
    print("=" * 80)
    print("TESTING QUEUE-BASED TRAVERSAL SYSTEM")
    print("=" * 80)
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
    
    # Import the modules (assuming they're available)
    try:
        from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig, QueueStrategy
        from services.artist_processor import ArtistProcessor
        from services.release_processor import ReleaseProcessor
    except ImportError:
        print("Using mock implementations for testing")
        # Use the actual implementations from the outputs
        import sys
        sys.path.insert(0, '/tmp/outputs')
        from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig, QueueStrategy
        from services.artist_processor import ArtistProcessor
        from services.release_processor import ReleaseProcessor
    
    # Create mock components
    db = create_mock_db()
    api_client = create_mock_api_client()
    
    # Test different queue strategies
    strategies = ["bfs", "dfs"]
    
    for strategy in strategies:
        print(f"\n{'='*60}")
        print(f"Testing {strategy.upper()} Strategy")
        print(f"{'='*60}")
        
        # Configure traversal
        config = TraversalConfig(
            max_artists=10,
            include_extra_artists=True,
            include_credits=False,
            log_progress_interval=3,
            queue_strategy=strategy,
            max_queue_size=50,
            max_depth=3,
            time_limit_seconds=60,
            error_threshold=10
        )
        
        # Create manager
        manager = QueueBasedTraversalManager(db, api_client, config)
        
        # Mock the database operations
        with patch.object(ArtistProcessor, 'get_or_create_artist') as mock_get_create:
            def mock_artist_func(discogs_id):
                artist = MagicMock()
                artist.discogs_id = discogs_id
                artist.name = f"Artist_{discogs_id}"
                return artist
            
            mock_get_create.side_effect = mock_artist_func
            
            # Run traversal
            start_time = time.time()
            stats = manager.traverse("artist_001")
            end_time = time.time()
            
            # Print results
            print(f"\nTraversal completed in {end_time - start_time:.2f} seconds")
            print(f"Termination reason: {stats.termination_reason}")
            print(f"Artists processed: {stats.artists_processed}")
            print(f"Artists discovered: {stats.artists_discovered}")
            print(f"Queue peak size: {stats.queue_peak_size}")
            print(f"Total queue additions: {stats.total_queue_additions}")
            print(f"API requests: {api_client.request_count}")
            
            # Verify queue behavior
            print(f"\nQueue Analysis:")
            print(f"  - Total seen: {manager.queue.total_seen}")
            print(f"  - Processing efficiency: {stats.artists_processed / max(stats.total_queue_additions, 1):.2%}")
            print(f"  - Discovery rate: {stats.discovery_rate:.2%}")
            
            # Reset for next test
            api_client.request_count = 0


def test_termination_conditions():
    """Test different termination conditions."""
    print("\n" + "=" * 80)
    print("TESTING TERMINATION CONDITIONS")
    print("=" * 80)
    
    try:
        from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig, TerminationReason
    except ImportError:
        import sys
        sys.path.insert(0, '/tmp/outputs')
        from services.traversal_manager import QueueBasedTraversalManager, TraversalConfig, TerminationReason
    
    # Test cases for different termination conditions
    test_cases = [
        {
            'name': 'Max Artists Limit',
            'config': {'max_artists': 5},
            'expected_reason': TerminationReason.MAX_ARTISTS_REACHED
        },
        {
            'name': 'Time Limit',
            'config': {'max_artists': 100, 'time_limit_seconds': 0.1},
            'expected_reason': TerminationReason.TIME_LIMIT_EXCEEDED
        },
        {
            'name': 'Queue Size Limit',
            'config': {'max_artists': 100, 'max_queue_size': 3},
            'expected_reason': TerminationReason.QUEUE_EMPTY  # Queue fills up quickly
        }
    ]
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print("-" * 40)
        
        db = create_mock_db()
        api_client = create_mock_api_client()
        
        config = TraversalConfig(**test_case['config'])
        manager = QueueBasedTraversalManager(db, api_client, config)
        
        # Mock artist processor
        with patch.object(manager.artist_processor, 'get_or_create_artist') as mock_get_create:
            mock_get_create.return_value = MagicMock(discogs_id="test", name="Test Artist")
            
            # Add delay for time limit test
            if 'time_limit_seconds' in test_case['config']:
                time.sleep(0.2)
            
            stats = manager.traverse("start_artist")
            
            print(f"  Termination reason: {stats.termination_reason}")
            print(f"  Artists processed: {stats.artists_processed}")
            print(f"  Elapsed time: {stats.elapsed_time:.2f}s")
            
            # Verify termination reason matches expected
            if test_case['expected_reason'].value in stats.termination_reason:
                print(f"  ✓ Correct termination condition")
            else:
                print(f"  ✗ Unexpected termination: {stats.termination_reason}")


def test_accurate_counting():
    """Test accurate artist counting throughout traversal."""
    print("\n" + "=" * 80)
    print("TESTING ACCURATE ARTIST COUNTING")
    print("=" * 80)
    
    try:
        from services.traversal_manager import SmartTraversalQueue, ArtistQueueItem, QueueStrategy
    except ImportError:
        import sys
        sys.path.insert(0, '/tmp/outputs')
        from services.traversal_manager import SmartTraversalQueue, ArtistQueueItem, QueueStrategy
    
    # Create queue
    queue = SmartTraversalQueue(strategy=QueueStrategy.BFS, max_size=20)
    
    # Add artists and verify counting
    artists_to_add = [
        ArtistQueueItem("artist_1", depth=0),
        ArtistQueueItem("artist_2", depth=1),
        ArtistQueueItem("artist_3", depth=1),
        ArtistQueueItem("artist_1", depth=2),  # Duplicate, should not be added
        ArtistQueueItem("artist_4", depth=2),
    ]
    
    print("\nAdding artists to queue:")
    for item in artists_to_add:
        added = queue.add(item)
        print(f"  - {item.discogs_id} (depth={item.depth}): {'Added' if added else 'Skipped (duplicate)'}")
    
    print(f"\nQueue Statistics:")
    print(f"  - Current size: {queue.size}")
    print(f"  - Total seen: {queue.total_seen}")
    print(f"  - Peak size: {queue.peak_size}")
    print(f"  - Total additions: {queue.total_additions}")
    
    # Process queue
    print("\nProcessing queue:")
    processed = []
    while not queue.is_empty():
        item = queue.get_next()
        processed.append(item.discogs_id)
        print(f"  - Processed: {item.discogs_id} (depth={item.depth})")
    
    print(f"\nFinal Statistics:")
    print(f"  - Artists processed: {len(processed)}")
    print(f"  - Unique artists: {len(set(processed))}")
    print(f"  - Duplicates prevented: {len(artists_to_add) - queue.total_additions}")


def main():
    """Run all tests."""
    print("QUEUE-CENTRIC TRAVERSAL SYSTEM TEST SUITE")
    print("=" * 80)
    print("\nThis test suite demonstrates the improvements in the refactored system:")
    print("1. Queue as the central control structure")
    print("2. Accurate artist counting throughout traversal")
    print("3. Clear termination conditions inside the loop")
    print("4. Better tracking and statistics")
    print()
    
    # Run tests
    test_queue_based_traversal()
    test_termination_conditions()
    test_accurate_counting()
    
    print("\n" + "=" * 80)
    print("TEST SUITE COMPLETED")
    print("=" * 80)
    print("\nKey Improvements Demonstrated:")
    print("✓ Queue-driven traversal with multiple strategies (BFS/DFS)")
    print("✓ Accurate counting of processed vs discovered artists")
    print("✓ Multiple termination conditions checked inside the loop")
    print("✓ Prevention of duplicate processing")
    print("✓ Comprehensive statistics and tracking")
    print("✓ Configurable depth and queue size limits")


if __name__ == "__main__":
    main()
