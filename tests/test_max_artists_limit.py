"""
Test script to verify that max_artists limit is properly respected.

This script demonstrates that the traversal stops at the configured limit.
"""

import logging
from unittest.mock import Mock, MagicMock
from traversal_manager import TraversalManager, TraversalConfig
from api_client import DiscogsAPIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_mock_artist(artist_id: str, name: str, related_artist_ids: list):
    """Create a mock artist object."""
    artist = Mock()
    artist.id = artist_id
    artist.discogs_id = artist_id
    artist.name = name
    artist.url = f"https://discogs.com/artist/{artist_id}"
    
    # Mock releases with related artists
    releases = []
    for i, related_id in enumerate(related_artist_ids):
        release = Mock()
        release.id = f"release_{artist_id}_{i}"
        release.title = f"Release {i} by {name}"
        release.url = f"https://discogs.com/release/{release.id}"
        release.year = 2020 + i
        
        # Add related artist
        related_artist = Mock()
        related_artist.id = related_id
        related_artist.name = f"Artist {related_id}"
        related_artist.url = f"https://discogs.com/artist/{related_id}"
        
        release.artists = [related_artist]
        release.extraartists = []
        release.credits = []
        
        releases.append(release)
    
    # Make releases non-paginated for simplicity
    artist.releases = releases
    
    return artist


def create_mock_api_client():
    """Create a mock API client that simulates a network of artists."""
    client = Mock(spec=DiscogsAPIClient)
    
    # Create a network of artists where each artist is related to 3 others
    # This should quickly exceed any reasonable max_artists limit
    artist_network = {
        "1": create_mock_artist("1", "Artist 1", ["2", "3", "4"]),
        "2": create_mock_artist("2", "Artist 2", ["1", "5", "6"]),
        "3": create_mock_artist("3", "Artist 3", ["1", "7", "8"]),
        "4": create_mock_artist("4", "Artist 4", ["1", "9", "10"]),
        "5": create_mock_artist("5", "Artist 5", ["2", "11", "12"]),
        "6": create_mock_artist("6", "Artist 6", ["2", "13", "14"]),
        "7": create_mock_artist("7", "Artist 7", ["3", "15", "16"]),
        "8": create_mock_artist("8", "Artist 8", ["3", "17", "18"]),
        "9": create_mock_artist("9", "Artist 9", ["4", "19", "20"]),
        "10": create_mock_artist("10", "Artist 10", ["4", "21", "22"]),
        # Add more artists to ensure we have a large network
        **{str(i): create_mock_artist(str(i), f"Artist {i}", [str((i*3) % 100), str((i*3+1) % 100), str((i*3+2) % 100)]) 
           for i in range(11, 101)}
    }
    
    # Mock the API methods
    client.fetch_artist_by_discogs_id = lambda artist_id: artist_network.get(artist_id)
    client.get_artist = lambda artist_id: artist_network.get(artist_id)
    client.get_release = Mock(return_value=None)
    client.request_count = 0
    client.error_count = 0
    
    # Track calls
    original_fetch = client.fetch_artist_by_discogs_id
    original_get = client.get_artist
    
    def tracked_fetch(artist_id):
        client.request_count += 1
        return original_fetch(artist_id)
    
    def tracked_get(artist_id):
        client.request_count += 1
        return original_get(artist_id)
    
    client.fetch_artist_by_discogs_id = tracked_fetch
    client.get_artist = tracked_get
    
    client.get_statistics = lambda: {
        'request_count': client.request_count,
        'error_count': client.error_count,
        'error_rate': 0.0
    }
    
    return client


def test_max_artists_limit():
    """Test that the traversal respects the max_artists limit."""
    print("\n" + "=" * 80)
    print("TESTING MAX_ARTISTS LIMIT")
    print("=" * 80)
    
    # Test with different limits
    test_limits = [5, 10, 20]
    
    for limit in test_limits:
        print(f"\n--- Testing with max_artists={limit} ---")
        
        # Create mock database session
        db = Mock()
        
        # Create mock API client
        api_client = create_mock_api_client()
        
        # Configure traversal
        config = TraversalConfig(
            max_artists=limit,
            include_extra_artists=False,  # Keep it simple
            include_credits=False
        )
        
        # Create traversal manager
        manager = TraversalManager(
            db=db,
            api_client=api_client,
            config=config
        )
        
        # Mock the database operations
        from unittest.mock import patch
        
        with patch('artist_processor.artist_crud') as mock_crud, \
             patch('release_processor.artist_crud') as mock_release_crud, \
             patch('release_processor.release_crud') as mock_release_crud2:
            
            # Mock database responses
            mock_crud.get_by_discogs_id.return_value = None  # Always create new
            mock_crud.create_with_releases.return_value = Mock(
                name="Created Artist",
                discogs_id="123"
            )
            
            # Start traversal
            stats = manager.traverse("1")
            
            # Verify results
            print(f"  Artists processed: {stats.artists_processed}")
            print(f"  Artists checked: {stats.artists_checked}")
            print(f"  Queue size at end: {manager.queue.size}")
            print(f"  API requests: {api_client.request_count}")
            
            # Assert that we didn't exceed the limit
            assert stats.artists_processed <= limit, \
                f"Processed {stats.artists_processed} artists, but limit was {limit}"
            
            print(f"  ✓ Successfully respected max_artists limit of {limit}")
    
    print("\n" + "=" * 80)
    print("ALL TESTS PASSED!")
    print("=" * 80)


def test_queue_growth():
    """Test that the queue doesn't grow unbounded."""
    print("\n" + "=" * 80)
    print("TESTING QUEUE GROWTH CONTROL")
    print("=" * 80)
    
    # Create mock database and API client
    db = Mock()
    api_client = create_mock_api_client()
    
    # Configure with small limit
    config = TraversalConfig(
        max_artists=10,
        include_extra_artists=False,
        include_credits=False
    )
    
    # Create traversal manager
    manager = TraversalManager(
        db=db,
        api_client=api_client,
        config=config
    )
    
    print(f"Queue max size: {manager.queue.max_size}")
    print(f"Max artists: {config.max_artists}")
    
    # Track queue sizes during traversal
    queue_sizes = []
    
    # Patch to track queue size
    original_process = manager.single_traverser.process_artist
    
    def tracked_process(artist_id):
        result = original_process(artist_id)
        queue_sizes.append(manager.queue.size)
        return result
    
    manager.single_traverser.process_artist = tracked_process
    
    # Mock database operations
    from unittest.mock import patch
    
    with patch('artist_processor.artist_crud') as mock_crud, \
         patch('release_processor.artist_crud') as mock_release_crud:
        
        mock_crud.get_by_discogs_id.return_value = None
        mock_crud.create_with_releases.return_value = Mock(
            name="Created Artist",
            discogs_id="123"
        )
        
        # Start traversal
        stats = manager.traverse("1")
        
        # Analyze results
        print(f"\nQueue size progression: {queue_sizes}")
        print(f"Maximum queue size reached: {max(queue_sizes) if queue_sizes else 0}")
        print(f"Final queue size: {manager.queue.size}")
        print(f"Artists processed: {stats.artists_processed}")
        
        # Verify queue didn't grow too large
        max_queue_size = max(queue_sizes) if queue_sizes else 0
        assert max_queue_size <= manager.queue.max_size, \
            f"Queue grew to {max_queue_size}, exceeding max of {manager.queue.max_size}"
        
        print(f"\n✓ Queue growth properly controlled")
    
    print("=" * 80)


if __name__ == "__main__":
    test_max_artists_limit()
    test_queue_growth()
