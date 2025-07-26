"""
Simple script to visualize how the max_artists limit works in the traversal.
"""

from traversal_manager import TraversalQueue, TraversalConfig

def visualize_queue_behavior():
    """Visualize how the queue behaves with limits."""
    print("\n" + "=" * 70)
    print("QUEUE BEHAVIOR VISUALIZATION")
    print("=" * 70)
    
    # Create a queue with max size of 10
    queue = TraversalQueue(max_size=10)
    
    print(f"\nQueue created with max_size=10")
    print(f"Initial state: pending={len(queue.pending)}, checked={len(queue.checked)}")
    
    # Try to add artists
    print("\n--- Adding artists one by one ---")
    for i in range(15):
        added = queue.add(f"artist_{i}")
        print(f"Adding artist_{i}: {'✓ Added' if added else '✗ Not added'} | "
              f"Queue size: {queue.size}")
    
    print(f"\nQueue is now at capacity: {queue.size}/{queue.max_size}")
    
    # Process some artists
    print("\n--- Processing some artists ---")
    for i in range(5):
        artist = queue.get_next()
        if artist:
            print(f"Processing: {artist} | Queue size: {queue.size} | "
                  f"Checked: {queue.total_checked}")
    
    # Try adding more after processing
    print("\n--- Trying to add more artists after processing ---")
    for i in range(15, 20):
        added = queue.add(f"artist_{i}")
        print(f"Adding artist_{i}: {'✓ Added' if added else '✗ Not added'} | "
              f"Queue size: {queue.size}")


def simulate_traversal_scenario():
    """Simulate a real traversal scenario with max_artists limit."""
    print("\n\n" + "=" * 70)
    print("TRAVERSAL SCENARIO SIMULATION")
    print("=" * 70)
    
    max_artists = 5
    print(f"\nSimulating traversal with max_artists={max_artists}")
    print("Each artist is connected to 3 other artists")
    
    # Simple simulation without actual API calls
    artists_processed = 0
    queue = TraversalQueue(max_size=max_artists * 2)  # Same as TraversalManager
    
    # Start with artist 1
    queue.add("artist_1")
    
    print("\n--- Traversal Progress ---")
    step = 0
    
    while queue.size > 0 and artists_processed < max_artists:
        step += 1
        print(f"\nStep {step}:")
        
        # Get next artist
        current = queue.get_next()
        artists_processed += 1
        
        print(f"  Processing: {current}")
        print(f"  Artists processed: {artists_processed}/{max_artists}")
        
        # Simulate finding 3 related artists
        related = [f"{current}_related_{i}" for i in range(1, 4)]
        print(f"  Found related: {related}")
        
        # Check if we can add more
        remaining_capacity = max_artists - artists_processed
        
        if remaining_capacity > 0:
            # Only add as many as we have capacity for
            to_add = related[:remaining_capacity]
            added = 0
            for artist in to_add:
                if queue.add(artist):
                    added += 1
            
            print(f"  Added to queue: {added} artists (capacity: {remaining_capacity})")
        else:
            print(f"  At capacity - not adding any related artists")
        
        print(f"  Queue size: {queue.size}")
        print(f"  Total checked: {queue.total_checked}")
    
    print("\n--- Final Results ---")
    print(f"Artists processed: {artists_processed}")
    print(f"Max artists limit: {max_artists}")
    print(f"Limit respected: {'✓ Yes' if artists_processed <= max_artists else '✗ No'}")
    print(f"Remaining in queue: {queue.size}")
    
    if queue.size > 0:
        print(f"Artists left unprocessed: {list(queue.pending)[:5]}...")


def compare_with_without_limit():
    """Compare traversal with and without proper limiting."""
    print("\n\n" + "=" * 70)
    print("COMPARISON: WITH vs WITHOUT PROPER LIMITING")
    print("=" * 70)
    
    # Simulate WITHOUT proper limiting (old behavior)
    print("\n--- OLD BEHAVIOR (without capacity check) ---")
    artists_processed = 0
    queue_size = 0
    max_artists = 5
    
    # Start with 1 artist that leads to 3 others
    queue_size = 3
    
    for i in range(max_artists):
        artists_processed += 1
        # Each artist adds 3 more (old behavior)
        new_artists = 3
        queue_size = queue_size - 1 + new_artists
        print(f"Step {i+1}: Processed 1, added {new_artists}, "
              f"queue size: {queue_size}, total processed: {artists_processed}")
    
    print(f"\nResult: Processed {artists_processed} artists, "
          f"but queue kept growing to {queue_size}")
    
    # Simulate WITH proper limiting (new behavior)
    print("\n--- NEW BEHAVIOR (with capacity check) ---")
    artists_processed = 0
    queue_size = 0
    
    # Start with 1 artist
    queue_size = 3
    
    for i in range(max_artists):
        artists_processed += 1
        remaining_capacity = max_artists - artists_processed
        
        # Only add as many as we have capacity for
        new_artists = min(3, remaining_capacity)
        queue_size = queue_size - 1 + new_artists
        
        print(f"Step {i+1}: Processed 1, added {new_artists} (capacity: {remaining_capacity}), "
              f"queue size: {queue_size}, total processed: {artists_processed}")
        
        if remaining_capacity == 0:
            break
    
    print(f"\nResult: Processed exactly {artists_processed} artists, "
          f"queue controlled at {queue_size}")
    print(f"✓ Limit of {max_artists} properly respected!")


if __name__ == "__main__":
    visualize_queue_behavior()
    simulate_traversal_scenario()
    compare_with_without_limit()
