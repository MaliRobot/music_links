# Fix for max_artists Limit Not Being Respected

## Problem Description

The `max_artists` parameter was not being properly respected during the traversal process. The code would check the limit only after processing an artist and extracting all related artists from their releases. This meant that if an artist had many releases with many collaborators, the number of artists added to the queue could far exceed the specified limit.

### Example Scenario
- `max_artists` set to 20
- Process artist A who has 50 releases with 100 unique collaborators
- All 100 collaborators would be added to the queue
- Only after returning to the main loop would the limit be checked
- Result: Way more than 20 artists could be traversed

## Root Cause

The issue was in the `SingleArtistTraverser._process_releases()` method and how the `TraversalManager` handled the queue:

1. **No limit checking during extraction**: When processing releases, all artists were extracted without checking against the limit
2. **No limit on queue additions**: The queue would accept all artists found, only checking the limit when deciding whether to process the next artist
3. **No communication of remaining capacity**: The traverser had no knowledge of how many more artists could be added

## Implemented Fixes

### 1. **Added `max_new_artists` parameter to `process_artist()`**
```python
def process_artist(self, discogs_id: str, max_new_artists: Optional[int] = None) -> Set[str]:
```
This allows the manager to tell the traverser exactly how many new artists it can extract.

### 2. **Early termination in `_process_releases()`**
The method now checks the limit:
- Before processing each page of releases
- Before processing each individual release
- When adding artists to the result set

```python
# Check if we've reached the limit
if max_new_artists is not None and len(all_artists) >= max_new_artists:
    logger.info(f"Reached max_new_artists limit ({max_new_artists}), stopping release processing")
    break
```

### 3. **Improved queue management**
- Queue max size set to `max_artists` to prevent unbounded growth
- Added `max_total_artists` parameter to `add_multiple()` to respect global limit
- Added `total_artists` property to track both checked and pending artists

### 4. **Capacity calculation in main loop**
```python
# Calculate how many new artists we can accept
total_artists_so_far = self.statistics.artists_processed + self.queue.size
remaining_capacity = max(0, self.config.max_artists - total_artists_so_far)

# Process artist with the limit on new artists
related_artists = self.single_traverser.process_artist(artist_id, max_new_artists=remaining_capacity)
```

## Benefits of the Fix

1. **Precise limit enforcement**: The traversal will now stop at exactly `max_artists` or slightly below (never above)
2. **Efficient processing**: Avoids wasting time extracting artists that won't be processed
3. **Predictable behavior**: The limit is respected consistently, not just eventually
4. **Better resource management**: Queue size is bounded, preventing memory issues with large traversals

## Testing Recommendations

To verify the fix works correctly:

1. **Test with small limits** (e.g., `max_artists=5`) and verify exactly 5 or fewer artists are processed
2. **Test with artists who have many collaborators** to ensure the limit is respected even when many artists are found
3. **Monitor the queue size** to ensure it doesn't grow beyond the limit
4. **Check log output** for messages indicating when limits are reached and processing stops

## Usage Example

```python
from services.traversal_manager import TraversalManager, TraversalConfig

# Configure with strict limit
config = TraversalConfig(
    max_artists=20,  # Will now strictly respect this limit
    include_extra_artists=True,
    include_credits=True
)

manager = TraversalManager(db, api_client, config)
stats = manager.traverse("12345")

# stats.artists_processed will be <= 20
print(f"Processed {stats.artists_processed} artists")  # Will never exceed 20
```

## File Changes Required

To implement this fix in your codebase, replace the content of `/services/traversal_manager.py` with the fixed version provided in `traversal_manager_fixed.py`.

The fix is backward compatible and doesn't require changes to other files unless they directly instantiate the internal classes with custom parameters.
