"""
Test script to compare the performance of original vs optimized traverser.
This script can be used to verify the improvements.
"""

import asyncio
import time
import sys
import os
from unittest.mock import MagicMock, patch
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_mock_release(release_id, title, year=2020):
    """Create a mock release object."""
    release = MagicMock()
    release.id = release_id
    release.title = title
    release.year = year
    release.url = f"http://discogs.com/release/{release_id}"
    return release


def create_mock_artist(artist_id, name, num_releases=100):
    """Create a mock artist object with releases."""
    artist = MagicMock()
    artist.id = artist_id
    artist.name = name
    artist.url = f"http://discogs.com/artist/{artist_id}"
    
    # Create mock releases
    releases = []
    for i in range(num_releases):
        release = create_mock_release(f"r{artist_id}_{i}", f"Release {i}")
        # Add some collaborating artists to each release
        release.artists = [MagicMock(id=f"collab_{i}", name=f"Collaborator {i}")]
        releases.append(release)
    
    # Mock paginated releases
    releases_paginator = MagicMock()
    releases_paginator.pages = (num_releases // 50) + 1  # 50 releases per page
    
    def get_page(page_num):
        start = (page_num - 1) * 50
        end = min(start + 50, num_releases)
        return releases[start:end]
    
    releases_paginator.page = get_page
    artist.releases = releases_paginator
    
    return artist


async def test_original_fetch_releases():
    """Test the original implementation's release fetching."""
    logger.info("Testing ORIGINAL implementation...")
    
    # Simulate the original problematic code
    async def fetch_release(r):
        await asyncio.sleep(0.01)  # Simulate API call
        return {"title": r.title, "id": r.id}
    
    # Create a large number of releases
    releases = [create_mock_release(f"r{i}", f"Release {i}") for i in range(1000)]
    
    start_time = time.time()
    
    try:
        # This is how the original code works - ALL at once
        tasks = [fetch_release(r) for r in releases]
        logger.info(f"Created {len(tasks)} concurrent tasks")
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        elapsed = time.time() - start_time
        logger.info(f"Original: Processed {len(results)} releases in {elapsed:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Original implementation failed: {e}")


async def test_optimized_fetch_releases():
    """Test the optimized implementation's release fetching."""
    logger.info("Testing OPTIMIZED implementation...")
    
    async def fetch_release(r):
        await asyncio.sleep(0.01)  # Simulate API call
        return {"title": r.title, "id": r.id}
    
    # Create a large number of releases
    releases = [create_mock_release(f"r{i}", f"Release {i}") for i in range(1000)]
    
    start_time = time.time()
    results = []
    
    BATCH_SIZE = 20
    
    try:
        # Process in batches as in the optimized version
        for i in range(0, len(releases), BATCH_SIZE):
            batch = releases[i:i + BATCH_SIZE]
            
            tasks = [fetch_release(r) for r in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend(batch_results)
            
            # Small delay between batches
            if i + BATCH_SIZE < len(releases):
                await asyncio.sleep(0.002)
        
        elapsed = time.time() - start_time
        logger.info(f"Optimized: Processed {len(results)} releases in {elapsed:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Optimized implementation failed: {e}")


async def test_rate_limiter():
    """Test the token bucket rate limiter."""
    logger.info("Testing Rate Limiter...")
    
    class RateLimiter:
        def __init__(self, rate: int = 10, window: int = 1):
            self.rate = rate
            self.window = window
            self.tokens = rate
            self.last_update = time.time()
            self._lock = asyncio.Lock()
        
        async def acquire(self):
            async with self._lock:
                now = time.time()
                elapsed = now - self.last_update
                
                tokens_to_add = elapsed * (self.rate / self.window)
                self.tokens = min(self.rate, self.tokens + tokens_to_add)
                self.last_update = now
                
                if self.tokens < 1:
                    wait_time = (1 - self.tokens) * (self.window / self.rate)
                    await asyncio.sleep(wait_time)
                    self.tokens = 0
                else:
                    self.tokens -= 1
    
    rate_limiter = RateLimiter(rate=10, window=1)  # 10 requests per second
    
    async def make_request(request_id):
        await rate_limiter.acquire()
        return f"Request {request_id} completed"
    
    start_time = time.time()
    
    # Try to make 30 requests
    tasks = [make_request(i) for i in range(30)]
    results = await asyncio.gather(*tasks)
    
    elapsed = time.time() - start_time
    rate = len(results) / elapsed
    
    logger.info(f"Rate Limiter: Made {len(results)} requests in {elapsed:.2f} seconds")
    logger.info(f"Actual rate: {rate:.2f} requests/second (target: 10/sec)")


async def test_memory_usage():
    """Compare memory usage between implementations."""
    import tracemalloc
    
    logger.info("Testing Memory Usage...")
    
    # Test with large dataset
    num_releases = 5000
    
    # Original approach
    tracemalloc.start()
    
    async def mock_task(i):
        await asyncio.sleep(0.001)
        return i
    
    # Create all tasks at once (original approach)
    tasks = [mock_task(i) for i in range(num_releases)]
    
    current, peak = tracemalloc.get_traced_memory()
    logger.info(f"Original approach - Peak memory: {peak / 1024 / 1024:.2f} MB")
    tracemalloc.stop()
    
    # Cancel tasks to clean up
    for task in tasks:
        if asyncio.iscoroutine(task):
            task.close()
    
    # Optimized approach
    tracemalloc.start()
    
    BATCH_SIZE = 50
    for i in range(0, num_releases, BATCH_SIZE):
        batch_tasks = [mock_task(j) for j in range(i, min(i + BATCH_SIZE, num_releases))]
        await asyncio.gather(*batch_tasks)
    
    current, peak = tracemalloc.get_traced_memory()
    logger.info(f"Optimized approach - Peak memory: {peak / 1024 / 1024:.2f} MB")
    tracemalloc.stop()


async def main():
    """Run all tests."""
    logger.info("="*60)
    logger.info("Starting Traverser Performance Tests")
    logger.info("="*60)
    
    # Test 1: Compare fetch releases performance
    logger.info("\n--- Test 1: Fetch Releases Performance ---")
    await test_original_fetch_releases()
    await asyncio.sleep(1)
    await test_optimized_fetch_releases()
    
    # Test 2: Rate limiter
    logger.info("\n--- Test 2: Rate Limiter ---")
    await test_rate_limiter()
    
    # Test 3: Memory usage
    logger.info("\n--- Test 3: Memory Usage ---")
    await test_memory_usage()
    
    logger.info("\n" + "="*60)
    logger.info("All tests completed!")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
