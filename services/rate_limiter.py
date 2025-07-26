"""
Rate limiter module for API requests.

This module handles rate limiting to respect API rate limits.
"""

import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter to respect API rate limits."""
    
    def __init__(self, requests_per_minute: int = 60):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_minute: Maximum number of requests allowed per minute
        """
        self.requests_per_minute = requests_per_minute
        self.interval = 60.0 / requests_per_minute
        self.last_request_time = 0.0
        logger.info(f"Initialized RateLimiter with {requests_per_minute} requests/minute")
    
    def wait_if_needed(self) -> Optional[float]:
        """
        Wait if necessary to respect rate limit.
        
        Returns:
            The time waited in seconds, or None if no wait was needed
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.interval:
            wait_time = self.interval - time_since_last_request
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
            self.last_request_time = time.time()
            return wait_time
        
        self.last_request_time = time.time()
        return None
    
    def reset(self):
        """Reset the rate limiter."""
        self.last_request_time = 0.0
        logger.debug("Rate limiter reset")
