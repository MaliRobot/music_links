"""
API client wrapper with retry logic and logging.

This module wraps the Discogs API client to add retry logic, logging, and error handling.
"""

import time
import logging
from typing import Any, Optional
from dataclasses import dataclass

import discogs_client.exceptions

from services.disco_conn import DiscoConnector
from services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    max_attempts: int = 3
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff: float = 60.0


class APIError(Exception):
    """Custom exception for API-related errors."""
    pass


class RateLimitError(APIError):
    """Exception raised when rate limit is exceeded."""
    pass


class DiscogsAPIClient:
    """
    Wrapper for DiscoConnector that adds retry logic, rate limiting, and logging.
    """
    
    def __init__(
        self, 
        client: DiscoConnector, 
        rate_limiter: Optional[RateLimiter] = None,
        retry_config: Optional[RetryConfig] = None
    ):
        """
        Initialize the API client wrapper.
        
        Args:
            client: The underlying Discogs client
            rate_limiter: Optional rate limiter instance
            retry_config: Optional retry configuration
        """
        self.client = client
        self.rate_limiter = rate_limiter or RateLimiter()
        self.retry_config = retry_config or RetryConfig()
        self.request_count = 0
        self.error_count = 0
        logger.info("Initialized Discogs API client wrapper")
    
    def _execute_with_retry(self, method_name: str, *args, **kwargs) -> Any:
        """
        Execute an API method with retry logic.
        
        Args:
            method_name: Name of the method to call on the client
            *args: Positional arguments for the method
            **kwargs: Keyword arguments for the method
            
        Returns:
            The result of the API call
            
        Raises:
            APIError: If the API call fails after all retries
        """
        # Apply rate limiting
        self.rate_limiter.wait_if_needed()
        self.request_count += 1
        
        logger.debug(f"API call #{self.request_count}: {method_name}(args={args}, kwargs={kwargs})")
        
        last_error = None
        backoff = self.retry_config.initial_backoff
        
        for attempt in range(self.retry_config.max_attempts):
            try:
                method = getattr(self.client, method_name)
                result = method(*args, **kwargs)
                logger.debug(f"API call #{self.request_count} successful")
                return result
                
            except discogs_client.exceptions.HTTPError as e:
                last_error = e
                self.error_count += 1
                
                if e.status_code == 429:  # Rate limit exceeded
                    if attempt < self.retry_config.max_attempts - 1:
                        logger.warning(
                            f"Rate limit hit (attempt {attempt + 1}/{self.retry_config.max_attempts}). "
                            f"Waiting {backoff} seconds..."
                        )
                        time.sleep(backoff)
                        backoff = min(backoff * self.retry_config.backoff_multiplier, 
                                    self.retry_config.max_backoff)
                    else:
                        logger.error(f"Rate limit exceeded after {self.retry_config.max_attempts} attempts")
                        raise RateLimitError(f"Rate limit exceeded: {e}") from e
                else:
                    logger.error(f"HTTP error in API call: {e}")
                    raise APIError(f"HTTP error: {e}") from e
                    
            except Exception as e:
                last_error = e
                self.error_count += 1
                logger.error(f"Unexpected error in API call {method_name}: {e}")
                
                if attempt < self.retry_config.max_attempts - 1:
                    logger.info(f"Retrying after error (attempt {attempt + 1}/{self.retry_config.max_attempts})")
                    time.sleep(backoff)
                    backoff = min(backoff * self.retry_config.backoff_multiplier, 
                                self.retry_config.max_backoff)
                else:
                    raise APIError(f"API call failed: {e}") from e
        
        # Should not reach here, but just in case
        raise APIError(f"API call failed after {self.retry_config.max_attempts} attempts") from last_error
    
    def fetch_artist_by_discogs_id(self, discogs_id: str) -> Optional[Any]:
        """
        Fetch artist data by Discogs ID.
        
        Args:
            discogs_id: The Discogs ID of the artist
            
        Returns:
            Artist data or None if not found
        """
        logger.info(f"Fetching artist with Discogs ID: {discogs_id}")
        return self._execute_with_retry('fetch_artist_by_discogs_id', discogs_id)
    
    def get_artist(self, artist_id: str) -> Optional[Any]:
        """
        Get artist data.
        
        Args:
            artist_id: The ID of the artist
            
        Returns:
            Artist data or None if not found
        """
        logger.info(f"Getting artist with ID: {artist_id}")
        return self._execute_with_retry('get_artist', artist_id)
    
    def get_release(self, release_id: str) -> Optional[Any]:
        """
        Get release data.
        
        Args:
            release_id: The ID of the release
            
        Returns:
            Release data or None if not found
        """
        logger.debug(f"Getting release with ID: {release_id}")
        return self._execute_with_retry('get_release', release_id)
    
    def get_statistics(self) -> dict:
        """
        Get statistics about API usage.
        
        Returns:
            Dictionary with statistics
        """
        return {
            'request_count': self.request_count,
            'error_count': self.error_count,
            'error_rate': self.error_count / max(self.request_count, 1)
        }
