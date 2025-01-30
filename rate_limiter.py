import time
import asyncio
from dataclasses import dataclass
from typing import Dict, Optional
import logging
from collections import deque

@dataclass
class RateLimit:
    """Configuration for rate limiting"""
    requests_per_second: float
    burst_limit: int = 1
    retry_interval: float = 1.0
    max_retries: int = 3

class AsyncRateLimiter:
    """Asynchronous rate limiter with token bucket algorithm"""
    def __init__(self, rate_limit: RateLimit):
        self.rate_limit = rate_limit
        self.tokens = rate_limit.burst_limit
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
        self.request_queue = deque()
        self.processing = False

    async def acquire(self):
        """Acquire a token for making a request"""
        async with self.lock:
            while True:
                now = time.monotonic()
                time_passed = now - self.last_update
                self.tokens = min(
                    self.rate_limit.burst_limit,
                    self.tokens + time_passed * self.rate_limit.requests_per_second
                )
                self.last_update = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return True
                
                wait_time = (1 - self.tokens) / self.rate_limit.requests_per_second
                logging.debug(f"Rate limit reached, waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)

    async def execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with rate limiting and retries"""
        retries = 0
        while retries <= self.rate_limit.max_retries:
            try:
                await self.acquire()
                return await func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries > self.rate_limit.max_retries:
                    raise e
                
                wait_time = self.rate_limit.retry_interval * (2 ** (retries - 1))  # Exponential backoff
                logging.warning(f"Request failed, retrying in {wait_time:.2f}s (attempt {retries}/{self.rate_limit.max_retries})")
                await asyncio.sleep(wait_time)

class RateLimiterRegistry:
    """Registry for managing multiple rate limiters"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RateLimiterRegistry, cls).__new__(cls)
            cls._instance.limiters: Dict[str, AsyncRateLimiter] = {}
            cls._instance._init_default_limiters()
        return cls._instance

    def _init_default_limiters(self):
        """Initialize default rate limiters for different APIs"""
        self.limiters = {
            'helius': AsyncRateLimiter(RateLimit(
                requests_per_second=10,  # 10 RPS for Helius
                burst_limit=20,
                retry_interval=2.0
            )),
            'twitter': AsyncRateLimiter(RateLimit(
                requests_per_second=0.5,  # Conservative rate for Twitter
                burst_limit=5,
                retry_interval=5.0
            )),
            'jupiter': AsyncRateLimiter(RateLimit(
                requests_per_second=5,  # 5 RPS for Jupiter
                burst_limit=10,
                retry_interval=1.0
            )),
            'default': AsyncRateLimiter(RateLimit(
                requests_per_second=2,
                burst_limit=5,
                retry_interval=1.0
            ))
        }

    def get_limiter(self, name: str) -> AsyncRateLimiter:
        """Get a rate limiter by name"""
        return self.limiters.get(name, self.limiters['default'])

# Global registry instance
registry = RateLimiterRegistry()
