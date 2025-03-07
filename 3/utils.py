import asyncio
import time
from functools import wraps
from typing import Callable

from aiohttp import ClientError

from consts import ASYNCIO_POLL_TIME_IN_SECONDS, MAX_RETRIES, INITIAL_RETRY_DELAY, BACKOFF_FACTOR
from logger import logger


class TokenBucket:
    """
    Implementation of the token bucket data structure for rate limiting via Token Bucket Algorithm.
    It is not thread-safe, because the assignment does nor require thread-safety.
    In a real-world situation, it is better to check for existing solutions,
    with more thorough implementation, e.g. aiolimiter.
    """
    def __init__(self, max_tokens: int, refill_rate: float | None = None):
        self._max_tokens: int = max_tokens
        self._current_tokens: int = max_tokens
        self._refill_rate: float = refill_rate or float(max_tokens)  # tokens per second
        self._updated_at: float = time.monotonic()

    async def wait_for_token(self):
        while self._current_tokens < 1:
            self._add_new_tokens()
            await asyncio.sleep(ASYNCIO_POLL_TIME_IN_SECONDS)
        self._current_tokens -= 1

    def _add_new_tokens(self):
        now = time.monotonic()
        time_since_update = now - self._updated_at
        new_tokens = time_since_update * self._refill_rate
        if self._current_tokens + new_tokens >= 1:
            self._current_tokens = min(self._current_tokens + new_tokens, self._max_tokens)
            self._updated_at = now


def retry[T](
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    backoff_factor: float = BACKOFF_FACTOR,
    retryable_exceptions: tuple = (ClientError, asyncio.TimeoutError),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    A decorator to retry an async function with exponential backoff.
    """
    # in production code it is better to use library like backoff
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            retry_delay = initial_delay

            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)

                except retryable_exceptions as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= backoff_factor

        return wrapper

    return decorator
