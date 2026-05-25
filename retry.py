"""
retry.py
--------
Retry logic for transient failures.
"""

import logging
import random
import time
from functools import wraps
from typing import Callable, Tuple, Type

log = logging.getLogger(__name__)

# Exceptions that are safe to retry
RETRYABLE_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,  # Includes network errors
)

# Add database-specific exceptions dynamically
try:
    import oracledb

    RETRYABLE_EXCEPTIONS += (
        oracledb.DatabaseError,
        oracledb.InterfaceError,
    )
except ImportError:
    pass

try:
    import psycopg2

    RETRYABLE_EXCEPTIONS += (
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
    )
except ImportError:
    pass


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: Tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS,
):
    """
    Decorator that retries a function with exponential backoff.
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        log.error(
                            "All %d retries exhausted for %s: %s",
                            max_retries,
                            func.__name__,
                            e,
                        )
                        raise

                    delay = min(
                        base_delay * (exponential_base**attempt),
                        max_delay,
                    )

                    if jitter:
                        delay *= 0.5 + random.random()

                    log.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.1fs...",
                        attempt + 1,
                        max_retries + 1,
                        func.__name__,
                        e,
                        delay,
                    )
                    time.sleep(delay)

            raise last_exception

        return wrapper

    return decorator


class RetryableOperation:
    """
    Context-based retry for operations that need state management.
    """

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.attempt = 0
        self._should_retry = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __iter__(self):
        while self.attempt <= self.max_retries:
            self._should_retry = False
            yield self.attempt

            if not self._should_retry:
                break

            self.attempt += 1
            if self.attempt <= self.max_retries:
                delay = self.base_delay * (2 ** (self.attempt - 1))
                time.sleep(delay)

    def retry(self):
        """Mark current attempt for retry."""
        self._should_retry = True

