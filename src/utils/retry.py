"""
Retry logic and strategies for the NuriJangter crawler.

This module provides decorators and utilities for implementing
robust retry logic with exponential backoff.
"""

import time
import asyncio
from typing import TypeVar, Callable, Optional, Type, Tuple
from functools import wraps
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')


class RetryStrategy:
    """
    Configurable retry strategy with exponential backoff.

    This class encapsulates retry configuration and provides
    methods for calculating delays.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 10.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        """
        Initialize retry strategy.

        Args:
            max_attempts: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            backoff_factor: Multiplier for exponential backoff
            max_delay: Maximum delay in seconds
            exceptions: Tuple of exception types to retry on
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.exceptions = exceptions

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt using exponential backoff.

        Args:
            attempt: Current attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        delay = self.initial_delay * (self.backoff_factor ** attempt)
        return min(delay, self.max_delay)

    def should_retry(self, exception: Exception) -> bool:
        """
        Check if an exception should trigger a retry.

        Args:
            exception: The exception that occurred

        Returns:
            True if should retry, False otherwise
        """
        return isinstance(exception, self.exceptions)


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 10.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> Callable:
    """
    Decorator for adding retry logic to synchronous functions.

    Args:
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Exponential backoff multiplier
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exception types to retry on
        on_retry: Optional callback function called on each retry

    Returns:
        Decorated function with retry logic

    Example:
        @with_retry(max_attempts=3, initial_delay=1.0)
        def fetch_data():
            # Function that might fail
            pass
    """
    strategy = RetryStrategy(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
        exceptions=exceptions
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception = None

            for attempt in range(strategy.max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if not strategy.should_retry(e):
                        logger.error(f"Non-retryable exception: {e}")
                        raise

                    if attempt < strategy.max_attempts - 1:
                        delay = strategy.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt + 1}/{strategy.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )

                        if on_retry:
                            on_retry(attempt + 1, e)

                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {strategy.max_attempts} attempts failed. Last error: {e}"
                        )

            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError("Retry logic failed without capturing exception")

        return wrapper

    return decorator


def with_async_retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 10.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[int, Exception], None]] = None
) -> Callable:
    """
    Decorator for adding retry logic to asynchronous functions.

    Args:
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay in seconds
        backoff_factor: Exponential backoff multiplier
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exception types to retry on
        on_retry: Optional callback function called on each retry

    Returns:
        Decorated async function with retry logic

    Example:
        @with_async_retry(max_attempts=3, initial_delay=1.0)
        async def fetch_data():
            # Async function that might fail
            pass
    """
    strategy = RetryStrategy(
        max_attempts=max_attempts,
        initial_delay=initial_delay,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
        exceptions=exceptions
    )

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_exception = None

            for attempt in range(strategy.max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if not strategy.should_retry(e):
                        logger.error(f"Non-retryable exception: {e}")
                        raise

                    if attempt < strategy.max_attempts - 1:
                        delay = strategy.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt + 1}/{strategy.max_attempts} failed: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )

                        if on_retry:
                            on_retry(attempt + 1, e)

                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"All {strategy.max_attempts} attempts failed. Last error: {e}"
                        )

            # If we get here, all retries failed
            if last_exception:
                raise last_exception
            raise RuntimeError("Retry logic failed without capturing exception")

        return wrapper

    return decorator


class RetryContext:
    """
    Context manager for retry logic.

    Provides a more flexible way to implement retry logic
    compared to decorators.

    Example:
        retry = RetryContext(max_attempts=3)
        for attempt in retry:
            with attempt:
                # Code that might fail
                result = risky_operation()
                break  # Success, exit retry loop
    """

    def __init__(self, strategy: Optional[RetryStrategy] = None, **kwargs):
        """
        Initialize retry context.

        Args:
            strategy: Optional RetryStrategy instance
            **kwargs: If strategy not provided, these are passed to RetryStrategy
        """
        self.strategy = strategy or RetryStrategy(**kwargs)
        self.current_attempt = 0
        self.last_exception = None

    def __iter__(self):
        """Iterate through retry attempts."""
        self.current_attempt = 0
        return self

    def __next__(self):
        """Get next retry attempt."""
        if self.current_attempt >= self.strategy.max_attempts:
            if self.last_exception:
                raise self.last_exception
            raise StopIteration

        attempt = _RetryAttempt(self)
        self.current_attempt += 1
        return attempt


class _RetryAttempt:
    """Individual retry attempt context manager."""

    def __init__(self, retry_context: RetryContext):
        """Initialize retry attempt."""
        self.retry_context = retry_context
        self.strategy = retry_context.strategy

    def __enter__(self):
        """Enter retry attempt context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit retry attempt context."""
        if exc_type is None:
            return True  # Success

        if not self.strategy.should_retry(exc_val):
            return False  # Don't suppress non-retryable exceptions

        self.retry_context.last_exception = exc_val

        if self.retry_context.current_attempt < self.strategy.max_attempts:
            delay = self.strategy.calculate_delay(self.retry_context.current_attempt - 1)
            logger.warning(
                f"Attempt {self.retry_context.current_attempt}/"
                f"{self.strategy.max_attempts} failed: {exc_val}. "
                f"Retrying in {delay:.2f}s..."
            )
            time.sleep(delay)
            return True  # Suppress exception for retry

        return False  # Don't suppress on last attempt
