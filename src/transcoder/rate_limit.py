"""
Token-bucket rate limiter for outbound API calls.

The Dropbox API enforces per-app and per-user limits; without a client-side
budget the daemon either gets throttled (429) or starves other clients on the
same token. This module provides a thread-safe token bucket and a wait helper
that respects a stop event.
"""

from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Thread-safe token bucket.

    The bucket starts full (capacity = burst). Tokens refill continuously at
    rate_per_min / 60 tokens per second. Callers acquire `weight` tokens;
    if not enough are available, acquire blocks until they accumulate or the
    stop_event is set.
    """

    def __init__(
        self,
        rate_per_min: int,
        burst: int,
        name: str = "tokens",
    ) -> None:
        if rate_per_min <= 0:
            raise ValueError("rate_per_min must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self.name = name
        self.rate_per_sec = rate_per_min / 60.0
        self.capacity = float(burst)
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
            self._last_refill = now

    def acquire(
        self,
        weight: float = 1.0,
        stop_event: threading.Event | None = None,
        max_wait_sec: float = 600.0,
    ) -> bool:
        """
        Block until `weight` tokens are available.

        Returns True if tokens were granted; False if the stop_event fired or
        max_wait_sec elapsed while waiting.
        """
        if weight > self.capacity:
            # A single weighted call larger than the burst would never succeed;
            # let it through but log so the operator can fix the config.
            logger.warning(
                "rate_limit[%s]: weight=%.1f exceeds capacity=%.1f, allowing through",
                self.name,
                weight,
                self.capacity,
            )
            return True

        deadline = time.monotonic() + max_wait_sec
        first_wait_logged = False

        while True:
            with self._lock:
                self._refill_locked()
                if self._tokens >= weight:
                    self._tokens -= weight
                    return True
                missing = weight - self._tokens
                wait_for = missing / self.rate_per_sec

            now = time.monotonic()
            if now >= deadline:
                logger.warning(
                    "rate_limit[%s]: gave up after %.1fs waiting for %.1f tokens",
                    self.name,
                    max_wait_sec,
                    weight,
                )
                return False

            wait_for = min(wait_for, deadline - now, 5.0)
            if not first_wait_logged and wait_for > 0.5:
                logger.info(
                    "rate_limit[%s]: throttling, waiting %.1fs for %.1f tokens",
                    self.name,
                    wait_for,
                    weight,
                )
                first_wait_logged = True

            if stop_event is not None:
                if stop_event.wait(wait_for):
                    return False
            else:
                time.sleep(wait_for)

    def on_throttle(self, retry_after_sec: float) -> None:
        """
        Called when the server returns 429. Drains the bucket so subsequent
        callers wait at least `retry_after_sec` before retrying.
        """
        with self._lock:
            penalty = max(0.0, retry_after_sec) * self.rate_per_sec
            self._tokens = max(0.0, self._tokens - penalty)
            logger.warning(
                "rate_limit[%s]: server-throttled, penalising %.1fs (~%.1f tokens)",
                self.name,
                retry_after_sec,
                penalty,
            )
