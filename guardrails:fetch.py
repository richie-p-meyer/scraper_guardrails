"""
Resilient fetch utilities: retry/backoff, rate limiting, and circuit breaker.

This module provides the fault-tolerance backbone for all scraping pipelines.
"""

import asyncio
import random
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Optional, Callable, Awaitable
from aiohttp import ClientSession, ClientTimeout


# ─────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────

class CircuitOpen(Exception):
    """Raised when the circuit breaker is open (cooldown period)."""
    pass


class RetryExhausted(Exception):
    """Raised after all retry attempts fail."""
    pass


# ─────────────────────────────────────────────────────────────
# Backoff Configuration
# ─────────────────────────────────────────────────────────────

@dataclass
class Backoff:
    attempts: int = 6         # max retry attempts
    base: float = 0.4         # initial delay
    cap: float = 8.0          # max delay cap
    jitter: float = 0.4       # random(0, jitter) added to delay
    multiplier: float = 2.0   # exponential growth factor


# ─────────────────────────────────────────────────────────────
# Rate Limiter (token bucket)
# ─────────────────────────────────────────────────────────────

class RateLimiter:
    """
    Simple token bucket rate limiter.
    Prevents upstream bans and handles uneven traffic.
    """
    def __init__(self, rate_per_sec: float, capacity: int):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = float(capacity)
        self.t = time.monotonic()

    async def acquire(self):
        while True:
            now = time.monotonic()
            self.tokens = min(
                self.capacity,
                self.tokens + (now - self.t) * self.rate
            )
            self.t = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            await asyncio.sleep(0.05)


# ─────────────────────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Prevents hammering a failing upstream.
    Opens after fail_threshold, closes after cooldown.
    """

    def __init__(self, fail_threshold=8, cooldown=15.0):
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown
        self.failures = 0
        self.open_until = 0.0

    def on_success(self):
        self.failures = 0
        self.open_until = 0.0

    def on_failure(self):
        self.failures += 1
        if self.failures >= self.fail_threshold:
            self.open_until = time.monotonic() + self.cooldown

    def check(self):
        if time.monotonic() < self.open_until:
            raise CircuitOpen("Circuit open: temporarily backing off.")


# ─────────────────────────────────────────────────────────────
# HTTP Session Context Manager
# ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def session(headers: Optional[dict] = None) -> ClientSession:
    """
    aiohttp session with sane defaults for production scraping.
    """
    ua = headers or {
        "User-Agent": "GuardrailsBot/1.0 (+https://example.com)"
    }
    timeout = ClientTimeout(
        total=30,
        connect=10,
        sock_read=20
    )
    async with ClientSession(headers=ua, timeout=timeout) as s:
        yield s


# ─────────────────────────────────────────────────────────────
# Exponential Backoff Executor
# ─────────────────────────────────────────────────────────────

async def exp_backoff_call(
    fn: Callable[[], Awaitable],
    backoff: Backoff,
    breaker: CircuitBreaker
):
    """
    Executes fn() with retries, jitter, exponential backoff, and circuit breaker protection.
    """
    delay = backoff.base
    last_exc = None

    for _ in range(backoff.attempts):
        try:
            breaker.check()
            result = await fn()
            breaker.on_success()
            return result

        except CircuitOpen as e:
            last_exc = e
            await asyncio.sleep(1.0)

        except Exception as e:
            last_exc = e
            breaker.on_failure()

            # Increase penalty for known retryable errors
            penalty = 0.0
            status = getattr(e, "status", None)
            if status in (429, 500, 502, 503, 504):
                penalty = 0.5 * delay

            sleep_for = (
                min(delay, backoff.cap)
                + random.random() * backoff.jitter
                + penalty
            )

            await asyncio.sleep(sleep_for)
            delay *= backoff.multiplier

    raise RetryExhausted(str(last_exc) if last_exc else "Retry attempts exhausted.")


# ─────────────────────────────────────────────────────────────
# Main Fetch Function
# ─────────────────────────────────────────────────────────────

async def fetch_text(
    url: str,
    rl: RateLimiter,
    headers: Optional[dict] = None,
    backoff: Backoff = Backoff(),
    breaker: CircuitBreaker = CircuitBreaker()
) -> str:
    """
    Fetch a URL with:
    - Rate limiting
    - Retry + exponential backoff
    - Jitter
    - Circuit breaker

    Returns:
        HTML/text response.
    """

    await rl.acquire()

    async with session(headers=headers) as s:

        async def _get():
            async with s.get(url, allow_redirects=True) as r:
                if r.status >= 400:
                    ex = Exception(f"http {r.status} {url}")
                    setattr(ex, "status", r.status)  # annotate for retry logic
                    raise ex

                return await r.text()

        return await exp_backoff_call(_get, backoff, breaker)
