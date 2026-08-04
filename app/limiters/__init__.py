"""Rate limiting algorithms.

Both implement the same `Limiter` interface, so which one is in use is a
configuration choice rather than a code change. See the README for the
tradeoff between them.
"""

import redis.asyncio as redis

from app.limiters.base import (
    Limiter,
    RateLimitResult,
    metrics_key,
    tagged_key,
)
from app.limiters.sliding_window_log import SlidingWindowLogLimiter
from app.limiters.token_bucket import TokenBucketLimiter

ALGORITHMS: dict[str, type[Limiter]] = {
    SlidingWindowLogLimiter.name: SlidingWindowLogLimiter,
    TokenBucketLimiter.name: TokenBucketLimiter,
}


def build_limiter(name: str, client: redis.Redis) -> Limiter:
    """Construct the named algorithm, or fail loudly at startup.

    Better to refuse to start than to silently fall back to a default, which
    would mean running a limiter nobody chose.
    """
    try:
        limiter_class = ALGORITHMS[name]
    except KeyError:
        known = ", ".join(sorted(ALGORITHMS))
        raise ValueError(f"unknown algorithm {name!r}; known algorithms: {known}") from None

    return limiter_class(client)


__all__ = [
    "ALGORITHMS",
    "Limiter",
    "RateLimitResult",
    "SlidingWindowLogLimiter",
    "TokenBucketLimiter",
    "build_limiter",
    "metrics_key",
    "tagged_key",
]
