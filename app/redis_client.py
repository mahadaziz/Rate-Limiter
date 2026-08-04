"""Shared Redis client.

One connection pool per app process, created on startup and closed on
shutdown. Every instance points at the same Redis, which is what makes the
rate limit a single shared counter rather than three independent ones.
"""

import redis.asyncio as redis

from app.config import REDIS_TIMEOUT_SECONDS, REDIS_URL

_client: redis.Redis | None = None


def create_client() -> redis.Redis:
    """Build the process-wide Redis client."""
    global _client
    _client = redis.from_url(
        REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        socket_timeout=REDIS_TIMEOUT_SECONDS,
        socket_connect_timeout=REDIS_TIMEOUT_SECONDS,
    )
    return _client


def get_client() -> redis.Redis:
    if _client is None:
        raise RuntimeError("Redis client not initialised")
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
