"""Reads the counters the limiter script writes.

The counters themselves are incremented inside the Lua script, so this module
only has to read and present them. Counts are per client and shared by every
instance, because they live in Redis rather than in any one process.
"""

import logging

import redis.asyncio as redis
from redis.exceptions import RedisError

from app.clients import registered_clients
from app.limiter import metrics_key, window_key

logger = logging.getLogger(__name__)


async def collect(client: redis.Redis) -> dict:
    """Per-client allowed/denied totals plus the live size of each window.

    Raises RedisError if Redis cannot be reached; the caller decides what to
    report in that case.
    """
    clients = registered_clients()

    seconds, micros = await client.time()
    now_ms = seconds * 1000 + micros // 1000

    # One round trip for all of it rather than two per client.
    pipe = client.pipeline(transaction=False)
    for c in clients:
        pipe.hgetall(metrics_key(c.client_id))
        # Count only what is still inside the window. The script prunes lazily,
        # so aged-out entries can still be sitting in the sorted set and a
        # plain ZCARD would over-report.
        pipe.zcount(window_key(c.client_id), now_ms - c.tier.window_ms, "+inf")
    results = await pipe.execute()

    report = {}
    for i, c in enumerate(clients):
        counters, in_window = results[i * 2], results[i * 2 + 1]
        allowed = int(counters.get("allowed", 0))
        denied = int(counters.get("denied", 0))
        report[c.client_id] = {
            "tier": c.tier.name,
            "limit": c.tier.limit,
            "window_ms": c.tier.window_ms,
            "allowed": allowed,
            "denied": denied,
            "total": allowed + denied,
            "in_window_now": in_window,
        }

    return report


async def reset(client: redis.Redis) -> None:
    """Clear the counters. Handy when running the load test repeatedly."""
    keys = [metrics_key(c.client_id) for c in registered_clients()]
    if keys:
        await client.delete(*keys)
