"""Reads the counters the limiter scripts write.

The counters are incremented inside whichever Lua script made the decision, so
this module only has to read and present them. Counts are per client and shared
by every instance, because they live in Redis rather than in any one process.
"""

import asyncio
import logging

from app.clients import registered_clients
from app.limiters import Limiter, metrics_key

logger = logging.getLogger(__name__)


async def collect(limiter: Limiter) -> dict:
    """Per-client allowed/denied totals plus how much of the limit is held now.

    Raises RedisError if Redis cannot be reached; the caller decides what to
    report in that case.
    """
    clients = registered_clients()

    # One clock reading for the whole report, so every client's usage is
    # measured against the same instant.
    now_ms = await limiter.now_ms()

    # One round trip for all the counters rather than one per client.
    pipe = limiter.client.pipeline(transaction=False)
    for c in clients:
        pipe.hgetall(metrics_key(c.client_id))
    counters = await pipe.execute()

    usage = await asyncio.gather(
        *(
            limiter.current_usage(c.client_id, c.tier.limit, c.tier.window_ms, now_ms)
            for c in clients
        )
    )

    report = {}
    for c, counts, in_use in zip(clients, counters, usage):
        allowed = int(counts.get("allowed", 0))
        denied = int(counts.get("denied", 0))
        report[c.client_id] = {
            "tier": c.tier.name,
            "limit": c.tier.limit,
            "window_ms": c.tier.window_ms,
            "allowed": allowed,
            "denied": denied,
            "total": allowed + denied,
            "in_use_now": in_use,
        }

    return report


async def reset(limiter: Limiter) -> None:
    """Clear the counters. Handy when running the load test repeatedly."""
    keys = [metrics_key(c.client_id) for c in registered_clients()]
    if keys:
        await limiter.client.delete(*keys)
