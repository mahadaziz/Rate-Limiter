"""Rate limiting logic.

Everything that decides whether a request is allowed lives here. The routes in
`main.py` only translate the result into HTTP.
"""

import logging
import uuid
from pathlib import Path
from typing import NamedTuple

import redis.asyncio as redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

_LUA_PATH = Path(__file__).parent / "lua" / "sliding_window_log.lua"
_LUA_SOURCE = _LUA_PATH.read_text()

KEY_PREFIX = "ratelimit"


class RateLimitResult(NamedTuple):
    allowed: bool
    limit: int
    remaining: int
    retry_after_ms: int
    # True when Redis could not be reached and the request was let through
    # without being counted.
    degraded: bool = False


class RateLimiter:
    """Sliding-window-log limiter backed by a single Redis Lua script."""

    def __init__(self, client: redis.Redis) -> None:
        # register_script sends the body once and then calls it by SHA,
        # re-sending it automatically if Redis has forgotten it. Either way the
        # server runs the whole script atomically.
        self._script = client.register_script(_LUA_SOURCE)

        # Per-instance count of requests let through because Redis was
        # unreachable. Deliberately not stored in Redis: the one moment this
        # number matters is the moment Redis cannot be written to.
        self.fallback_count = 0

    async def check(self, client_id: str, limit: int, window_ms: int) -> RateLimitResult:
        """Record a request against `client_id` and say whether it is allowed.

        Fails open. If Redis cannot be reached the request is allowed through
        uncounted, which trades correctness of the limit for availability of
        the service behind it: a limiter outage degrades into no limiting
        rather than into a total outage. That is the right call when the
        limiter is protecting against accidental overuse by known clients, and
        the wrong one when it is the thing standing between an unauthenticated
        internet and an expensive backend. Fail closed there instead.
        """
        # A unique member per request, so two requests landing in the same
        # millisecond are two entries in the log rather than one overwriting
        # the other.
        member = uuid.uuid4().hex

        try:
            allowed, remaining, retry_after_ms = await self._script(
                keys=[f"{KEY_PREFIX}:{client_id}"],
                args=[window_ms, limit, member],
            )
        except (RedisError, OSError) as exc:
            self.fallback_count += 1
            logger.error(
                "redis unreachable, failing open for client=%s (%s: %s); "
                "fallbacks on this instance: %d",
                client_id,
                type(exc).__name__,
                exc,
                self.fallback_count,
            )
            return RateLimitResult(
                allowed=True,
                limit=limit,
                remaining=limit,
                retry_after_ms=0,
                degraded=True,
            )

        return RateLimitResult(
            allowed=bool(allowed),
            limit=limit,
            remaining=int(remaining),
            retry_after_ms=int(retry_after_ms),
        )
