"""Rate limiting logic.

Everything that decides whether a request is allowed lives here. The routes in
`main.py` only translate the result into HTTP.
"""

import logging
import uuid
from pathlib import Path
from typing import NamedTuple

import redis.asyncio as redis

logger = logging.getLogger(__name__)

_LUA_PATH = Path(__file__).parent / "lua" / "sliding_window_log.lua"
_LUA_SOURCE = _LUA_PATH.read_text()

KEY_PREFIX = "ratelimit"


class RateLimitResult(NamedTuple):
    allowed: bool
    limit: int
    remaining: int
    retry_after_ms: int


class RateLimiter:
    """Sliding-window-log limiter backed by a single Redis Lua script."""

    def __init__(self, client: redis.Redis) -> None:
        # register_script sends the body once and then calls it by SHA,
        # re-sending it automatically if Redis has forgotten it. Either way the
        # server runs the whole script atomically.
        self._script = client.register_script(_LUA_SOURCE)

    async def check(self, client_id: str, limit: int, window_ms: int) -> RateLimitResult:
        """Record a request against `client_id` and say whether it is allowed."""
        # A unique member per request, so two requests landing in the same
        # millisecond are two entries in the log rather than one overwriting
        # the other.
        member = uuid.uuid4().hex

        allowed, remaining, retry_after_ms = await self._script(
            keys=[f"{KEY_PREFIX}:{client_id}"],
            args=[window_ms, limit, member],
        )

        return RateLimitResult(
            allowed=bool(allowed),
            limit=limit,
            remaining=int(remaining),
            retry_after_ms=int(retry_after_ms),
        )
