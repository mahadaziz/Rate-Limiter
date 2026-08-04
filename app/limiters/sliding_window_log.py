"""Sliding window log: exact, at the cost of remembering every request."""

import uuid

from app.limiters.base import Limiter, RateLimitResult, load_lua, metrics_key


class SlidingWindowLogLimiter(Limiter):
    """Keeps one Redis sorted set entry per accepted request.

    Exact for any window you care to ask about, and never lets a client exceed
    `limit` in a rolling `window_ms`. The cost is memory: one entry per request
    per window, so a client on a limit of 100k is 100k sorted set members.
    """

    name = "sliding_window_log"
    state_prefix = "ratelimit"

    def __init__(self, client) -> None:
        super().__init__(client)
        # register_script sends the body once and then calls it by SHA,
        # re-sending it automatically if Redis has forgotten it. Either way the
        # server runs the whole script atomically.
        self._script = client.register_script(load_lua("sliding_window_log"))

    async def _check(self, client_id: str, limit: int, window_ms: int) -> RateLimitResult:
        # A unique member per request, so two requests landing in the same
        # millisecond are two entries in the log rather than one overwriting
        # the other.
        member = uuid.uuid4().hex

        allowed, remaining, retry_after_ms = await self._script(
            keys=[self.state_key(client_id), metrics_key(client_id)],
            args=[window_ms, limit, member],
        )

        return RateLimitResult(
            allowed=bool(allowed),
            limit=limit,
            remaining=int(remaining),
            retry_after_ms=int(retry_after_ms),
        )

    async def current_usage(
        self, client_id: str, limit: int, window_ms: int, now_ms: int
    ) -> int:
        # Count only what is still inside the window. The script prunes lazily,
        # so aged-out entries can still be sitting in the sorted set and a
        # plain ZCARD would over-report.
        return await self._client.zcount(
            self.state_key(client_id), now_ms - window_ms, "+inf"
        )
