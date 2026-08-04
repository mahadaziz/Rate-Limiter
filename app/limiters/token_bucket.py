"""Token bucket: constant memory, at the cost of allowing bursts."""

from app.limiters.base import Limiter, RateLimitResult, load_lua, metrics_key


class TokenBucketLimiter(Limiter):
    """Keeps two numbers per client: tokens left, and when they were counted.

    `limit` is read as both the bucket capacity and the tokens refilled per
    `window_ms`, so the sustained rate matches the log version. The behaviour
    differs at the edges: a client that has been idle arrives with a full
    bucket and can spend it at once, then continues at the refill rate. Across
    a rolling window that straddles such a burst, a client can therefore be
    served more than `limit` requests, which the log version never permits.

    In exchange the state is two hash fields regardless of how large the limit
    is, where the log version is one sorted set entry per request.
    """

    name = "token_bucket"
    state_prefix = "bucket"

    def __init__(self, client) -> None:
        super().__init__(client)
        self._script = client.register_script(load_lua("token_bucket"))

    async def _check(self, client_id: str, limit: int, window_ms: int) -> RateLimitResult:
        allowed, remaining, retry_after_ms = await self._script(
            keys=[self.state_key(client_id), metrics_key(client_id)],
            args=[window_ms, limit],
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
        """Tokens spent, i.e. capacity minus what is in the bucket right now.

        Recomputes the refill the same way the script does, so a bucket that
        has been sitting idle reports as recovered rather than as however it
        looked when it was last written.
        """
        tokens, updated_ms = await self._client.hmget(
            self.state_key(client_id), "tokens", "updated_ms"
        )

        if tokens is None or updated_ms is None:
            return 0

        rate = limit / window_ms
        elapsed = max(0, now_ms - float(updated_ms))
        current = min(limit, float(tokens) + elapsed * rate)

        return max(0, limit - int(current))
