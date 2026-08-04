"""The contract every limiting algorithm implements.

Two things live here rather than in the algorithms, because they must not vary
between them: how a client's Redis keys are named, and what happens when Redis
cannot be reached. An algorithm that got its own fail-open behaviour would be a
way for the service to behave differently under failure depending on a config
flag, which is the last place you want a surprise.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import NamedTuple

import redis.asyncio as redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

_LUA_DIR = Path(__file__).parent.parent / "lua"

METRICS_PREFIX = "metrics"


def tagged_key(prefix: str, client_id: str) -> str:
    """Namespace a key and pin it to one Redis Cluster slot.

    The braces are a cluster hash tag: only what is inside them is hashed, so
    every key for a client lands on the same node. A script touching more than
    one key requires that. On a single Redis they are just characters.
    """
    return f"{prefix}:{{{client_id}}}"


def metrics_key(client_id: str) -> str:
    return tagged_key(METRICS_PREFIX, client_id)


def load_lua(name: str) -> str:
    return (_LUA_DIR / f"{name}.lua").read_text()


class RateLimitResult(NamedTuple):
    allowed: bool
    limit: int
    remaining: int
    retry_after_ms: int
    # True when Redis could not be reached and the request was let through
    # without being counted.
    degraded: bool = False


class Limiter(ABC):
    """A rate limiting algorithm.

    Subclasses implement `_check`, which may assume Redis is reachable and is
    free to raise if it is not. `check` is what callers use, and it is where
    the failure policy is applied.
    """

    #: Name this algorithm is selected by.
    name: str
    #: Key prefix for whatever state the algorithm keeps per client.
    state_prefix: str

    def __init__(self, client: redis.Redis) -> None:
        self._client = client

        # Per-instance count of requests let through because Redis was
        # unreachable. Deliberately not stored in Redis: the one moment this
        # number matters is the moment Redis cannot be written to.
        self.fallback_count = 0

    @property
    def client(self) -> redis.Redis:
        return self._client

    def state_key(self, client_id: str) -> str:
        return tagged_key(self.state_prefix, client_id)

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
        try:
            return await self._check(client_id, limit, window_ms)
        except (RedisError, OSError) as exc:
            self.fallback_count += 1
            logger.error(
                "redis unreachable, failing open for client=%s using %s "
                "(%s: %s); fallbacks on this instance: %d",
                client_id,
                self.name,
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

    @abstractmethod
    async def _check(self, client_id: str, limit: int, window_ms: int) -> RateLimitResult:
        """Make the decision. May raise if Redis is unreachable."""

    @abstractmethod
    async def current_usage(
        self, client_id: str, limit: int, window_ms: int, now_ms: int
    ) -> int:
        """How much of the limit the client is currently holding.

        Read-only, for reporting. Each algorithm answers from its own state,
        so /metrics can describe either of them the same way.
        """

    async def now_ms(self) -> int:
        """The shared clock, taken from Redis rather than from this process."""
        seconds, micros = await self._client.time()
        return seconds * 1000 + micros // 1000
