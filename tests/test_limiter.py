"""The rate limiting decision itself."""

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.limiter import RateLimiter, metrics_key, window_key

WINDOW_MS = 60_000


async def redis_now_ms(client) -> int:
    seconds, micros = await client.time()
    return seconds * 1000 + micros // 1000


async def test_allows_up_to_the_limit(limiter):
    results = [await limiter.check("c", 5, WINDOW_MS) for _ in range(5)]
    assert all(r.allowed for r in results)


async def test_denies_past_the_limit(limiter):
    for _ in range(5):
        await limiter.check("c", 5, WINDOW_MS)

    result = await limiter.check("c", 5, WINDOW_MS)
    assert not result.allowed
    assert result.remaining == 0


async def test_remaining_counts_down(limiter):
    remaining = [(await limiter.check("c", 5, WINDOW_MS)).remaining for _ in range(5)]
    assert remaining == [4, 3, 2, 1, 0]


async def test_clients_do_not_share_a_budget(limiter):
    for _ in range(5):
        await limiter.check("noisy", 5, WINDOW_MS)

    assert not (await limiter.check("noisy", 5, WINDOW_MS)).allowed
    assert (await limiter.check("quiet", 5, WINDOW_MS)).allowed


async def test_denied_request_reports_when_to_retry(limiter):
    for _ in range(2):
        await limiter.check("c", 2, WINDOW_MS)

    result = await limiter.check("c", 2, WINDOW_MS)
    assert not result.allowed
    # The oldest entry has only just been written, so the wait should be most
    # of a window.
    assert 0 < result.retry_after_ms <= WINDOW_MS


async def test_window_key_expires_on_its_own(limiter, redis_client):
    await limiter.check("c", 5, WINDOW_MS)

    ttl = await redis_client.pttl(window_key("c"))
    assert 0 < ttl <= WINDOW_MS


async def test_holds_the_limit_exactly_under_concurrency(limiter):
    """The headline claim.

    200 requests in flight at once against a limit of 50. If the check and the
    increment were separate round trips, requests would read the same stale
    count between them and more than 50 would get through.
    """
    results = await asyncio.gather(
        *(limiter.check("c", 50, WINDOW_MS) for _ in range(200))
    )

    allowed = [r for r in results if r.allowed]
    assert len(allowed) == 50

    # Every allowed request should have been handed a distinct slot; a repeat
    # would mean two of them saw the same pre-write state.
    assert sorted(r.remaining for r in allowed) == list(range(50))


async def test_window_slides_instead_of_resetting(limiter, redis_client):
    """Capacity returns gradually, not all at once.

    A fixed window would give back the whole limit at a boundary. Here only the
    entries that have actually aged out free up a slot.
    """
    limit, window = 4, 1500
    half = limit // 2
    t0 = await redis_now_ms(redis_client)

    async def elapsed() -> int:
        return await redis_now_ms(redis_client) - t0

    async def send(n: int) -> int:
        results = [await limiter.check("c", limit, window) for _ in range(n)]
        return sum(1 for r in results if r.allowed)

    async def wait_until(ms: int) -> None:
        while await elapsed() < ms:
            await asyncio.sleep(0.02)

    assert await send(half) == half

    # Second half lands mid-window, so the two halves age out at clearly
    # separate times.
    await wait_until(window // 2)
    assert await send(half) == half

    # Now full.
    assert await send(1) == 0

    # Past the first half's expiry, well short of the second's.
    await wait_until(int(window * 1.2))
    assert await send(limit) == half


async def test_counters_agree_with_the_decisions(limiter, redis_client):
    for _ in range(7):
        await limiter.check("c", 5, WINDOW_MS)

    counters = await redis_client.hgetall(metrics_key("c"))
    assert int(counters["allowed"]) == 5
    assert int(counters["denied"]) == 2


async def test_fails_open_when_redis_is_unreachable(limiter):
    """A limiter outage must not become a service outage."""

    async def unreachable(*args, **kwargs):
        raise RedisConnectionError("simulated outage")

    limiter._script = unreachable

    result = await limiter.check("c", 1, WINDOW_MS)

    assert result.allowed
    assert result.degraded
    assert limiter.fallback_count == 1


async def test_fallbacks_are_counted(limiter):
    async def unreachable(*args, **kwargs):
        raise RedisConnectionError("simulated outage")

    limiter._script = unreachable

    for _ in range(3):
        await limiter.check("c", 1, WINDOW_MS)

    assert limiter.fallback_count == 3


async def test_failing_open_ignores_the_limit(limiter):
    """Even a client already at its limit gets through while Redis is down."""
    for _ in range(2):
        await limiter.check("c", 2, WINDOW_MS)
    assert not (await limiter.check("c", 2, WINDOW_MS)).allowed

    async def unreachable(*args, **kwargs):
        raise RedisConnectionError("simulated outage")

    limiter._script = unreachable

    assert (await limiter.check("c", 2, WINDOW_MS)).allowed


def test_keys_share_a_cluster_hash_tag():
    """Window and counters must land on the same node under Redis Cluster.

    Only what is inside the braces is hashed to a slot, so both keys have to
    carry the same tag for the multi-key script to be legal.
    """
    assert "{acme}" in window_key("acme")
    assert "{acme}" in metrics_key("acme")
