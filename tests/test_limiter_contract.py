"""Behaviour every algorithm must share.

The `limiter` fixture is parametrised over all registered algorithms, so each
of these runs once per implementation. If a new algorithm is added and any of
these fail, it does not satisfy the interface.
"""

import asyncio

from redis.exceptions import ConnectionError as RedisConnectionError

from app.limiters import ALGORITHMS, metrics_key, tagged_key

WINDOW_MS = 60_000


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
    assert 0 < result.retry_after_ms <= WINDOW_MS


async def test_state_expires_on_its_own(limiter):
    """Idle clients must not leak a key each."""
    await limiter.check("c", 5, WINDOW_MS)

    ttl = await limiter.client.pttl(limiter.state_key("c"))
    assert ttl > 0


async def test_holds_the_limit_exactly_under_concurrency(limiter):
    """The headline claim, required of every algorithm.

    200 requests in flight at once against a limit of 50. If the check and the
    write were separate round trips, requests would read the same stale state
    between them and more than 50 would get through.
    """
    results = await asyncio.gather(
        *(limiter.check("c", 50, WINDOW_MS) for _ in range(200))
    )

    allowed = [r for r in results if r.allowed]
    assert len(allowed) == 50

    # Every allowed request should have been handed a distinct slot; a repeat
    # would mean two of them saw the same pre-write state.
    assert sorted(r.remaining for r in allowed) == list(range(50))


async def test_usage_reflects_what_was_spent(limiter):
    for _ in range(3):
        await limiter.check("c", 10, WINDOW_MS)

    now_ms = await limiter.now_ms()
    assert await limiter.current_usage("c", 10, WINDOW_MS, now_ms) == 3


async def test_usage_is_zero_for_an_unseen_client(limiter):
    now_ms = await limiter.now_ms()
    assert await limiter.current_usage("never-seen", 10, WINDOW_MS, now_ms) == 0


async def test_counters_agree_with_the_decisions(limiter):
    for _ in range(7):
        await limiter.check("c", 5, WINDOW_MS)

    counters = await limiter.client.hgetall(metrics_key("c"))
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


def test_algorithms_use_distinct_state_prefixes():
    """Switching algorithm must not read the other one's state as its own."""
    prefixes = [cls.state_prefix for cls in ALGORITHMS.values()]
    assert len(prefixes) == len(set(prefixes))


def test_keys_share_a_cluster_hash_tag(limiter):
    """State and counters must land on the same node under Redis Cluster.

    Only what is inside the braces is hashed to a slot, so both keys have to
    carry the same tag for the multi-key script to be legal.
    """
    assert "{acme}" in limiter.state_key("acme")
    assert "{acme}" in metrics_key("acme")
    assert tagged_key("x", "acme") == "x:{acme}"
