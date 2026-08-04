"""Behaviour specific to the token bucket."""

from tests.helpers import send, wait_until


async def test_state_is_two_fields_regardless_of_limit(token_bucket):
    """The memory win over the log: state does not grow with the limit."""
    await send(token_bucket, 50, 1000, 60_000)

    state = await token_bucket.client.hgetall(token_bucket.state_key("c"))
    assert set(state) == {"tokens", "updated_ms"}


async def test_an_unseen_client_starts_full(token_bucket):
    assert await send(token_bucket, 5, 5, 60_000) == 5


async def test_refills_continuously(token_bucket):
    """Waiting half a window returns roughly half the capacity.

    This is the visible difference from the log version, where capacity comes
    back only as individual entries age out.
    """
    capacity, window = 8, 1500

    assert await send(token_bucket, capacity, capacity, window) == capacity
    assert await send(token_bucket, 1, capacity, window) == 0

    t0 = await token_bucket.now_ms()
    await wait_until(token_bucket, t0, window // 2)

    refilled = await send(token_bucket, capacity, capacity, window)
    # About half, with a token of slack for the time spent issuing requests.
    assert abs(refilled - capacity // 2) <= 1


async def test_idling_does_not_bank_more_than_capacity(token_bucket):
    """A bucket left alone caps out rather than accumulating a huge burst."""
    capacity, window = 5, 400

    await send(token_bucket, capacity, capacity, window)

    # Idle for several windows' worth of refill.
    t0 = await token_bucket.now_ms()
    await wait_until(token_bucket, t0, window * 3)

    assert await send(token_bucket, capacity * 3, capacity, window) == capacity


async def test_retry_after_is_about_one_token_of_refill(token_bucket):
    capacity, window = 10, 2000

    await send(token_bucket, capacity, capacity, window)
    result = await token_bucket.check("c", capacity, window)

    assert not result.allowed
    # One token is window/capacity milliseconds of refill; allow slack for the
    # fraction already accumulated while the burst was issued.
    assert 0 < result.retry_after_ms <= window // capacity + 50
