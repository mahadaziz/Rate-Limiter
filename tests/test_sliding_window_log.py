"""Behaviour specific to the sliding window log."""

from tests.helpers import send, wait_until


async def test_window_slides_instead_of_resetting(sliding_window):
    """Capacity returns gradually, not all at once.

    A fixed window would give back the whole limit at a boundary. Here only the
    entries that have actually aged out free up a slot.
    """
    limiter = sliding_window
    limit, window = 4, 1500
    half = limit // 2
    t0 = await limiter.now_ms()

    assert await send(limiter, half, limit, window) == half

    # Second half lands mid-window, so the two halves age out at clearly
    # separate times.
    await wait_until(limiter, t0, window // 2)
    assert await send(limiter, half, limit, window) == half

    # Now full.
    assert await send(limiter, 1, limit, window) == 0

    # Past the first half's expiry, well short of the second's.
    await wait_until(limiter, t0, int(window * 1.2))
    assert await send(limiter, limit, limit, window) == half


async def test_keeps_one_entry_per_accepted_request(sliding_window):
    """The memory cost that pays for exactness."""
    await send(sliding_window, 6, 10, 60_000)

    assert await sliding_window.client.zcard(sliding_window.state_key("c")) == 6


async def test_denied_requests_are_not_logged(sliding_window):
    """Only accepted requests occupy the window."""
    await send(sliding_window, 5, 3, 60_000)

    assert await sliding_window.client.zcard(sliding_window.state_key("c")) == 3


async def test_never_exceeds_the_limit_across_a_boundary(sliding_window):
    """The property a fixed window does not have.

    Spend the whole limit, wait just past the window, spend it again, and check
    that the window never holds more than the limit.
    """
    limiter = sliding_window
    limit, window = 3, 800

    t0 = await limiter.now_ms()
    assert await send(limiter, limit, limit, window) == limit

    # Just past expiry of the first batch.
    await wait_until(limiter, t0, window + 100)

    assert await send(limiter, limit, limit, window) == limit

    # Whatever is inside the window right now must still respect the limit.
    now_ms = await limiter.now_ms()
    assert await limiter.current_usage("c", limit, window, now_ms) <= limit
