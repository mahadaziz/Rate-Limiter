"""Small helpers shared by the test modules."""


async def send(limiter, n: int, limit: int, window_ms: int, client_id: str = "c") -> int:
    """Issue n sequential requests, return how many were allowed.

    A plain loop rather than a comprehension: a generator expression containing
    `await` is an async generator, which `sum` cannot consume.
    """
    allowed = 0
    for _ in range(n):
        if (await limiter.check(client_id, limit, window_ms)).allowed:
            allowed += 1
    return allowed


async def wait_until(limiter, t0: int, ms: int) -> None:
    """Block until the Redis clock says `ms` have passed since `t0`."""
    import asyncio

    while await limiter.now_ms() - t0 < ms:
        await asyncio.sleep(0.02)
