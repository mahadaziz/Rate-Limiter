"""Show the race that the Lua script exists to prevent.

This is a negative control. `verify_atomicity` passing only means something if
the same test can fail, so here is the same workload against a limiter written
the obvious wrong way: read the count, decide, then write, as three separate
round trips instead of one atomic script.

Between the read and the write there is a window where every other concurrent
request sees the same stale count and concludes it is also under the limit. The
allowed total comes out above the limit, by more the more concurrency there is.

Nothing here is used by the application. Run it to see the failure mode:

    docker compose exec app-1 python -m scripts.demo_race
"""

import asyncio
import sys
import uuid

from app.limiters import SlidingWindowLogLimiter
from app.redis_client import close_client, create_client

CONCURRENCY = 200
LIMIT = 50
WINDOW_MS = 60_000


async def naive_check(client, key: str, limit: int, window_ms: int) -> bool:
    """The wrong way: check and increment as separate operations."""
    seconds, micros = await client.time()
    now_ms = seconds * 1000 + micros // 1000

    await client.zremrangebyscore(key, "-inf", now_ms - window_ms)
    count = await client.zcard(key)

    # Every concurrent request that got here before any of them wrote sees the
    # same count, so they all decide they are under the limit.
    if count >= limit:
        return False

    await client.zadd(key, {uuid.uuid4().hex: now_ms})
    await client.pexpire(key, window_ms)
    return True


async def main() -> int:
    client = create_client()

    atomic = SlidingWindowLogLimiter(client)
    naive_key = atomic.state_key("race-demo-naive")
    await client.delete(naive_key)
    naive_results = await asyncio.gather(
        *(naive_check(client, naive_key, LIMIT, WINDOW_MS) for _ in range(CONCURRENCY))
    )
    naive_allowed = sum(naive_results)

    atomic_id = "race-demo-atomic"
    await client.delete(atomic.state_key(atomic_id))
    atomic_results = await asyncio.gather(
        *(atomic.check(atomic_id, LIMIT, WINDOW_MS) for _ in range(CONCURRENCY))
    )
    atomic_allowed = sum(1 for r in atomic_results if r.allowed)

    await client.delete(naive_key, atomic.state_key(atomic_id))
    await close_client()

    print(f"{CONCURRENCY} concurrent requests, limit {LIMIT}\n")
    print(f"  separate INCR/EXPIRE style calls: {naive_allowed} allowed "
          f"({naive_allowed - LIMIT:+d} vs the limit)")
    print(f"  single atomic Lua script:         {atomic_allowed} allowed "
          f"({atomic_allowed - LIMIT:+d} vs the limit)")

    if naive_allowed > LIMIT and atomic_allowed == LIMIT:
        print("\nAs expected: the non-atomic version overshoots, the script does not.")
        return 0

    if atomic_allowed != LIMIT:
        print("\nUNEXPECTED: the atomic version did not hold the limit.")
        return 1

    # Possible on a fast enough machine with little real interleaving.
    print("\nThe non-atomic version happened not to overshoot this time; "
          "rerun or raise CONCURRENCY to see it.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
