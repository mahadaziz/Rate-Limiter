"""Show how the token bucket differs from the sliding window log.

Two properties the log version does not have:

  - capacity refills continuously, so waiting half a window returns roughly
    half the limit rather than however many individual requests aged out;
  - an idle client arrives with a full bucket and can spend it in one burst.

The second is the tradeoff. Across a window that straddles a burst, a token
bucket client can be served more than `limit` requests, which the log version
never allows. What it buys is O(1) state per client instead of one entry per
request.

Run inside the app container:

    docker compose exec app-1 python -m scripts.verify_token_bucket
"""

import asyncio
import sys

from app.limiters import TokenBucketLimiter
from app.redis_client import close_client, create_client

CAPACITY = 8
WINDOW_MS = 2000


async def main() -> int:
    client = create_client()
    limiter = TokenBucketLimiter(client)
    client_id = "token-bucket-demo"
    await client.delete(limiter.state_key(client_id))

    t0 = await limiter.now_ms()

    async def elapsed() -> int:
        return await limiter.now_ms() - t0

    async def send(n: int) -> int:
        results = [await limiter.check(client_id, CAPACITY, WINDOW_MS) for _ in range(n)]
        return sum(1 for r in results if r.allowed)

    async def wait_until(ms: int) -> None:
        while await elapsed() < ms:
            await asyncio.sleep(0.02)

    checks = []

    # A client that has never been seen starts full and can spend it at once.
    burst = await send(CAPACITY)
    checks.append(("idle client can spend a full bucket", burst == CAPACITY,
                   f"{burst}/{CAPACITY} at +{await elapsed()}ms"))

    empty = await send(1)
    checks.append(("empty bucket denies", empty == 0, f"allowed {empty}"))

    # Half a window of refill should be worth about half the capacity. The
    # tolerance covers the milliseconds spent issuing the requests.
    await wait_until(WINDOW_MS // 2)
    at = await elapsed()
    refilled = await send(CAPACITY)
    expected = CAPACITY // 2
    close = abs(refilled - expected) <= 1
    checks.append((
        "half a window refills about half the bucket",
        close,
        f"{refilled} allowed at +{at}ms, expected about {expected}",
    ))

    # Long idle: the bucket caps at capacity rather than banking more.
    await wait_until(WINDOW_MS * 3)
    at = await elapsed()
    capped = await send(CAPACITY * 2)
    checks.append((
        "a long idle period does not bank more than capacity",
        capped == CAPACITY,
        f"{capped} allowed at +{at}ms after idling {WINDOW_MS * 3 - WINDOW_MS // 2}ms, "
        f"capacity is {CAPACITY}",
    ))

    state = await client.hgetall(limiter.state_key(client_id))
    checks.append((
        "state is O(1) per client",
        set(state) == {"tokens", "updated_ms"},
        f"fields: {sorted(state)}",
    ))

    await client.delete(limiter.state_key(client_id))
    await close_client()

    print(f"capacity {CAPACITY}, window {WINDOW_MS}ms\n")
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    if all(ok for _, ok, _ in checks):
        print("\nPASS: the bucket refills continuously and caps at capacity.")
        return 0

    print("\nFAIL: the bucket did not behave as expected.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
