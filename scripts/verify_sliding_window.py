"""Prove the window slides instead of resetting in fixed buckets.

A fixed-window counter forgets everything at once when its bucket rolls over,
which lets a client spend a full limit at the end of one bucket and another
full limit at the start of the next. A sliding log instead gives capacity back
one request at a time, as each individual entry ages out.

This drives that difference directly: fill half the window early, half of it
later, then wait long enough for only the early half to expire and check that
exactly that many slots came back.

Timing is measured from Redis rather than assumed, because `asyncio.sleep`
guarantees only a lower bound and a loaded host can overshoot it.

Run inside the app container:

    docker compose exec app-1 python -m scripts.verify_sliding_window
"""

import asyncio
import sys

from app.limiter import RateLimiter, window_key
from app.redis_client import close_client, create_client

LIMIT = 6
WINDOW_MS = 3000
HALF = LIMIT // 2


async def redis_now_ms(client) -> int:
    seconds, micros = await client.time()
    return seconds * 1000 + micros // 1000


async def main() -> int:
    client = create_client()
    limiter = RateLimiter(client)
    client_id = "sliding-window-test"
    key = window_key(client_id)
    await client.delete(key)

    async def send(n: int) -> int:
        """Send n requests, return how many were allowed."""
        results = [await limiter.check(client_id, LIMIT, WINDOW_MS) for _ in range(n)]
        return sum(1 for r in results if r.allowed)

    t0 = await redis_now_ms(client)

    async def elapsed() -> int:
        return await redis_now_ms(client) - t0

    checks = []

    early = await send(HALF)
    checks.append(("first half allowed", early == HALF, f"{early}/{HALF} at +{await elapsed()}ms"))

    # Land the second half around the middle of the window, far enough from the
    # first half that the two age out at clearly separate times.
    await asyncio.sleep(WINDOW_MS / 2 / 1000)
    late = await send(HALF)
    checks.append(("second half allowed", late == HALF, f"{late}/{HALF} at +{await elapsed()}ms"))

    # Now at capacity.
    over = await send(1)
    checks.append(("at capacity, next denied", over == 0, f"allowed {over} at +{await elapsed()}ms"))

    # Wait until the first half has aged out but the second half has not. The
    # first half expires at WINDOW_MS, the second at WINDOW_MS * 1.5.
    target = WINDOW_MS * 1.25
    while await elapsed() < target:
        await asyncio.sleep(0.05)

    at = await elapsed()
    # Entries are only pruned when the script next runs, so this counts the
    # aged-out ones too. It is the log before pruning, not the live window.
    before_prune = await client.zcard(key)
    recovered = await send(LIMIT)
    after = await client.zcard(key)
    checks.append(
        (
            "only the early half came back",
            recovered == HALF,
            f"{recovered} of {LIMIT} allowed at +{at}ms "
            f"(log held {before_prune} unpruned, {after} after pruning; "
            f"a fixed window would give back {LIMIT} or 0)",
        )
    )

    await client.delete(key)
    await close_client()

    print(f"limit {LIMIT}, window {WINDOW_MS}ms\n")
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    if all(ok for _, ok, _ in checks):
        print("\nPASS: capacity returned gradually, so the window slides.")
        return 0

    print("\nFAIL: the window did not slide as expected.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
