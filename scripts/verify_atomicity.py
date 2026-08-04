"""Prove the check-and-increment is atomic on a single instance.

Fires many concurrent requests for one client at a limit well below that count
and asserts exactly `LIMIT` of them were allowed. If the check and the write
were separate round trips, several requests would read "under limit" before any
of them wrote, and the allowed count would come out above the limit.

Run inside the app container:

    docker compose exec app-1 python -m scripts.verify_atomicity
"""

import asyncio
import sys

from app.limiter import RateLimiter, window_key
from app.redis_client import close_client, create_client

CONCURRENCY = 200
LIMIT = 50
WINDOW_MS = 60_000
ROUNDS = 5


async def run_round(client, limiter: RateLimiter, round_no: int) -> bool:
    client_id = f"atomicity-test-{round_no}"
    key = window_key(client_id)
    await client.delete(key)

    results = await asyncio.gather(
        *(limiter.check(client_id, LIMIT, WINDOW_MS) for _ in range(CONCURRENCY))
    )

    allowed = [r for r in results if r.allowed]
    logged = await client.zcard(key)
    # Each allowed request should have been handed its own slot in the window,
    # so the remaining counts should be exactly LIMIT-1 down to 0 with no
    # repeats. A duplicate means two requests saw the same pre-write state.
    remainings = sorted(r.remaining for r in allowed)

    ok = (
        len(allowed) == LIMIT
        and logged == LIMIT
        and remainings == list(range(LIMIT))
    )

    print(
        f"round {round_no}: {len(allowed)}/{CONCURRENCY} allowed "
        f"(expected {LIMIT}), log size {logged}, "
        f"distinct slots {'yes' if remainings == list(range(LIMIT)) else 'NO'} "
        f"-> {'PASS' if ok else 'FAIL'}"
    )

    await client.delete(key)
    return ok


async def main() -> int:
    client = create_client()
    limiter = RateLimiter(client)

    print(
        f"{CONCURRENCY} concurrent requests per round, limit {LIMIT}, "
        f"{ROUNDS} rounds\n"
    )
    try:
        outcomes = [await run_round(client, limiter, n) for n in range(1, ROUNDS + 1)]
    finally:
        await close_client()

    if all(outcomes):
        print("\nPASS: the limit held exactly on every round.")
        return 0

    print("\nFAIL: the limit was not held; the check-and-increment is not atomic.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
