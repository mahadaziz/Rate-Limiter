"""Prove the check-and-increment is atomic, for every algorithm.

Fires many concurrent requests for one client at a limit well below that count
and asserts exactly `LIMIT` of them were allowed. If the check and the write
were separate round trips, several requests would read "under limit" before any
of them wrote, and the allowed count would come out above the limit.

Runs against both algorithms, since atomicity is a property of the interface
rather than of one implementation.

Run inside the app container:

    docker compose exec app-1 python -m scripts.verify_atomicity
"""

import asyncio
import sys

from app.limiters import ALGORITHMS, Limiter, build_limiter
from app.redis_client import close_client, create_client

CONCURRENCY = 200
LIMIT = 50
WINDOW_MS = 60_000
ROUNDS = 3


async def run_round(limiter: Limiter, round_no: int) -> bool:
    client_id = f"atomicity-test-{limiter.name}-{round_no}"
    await limiter.client.delete(limiter.state_key(client_id))

    results = await asyncio.gather(
        *(limiter.check(client_id, LIMIT, WINDOW_MS) for _ in range(CONCURRENCY))
    )

    allowed = [r for r in results if r.allowed]
    now_ms = await limiter.now_ms()
    in_use = await limiter.current_usage(client_id, LIMIT, WINDOW_MS, now_ms)

    # Each allowed request should have been handed its own slot, so the
    # remaining counts should be exactly LIMIT-1 down to 0 with no repeats. A
    # duplicate means two requests saw the same pre-write state.
    remainings = sorted(r.remaining for r in allowed)
    distinct = remainings == list(range(LIMIT))

    ok = len(allowed) == LIMIT and in_use == LIMIT and distinct

    print(
        f"  round {round_no}: {len(allowed)}/{CONCURRENCY} allowed "
        f"(expected {LIMIT}), usage {in_use}, "
        f"distinct slots {'yes' if distinct else 'NO'} "
        f"-> {'PASS' if ok else 'FAIL'}"
    )

    await limiter.client.delete(limiter.state_key(client_id))
    return ok


async def main() -> int:
    client = create_client()

    print(
        f"{CONCURRENCY} concurrent requests per round, limit {LIMIT}, "
        f"{ROUNDS} rounds\n"
    )

    outcomes = []
    try:
        for name in sorted(ALGORITHMS):
            print(f"{name}:")
            limiter = build_limiter(name, client)
            for n in range(1, ROUNDS + 1):
                outcomes.append(await run_round(limiter, n))
            print()
    finally:
        await close_client()

    if all(outcomes):
        print("PASS: every algorithm held the limit exactly on every round.")
        return 0

    print("FAIL: the limit was not held; the check-and-increment is not atomic.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
