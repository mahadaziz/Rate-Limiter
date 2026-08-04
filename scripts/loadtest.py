"""Concurrent load test against the load balancer.

Fires far more requests than each client is allowed, all at once, from several
simulated clients at the same time, and checks that every client got through
exactly its limit and no more.

This is the test the whole design exists to pass. Requests are spread across
three instances by nginx, so if the check and the increment were not a single
atomic operation, several instances would read "under limit" from the same
stale state and let more than `limit` requests through. Undercounting would
show up too: a client that got fewer than its limit was wrongly throttled.

Two independent views have to agree at the end: what the clients observed on
their responses, and what the server counted in Redis.

Run inside the app container, which already has the dependencies:

    docker compose exec app-1 python -m scripts.loadtest
"""

import asyncio
import os
import sys
from collections import Counter

import aiohttp

from app.clients import registered_clients
from app.limiter import window_key
from app.metrics import reset as reset_metrics
from app.redis_client import close_client, create_client

# Inside the compose network the load balancer is reachable by service name.
TARGET = os.getenv("TARGET_URL", "http://lb:80")

# api key -> client id. Mirrors the registry in app.clients.
API_KEYS = {
    "free-key-acme": "acme",
    "free-key-globex": "globex",
    "pro-key-initech": "initech",
    "pro-key-umbrella": "umbrella",
}

# Requests per client, as a multiple of that client's limit.
OVERSHOOT = 3

# Cap on simultaneous sockets. High enough to create real contention, low
# enough not to just exhaust the local ephemeral port range.
MAX_CONNECTIONS = 200


async def fire(session: aiohttp.ClientSession, api_key: str) -> tuple[int, str]:
    """One request. Returns its status and the instance that served it."""
    async with session.get(
        f"{TARGET}/limited", headers={"X-API-Key": api_key}
    ) as resp:
        await resp.read()
        return resp.status, resp.headers.get("X-Instance", "?")


async def main() -> int:
    redis_client = create_client()

    # Start from a clean slate: empty windows and zeroed counters, so the
    # numbers below describe this run only.
    tiers = {c.client_id: c.tier for c in registered_clients()}
    await redis_client.delete(*(window_key(cid) for cid in tiers))
    await reset_metrics(redis_client)

    plan = []
    for api_key, client_id in API_KEYS.items():
        limit = tiers[client_id].limit
        plan.extend([api_key] * (limit * OVERSHOOT))

    print(f"target: {TARGET}")
    print(
        f"{len(plan)} requests across {len(API_KEYS)} clients, "
        f"{OVERSHOOT}x each client's limit, all fired at once\n"
    )

    connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        results = await asyncio.gather(*(fire(session, key) for key in plan))

    # What the clients saw.
    observed: dict[str, Counter] = {cid: Counter() for cid in tiers}
    instances: Counter = Counter()
    for api_key, (status, instance) in zip(plan, results):
        observed[API_KEYS[api_key]][status] += 1
        instances[instance] += 1

    # What the server counted.
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(f"{TARGET}/metrics") as resp:
            server = (await resp.json())["clients"]

    await close_client()

    print("instances that served traffic:")
    for name, count in sorted(instances.items()):
        print(f"  {name}: {count}")
    print()

    header = f"  {'client':10} {'tier':5} {'limit':>6} {'200':>6} {'429':>6} {'other':>6}  result"
    print(header)
    print("  " + "-" * (len(header) - 2))

    ok = True
    for client_id, tier in sorted(tiers.items()):
        counts = observed[client_id]
        allowed = counts[200]
        denied = counts[429]
        other = sum(v for k, v in counts.items() if k not in (200, 429))

        # The exact-equality check is the point. Over the limit means the
        # atomicity broke; under it means a client was throttled early.
        passed = allowed == tier.limit and other == 0
        ok = ok and passed
        verdict = "PASS" if passed else ("FAIL over" if allowed > tier.limit else "FAIL under")
        print(
            f"  {client_id:10} {tier.name:5} {tier.limit:6} {allowed:6} "
            f"{denied:6} {other:6}  {verdict}"
        )

    print("\n  server counters vs what the clients observed:")
    for client_id in sorted(tiers):
        counts = observed[client_id]
        s = server[client_id]
        agrees = s["allowed"] == counts[200] and s["denied"] == counts[429]
        ok = ok and agrees
        print(
            f"    {client_id:10} server {s['allowed']:>4}/{s['denied']:<4} "
            f"clients {counts[200]:>4}/{counts[429]:<4}  "
            f"{'agree' if agrees else 'DISAGREE'}"
        )

    served_by_all = len([i for i in instances if i != "?"]) >= 3
    if not served_by_all:
        ok = False
        print("\n  WARNING: fewer than 3 instances served traffic; "
              "this did not exercise the distributed path")

    if ok:
        print("\nPASS: every client got exactly its limit, across all instances.")
        return 0

    print("\nFAIL: the limit did not hold.")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
