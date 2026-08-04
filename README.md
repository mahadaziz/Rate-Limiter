# Distributed Rate Limiter

A rate limiter that holds a single shared limit across several application
instances, built with FastAPI and Redis.

The interesting part is not the counting. It is that three separate processes,
each handling requests for the same client at the same time, agree on one
number without coordinating with each other.

## Architecture

```
                    ┌──────────────┐
   client ────────► │ nginx :8080  │  round robin
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
       ┌─────────┐    ┌─────────┐    ┌─────────┐
       │  app-1  │    │  app-2  │    │  app-3  │   FastAPI
       └────┬────┘    └────┬────┘    └────┬────┘
            │              │              │
            └──────────────┼──────────────┘
                           ▼
                     ┌───────────┐
                     │   Redis   │   sliding window log
                     └───────────┘     + Lua script
```

No instance keeps any limit state in memory. All of it is in Redis, and the
decision is made by a Lua script that Redis runs atomically. An instance is
therefore interchangeable with any other, and adding a fourth changes nothing
about correctness.

nginx uses plain round robin on purpose. Sticky sessions would send each client
to one instance and hide the problem this project is about.

## Running it

```bash
docker compose up -d --build
curl -s http://localhost:8080/health
```

Everything enters through the load balancer on port 8080. The app instances do
not publish ports of their own.

```bash
# free tier: 10 requests per minute
for i in $(seq 1 12); do
  curl -s -o /dev/null -w "%{http_code} " \
    -H "X-API-Key: free-key-acme" http://localhost:8080/limited
done
# 200 200 200 200 200 200 200 200 200 200 429 429
```

Tear down with `docker compose down`.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /limited` | The rate limited endpoint. Requires `X-API-Key`. |
| `GET /health` | Whether this instance can reach Redis. Always 200; the body says `ok` or `degraded`. |
| `GET /metrics` | Per-client allowed/denied totals, shared across instances. |
| `POST /metrics/reset` | Zero the counters, for repeated load test runs. |

`/limited` responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Tier` and `X-Instance`, plus `Retry-After` on a 429.

`/health` and `/metrics` are unauthenticated operational endpoints. Behind a
real edge they would sit on an internal listener rather than the public one.

## Clients and tiers

Two tiers, four demo keys, held in a dict in `app/clients.py`:

| API key | Client | Tier | Limit |
| --- | --- | --- | --- |
| `free-key-acme` | acme | free | 10 / min |
| `free-key-globex` | globex | free | 10 / min |
| `pro-key-initech` | initech | pro | 100 / min |
| `pro-key-umbrella` | umbrella | pro | 100 / min |

An unknown or missing key gets a 401 rather than a default limit. If anonymous
callers were allowed through, dropping the header would be a way around
whatever tier the key carries.

`lookup()` is the seam a real user store would slot into.

## How the limiting works

A sliding window log. Each client has a Redis sorted set holding one member per
accepted request, scored by the millisecond it arrived. A request is allowed if,
after dropping the members that have aged out, the set holds fewer entries than
the client's limit.

Compared to a fixed-window counter, this costs more memory but does not let a
client spend a full limit at the end of one window and another full limit at the
start of the next. Capacity comes back gradually, one request at a time, as each
entry ages out.

The whole check-and-increment is one Lua script (`app/lua/sliding_window_log.lua`),
which Redis executes as a single atomic unit:

```lua
redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now_ms, member)
    redis.call('PEXPIRE', key, window_ms)
    ...
```

Doing this as separate round trips is the bug the project exists to avoid. Every
concurrent request between the read and the write sees the same stale count and
concludes it is under the limit. `scripts/demo_race.py` shows exactly that: at
200 concurrent requests against a limit of 50, the non-atomic version lets all
200 through while the script holds at 50.

Three details worth calling out:

**The clock comes from Redis.** The script calls `TIME` itself rather than
trusting a timestamp from the caller. Three instances have three clocks, and a
skewed one would compute a different window boundary for the same client. One
clock on the shared server removes the question. This is safe because Redis
replicates script *effects* rather than the script itself; on Redis 4 or older
it would have needed `replicate_commands()`.

**Keys use cluster hash tags.** `ratelimit:{acme}` and `metrics:{acme}` — only
what is inside the braces is hashed to a slot, so a client's window and its
counters always live on the same node. A multi-key script requires that under
Redis Cluster. On a single Redis the braces are just characters.

**The counters are bumped inside the script.** Allowed and denied are
incremented in the same atomic block that made the decision, so the metrics
cannot disagree with what actually happened.

## When Redis is down

The limiter fails open: the request is allowed through uncounted, the response
carries `X-RateLimit-Degraded: true`, and every fallback is logged at ERROR.

```
ERROR [app-2] app.limiter: redis unreachable, failing open for client=acme
(TimeoutError: Timeout connecting to server); fallbacks on this instance: 5
```

The tradeoff: **fail open** trades correctness of the limit for availability of
the service behind it. A limiter outage degrades into no limiting rather than a
total outage. That is right when the limiter protects against accidental
overuse by known, authenticated clients — the failure mode is a bill, and the
alternative is taking the whole API down because a supporting service is sick.

**Fail closed** is right when the limiter is the thing standing between an
unauthenticated internet and something expensive or fragile, where the failure
mode is not a bill but a breach or a collapse. Under those conditions rejecting
traffic you cannot account for beats admitting it.

This project fails open because its clients are authenticated and tiered. Note
what does *not* degrade: API key checks still return 401 while Redis is down.
Authentication and rate limiting fail in different directions, deliberately.

`REDIS_TIMEOUT_SECONDS` (default 0.5) bounds how long a degraded request waits.
Without it, a hung Redis would turn every request into a hang rather than a
fast pass-through.

## Verifying it

Four scripts, all run inside a container that already has the dependencies.

```bash
# the limit holds exactly under concurrency, on one instance
docker compose exec app-1 python -m scripts.verify_atomicity

# capacity returns gradually, so the window really slides
docker compose exec app-1 python -m scripts.verify_sliding_window

# the negative control: what a non-atomic limiter does instead
docker compose exec app-1 python -m scripts.demo_race

# the real test: concurrent load across all three instances
docker compose exec app-1 python -m scripts.loadtest
```

The load test fires 3x each client's limit, from four clients at once, through
the load balancer:

```
660 requests across 4 clients, 3x each client's limit, all fired at once

instances that served traffic:
  app-1: 220
  app-2: 220
  app-3: 220

  client     tier   limit    200    429  other  result
  ----------------------------------------------------
  acme       free      10     10     20      0  PASS
  globex     free      10     10     20      0  PASS
  initech    pro      100    100    200      0  PASS
  umbrella   pro      100    100    200      0  PASS

  server counters vs what the clients observed:
    acme       server   10/20   clients   10/20    agree
    ...

PASS: every client got exactly its limit, across all instances.
```

It checks for exact equality in both directions. More than the limit means the
atomicity broke; fewer means a client was throttled early. It also cross-checks
what the clients observed against what the server counted, so two independent
views have to agree, and it fails if fewer than three instances served traffic —
otherwise the run would not have exercised the distributed path at all.

## Layout

```
app/
  main.py           routes: identify caller, ask limiter, render HTTP
  limiter.py        the rate limiting decision
  lua/
    sliding_window_log.lua    the atomic check-and-increment
  clients.py        API keys and tier definitions
  metrics.py        reads the counters the script writes
  config.py         environment settings
  redis_client.py   one connection pool per process
scripts/            verification and load testing
nginx/nginx.conf    round robin across the three instances
```

## What would change for production

- Client and tier config would come from a real store, behind `lookup()` and
  cached, rather than a dict.
- Counters would go to Prometheus or similar rather than a JSON endpoint;
  the current ones grow without bound and have no TTL.
- Redis would be replicated. A single instance is a single point of failure,
  which today means the fail-open path is one restart away.
- The sorted set holds one entry per request per window, so a very high limit
  costs real memory. Above some threshold an approximate algorithm such as a
  sliding window counter would be the better trade.
