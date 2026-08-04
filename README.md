# Distributed Rate Limiter

[![CI](https://github.com/mahadaziz/Rate-Limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/mahadaziz/Rate-Limiter/actions/workflows/ci.yml)

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
                     │   Redis   │   all limit state,
                     └───────────┘   decided by a Lua script
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
| `GET /health` | Whether this instance can reach Redis, and which algorithm it runs. Always 200; the body says `ok` or `degraded`. |
| `GET /metrics` | Per-client allowed/denied totals, shared across instances. |
| `POST /metrics/reset` | Zero the counters, for repeated load test runs. |

`/limited` responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining`,
`X-RateLimit-Tier`, `X-RateLimit-Algorithm` and `X-Instance`, plus `Retry-After`
on a 429 and `X-RateLimit-Degraded` when Redis was unreachable.

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

Two algorithms are implemented behind one interface. Which runs is a
configuration choice, not a code change:

```bash
ALGORITHM=token_bucket docker compose up -d       # default: sliding_window_log
```

Both make the same decision atomically inside a Lua script, both take their
clock from Redis, and both fail open the same way — that behaviour lives in the
shared base class precisely so it cannot vary between them.

### The tradeoff

|  | sliding window log | token bucket |
| --- | --- | --- |
| State per client | one sorted set entry **per request** | two hash fields, **O(1)** |
| Exactness | never more than `limit` in any window | bursts can exceed it |
| Capacity returns | in steps, as entries age out | continuously, at the refill rate |
| Idle client | no advantage | arrives with a full bucket |

**The sliding window log is exact.** Each client has a sorted set holding one
member per accepted request, scored by arrival millisecond. A request is allowed
if, after dropping members that have aged out, the set holds fewer entries than
the limit. No window of length `window_ms` ever contains more than `limit`
requests — not at a boundary, not after idling, never.

You pay for that in memory. One entry per request per window means a client on a
limit of 100k holds 100k sorted set members.

**The token bucket is constant-size.** It stores tokens remaining and when they
were last counted, refilling continuously at `limit` per `window_ms`. State is
two numbers no matter how large the limit is.

The cost is burst tolerance. An idle client arrives with a full bucket and can
spend it at once, then continue at the refill rate — so a rolling window
straddling that burst can contain more than `limit` requests. Sustained rate is
identical; only the shape differs.

**Which to pick.** If the limit is a billing or fairness boundary you have to
defend exactly, the log is correct and the memory is the price. If you are
protecting a backend from sustained overload and a short burst is survivable,
the bucket costs a fraction of the memory and is kinder to bursty-but-legitimate
clients. This project defaults to the log because the exactness is easier to
demonstrate and to test.

### Why both are atomic

The whole check-and-increment is one Lua script, which Redis executes as a
single atomic unit:

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
what is inside the braces is hashed to a slot, so a client's state and its
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

## Tests

```bash
docker compose --profile test run --rm tests
```

53 tests covering the limiter decisions and the HTTP surface. The shared
contract in `tests/test_limiter_contract.py` is parametrised over every
registered algorithm, so adding a third would immediately be held to the same
behaviour; each algorithm then has its own file for what makes it different. They run against a
real Redis rather than a fake one: the behaviour under test is atomic execution
of a Lua script and a server-side clock, and a Python reimplementation of those
would let the suite pass while the thing it claims to prove was broken.

The test container is a separate build stage, so the image that serves traffic
does not ship pytest. It flushes Redis on each test, so do not run it against a
stack you are load testing.

CI runs this suite on every push, plus a job that brings up the full
three-instance stack and runs the verification scripts against it, once per
algorithm. The parts that matter here are the ones a unit test cannot reach, so
both jobs earn their place.

The suite was checked against deliberate mutations to confirm it has teeth.
Changing `count < limit` to `count <= limit` in the script fails 13 tests;
removing the `ZREMRANGEBYSCORE` prune fails exactly one, the sliding window
test, which is the only behaviour that mutation actually changes.

## Verifying it

Five scripts on top of the test suite, all run inside a container that already
has the dependencies.

```bash
# both algorithms hold the limit exactly under concurrency
docker compose exec app-1 python -m scripts.verify_atomicity

# capacity returns gradually, so the window really slides
docker compose exec app-1 python -m scripts.verify_sliding_window

# the bucket refills continuously and caps at capacity
docker compose exec app-1 python -m scripts.verify_token_bucket

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
  limiters/
    base.py                   the interface, key naming, fail-open policy
    sliding_window_log.py     exact, one entry per request
    token_bucket.py           O(1) state, allows bursts
  lua/
    sliding_window_log.lua    the atomic check-and-increment
    token_bucket.lua          atomic refill-and-spend
  clients.py        API keys and tier definitions
  metrics.py        reads the counters the scripts write
  config.py         environment settings, including ALGORITHM
  redis_client.py   one connection pool per process
tests/              pytest suite, run against a real Redis
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
- The sliding window log holds one entry per request per window, so a very high
  limit costs real memory. Switching such a tier to `token_bucket` is a config
  change; picking per tier rather than per deployment would be a small addition
  to `app/clients.py`.

## License

MIT. See [LICENSE](LICENSE).
