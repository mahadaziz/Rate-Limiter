"""Shared fixtures.

The tests run against a real Redis rather than a fake one. The behaviour under
test is atomic execution of a Lua script and a server-side clock, which a
reimplementation in Python would not reproduce faithfully; a fake would let the
suite pass while the thing it claims to prove was broken.
"""

import pytest
import pytest_asyncio
import redis.asyncio as redis
from fastapi.testclient import TestClient

from app.config import REDIS_URL
from app.limiters import ALGORITHMS, build_limiter


@pytest_asyncio.fixture
async def redis_client():
    """A clean Redis for each test.

    Its own client rather than the app's module-level one, so tests cannot be
    affected by the app's connection lifecycle.
    """
    client = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2)
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(params=sorted(ALGORITHMS))
async def limiter(request, redis_client):
    """Every test using this fixture runs once per algorithm.

    Anything asserted through it is part of the shared contract, not a quirk of
    one implementation.
    """
    return build_limiter(request.param, redis_client)


@pytest_asyncio.fixture
async def sliding_window(redis_client):
    return build_limiter("sliding_window_log", redis_client)


@pytest_asyncio.fixture
async def token_bucket(redis_client):
    return build_limiter("token_bucket", redis_client)


@pytest.fixture
def api(redis_client):
    """The app under test, with lifespan run so the limiter is wired up.

    Depends on redis_client purely for its flush, so each test starts from an
    empty Redis.
    """
    from app.main import app

    with TestClient(app) as client:
        yield client
