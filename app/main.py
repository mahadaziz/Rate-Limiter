"""FastAPI application entrypoint.

Routes only: they identify the caller, ask the limiter for a decision, and
turn that decision into HTTP. The decision itself lives in `app.limiters`, and
who gets what limit lives in `app.clients`.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError

from app import metrics
from app.clients import Client, lookup
from app.config import ALGORITHM, INSTANCE_ID
from app.limiters import Limiter, build_limiter
from app.redis_client import close_client, create_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [" + INSTANCE_ID + "] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = create_client()
    app.state.limiter = build_limiter(ALGORITHM, client)
    logger.info("instance started using the %s algorithm", ALGORITHM)
    yield
    await close_client()


app = FastAPI(title="Distributed Rate Limiter", lifespan=lifespan)


async def require_client(x_api_key: str | None = Header(default=None)) -> Client:
    """Resolve the caller from their API key, or reject the request.

    Unregistered keys are turned away rather than given a default limit; if
    anonymous callers were allowed through, dropping the header would be a way
    to sidestep whatever tier the key carries.
    """
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="missing X-API-Key header")

    client = lookup(x_api_key)
    if client is None:
        raise HTTPException(status_code=401, detail="unknown API key")

    return client


@app.get("/health")
async def health(request: Request) -> dict:
    """Report whether this instance can reach Redis.

    Always 200: the limiter fails open, so an instance that has lost Redis is
    degraded but still serving. The body says which.
    """
    try:
        await request.app.state.limiter.client.ping()
        redis_state = "up"
        status = "ok"
    except Exception as exc:  # noqa: BLE001 - health check reports, never raises
        logger.warning("health check could not reach redis: %s", exc)
        redis_state = "down"
        status = "degraded"

    return {
        "status": status,
        "redis": redis_state,
        "instance": INSTANCE_ID,
        "algorithm": ALGORITHM,
    }


@app.get("/metrics")
async def get_metrics(request: Request):
    """Per-client allowed/denied counts.

    The counts are shared across instances because they are kept in Redis, so
    this reports the whole cluster's view no matter which instance answers.
    The fallback count is the exception: it is per instance, since it counts
    the times this process could not reach Redis to record anything.
    """
    limiter: Limiter = request.app.state.limiter

    try:
        clients = await metrics.collect(limiter)
    except (RedisError, OSError) as exc:
        logger.error("metrics unavailable, redis unreachable: %s", exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "metrics unavailable, redis unreachable",
                "instance": INSTANCE_ID,
                "algorithm": limiter.name,
                "fallbacks_this_instance": limiter.fallback_count,
            },
        )

    return {
        "instance": INSTANCE_ID,
        "algorithm": limiter.name,
        "fallbacks_this_instance": limiter.fallback_count,
        "clients": clients,
    }


@app.post("/metrics/reset")
async def reset_metrics(request: Request):
    """Zero the counters. Convenience for repeated load test runs."""
    try:
        await metrics.reset(request.app.state.limiter)
    except (RedisError, OSError) as exc:
        logger.error("metrics reset failed, redis unreachable: %s", exc)
        raise HTTPException(status_code=503, detail="redis unreachable") from exc

    return {"detail": "metrics reset"}


@app.get("/limited")
async def limited(request: Request, client: Client = Depends(require_client)):
    """A rate limited endpoint, at whatever limit the caller's tier carries."""
    tier = client.tier

    result = await request.app.state.limiter.check(
        client.client_id, tier.limit, tier.window_ms
    )

    headers = {
        "X-RateLimit-Limit": str(result.limit),
        "X-RateLimit-Remaining": str(result.remaining),
        "X-RateLimit-Tier": tier.name,
        "X-RateLimit-Algorithm": request.app.state.limiter.name,
        "X-Instance": INSTANCE_ID,
    }

    if result.degraded:
        # Say so on the response rather than only in the logs, so a caller can
        # tell that the limit was not actually enforced for this request.
        headers["X-RateLimit-Degraded"] = "true"

    if not result.allowed:
        # Round up, so a caller that waits exactly this long is past the edge
        # of the window rather than sitting on it.
        retry_after_s = max(1, -(-result.retry_after_ms // 1000))
        headers["Retry-After"] = str(retry_after_s)
        return JSONResponse(
            status_code=429,
            content={
                "detail": "rate limit exceeded",
                "client_id": client.client_id,
                "tier": tier.name,
                "retry_after_ms": result.retry_after_ms,
            },
            headers=headers,
        )

    return JSONResponse(
        content={
            "detail": "ok",
            "client_id": client.client_id,
            "tier": tier.name,
            "remaining": result.remaining,
            "instance": INSTANCE_ID,
        },
        headers=headers,
    )
