"""FastAPI application entrypoint."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import INSTANCE_ID
from app.redis_client import close_client, create_client, get_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [" + INSTANCE_ID + "] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_client()
    logger.info("instance started")
    yield
    await close_client()


app = FastAPI(title="Distributed Rate Limiter", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    """Report whether this instance can reach Redis.

    Always 200: the limiter fails open, so an instance that has lost Redis is
    degraded but still serving. The body says which.
    """
    try:
        await get_client().ping()
        redis_state = "up"
        status = "ok"
    except Exception as exc:  # noqa: BLE001 - health check reports, never raises
        logger.warning("health check could not reach redis: %s", exc)
        redis_state = "down"
        status = "degraded"

    return {"status": status, "redis": redis_state, "instance": INSTANCE_ID}
