"""Application configuration, read from the environment."""

import os

# Redis connection string shared by every app instance.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Identifies which app instance served a request. Set per-container in
# docker-compose so responses make the load balancing visible.
INSTANCE_ID = os.getenv("INSTANCE_ID", "local")

# How long to wait on Redis before giving up on a call.
REDIS_TIMEOUT_SECONDS = float(os.getenv("REDIS_TIMEOUT_SECONDS", "0.5"))

# Limit applied to callers that have not been identified yet. Step 3 replaces
# this with per-client tiers.
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "10"))
DEFAULT_WINDOW_MS = int(os.getenv("DEFAULT_WINDOW_MS", "60000"))
