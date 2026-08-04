"""Application configuration, read from the environment.

Per-client limits live in `app.clients`; this module holds only the settings
that are the same for every caller.
"""

import os

# Redis connection string shared by every app instance.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Identifies which app instance served a request. Set per-container in
# docker-compose so responses make the load balancing visible.
INSTANCE_ID = os.getenv("INSTANCE_ID", "local")

# How long to wait on Redis before giving up on a call.
REDIS_TIMEOUT_SECONDS = float(os.getenv("REDIS_TIMEOUT_SECONDS", "0.5"))

# Which limiting algorithm to run: sliding_window_log or token_bucket.
# Both enforce the same sustained rate; they differ in memory cost and in
# whether they permit bursts. See the README.
ALGORITHM = os.getenv("ALGORITHM", "sliding_window_log")
