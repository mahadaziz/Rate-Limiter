"""The HTTP surface: identification, status codes, headers, metrics."""

FREE_KEY = "free-key-acme"
FREE_LIMIT = 10
PRO_KEY = "pro-key-initech"
PRO_LIMIT = 100


def test_health_reports_redis_reachable(api):
    body = api.get("/health").json()
    assert body["status"] == "ok"
    assert body["redis"] == "up"


def test_missing_api_key_is_rejected(api):
    assert api.get("/limited").status_code == 401


def test_unknown_api_key_is_rejected(api):
    assert api.get("/limited", headers={"X-API-Key": "not-a-key"}).status_code == 401


def test_free_tier_stops_at_its_limit(api):
    codes = [
        api.get("/limited", headers={"X-API-Key": FREE_KEY}).status_code
        for _ in range(FREE_LIMIT + 2)
    ]
    assert codes.count(200) == FREE_LIMIT
    assert codes.count(429) == 2


def test_pro_tier_gets_a_higher_limit(api):
    codes = [
        api.get("/limited", headers={"X-API-Key": PRO_KEY}).status_code
        for _ in range(PRO_LIMIT + 2)
    ]
    assert codes.count(200) == PRO_LIMIT
    assert codes.count(429) == 2


def test_tiers_do_not_share_a_budget(api):
    for _ in range(FREE_LIMIT):
        api.get("/limited", headers={"X-API-Key": FREE_KEY})

    assert api.get("/limited", headers={"X-API-Key": FREE_KEY}).status_code == 429
    assert api.get("/limited", headers={"X-API-Key": PRO_KEY}).status_code == 200


def test_allowed_response_carries_rate_limit_headers(api):
    resp = api.get("/limited", headers={"X-API-Key": FREE_KEY})

    assert resp.headers["X-RateLimit-Limit"] == str(FREE_LIMIT)
    assert resp.headers["X-RateLimit-Remaining"] == str(FREE_LIMIT - 1)
    assert resp.headers["X-RateLimit-Tier"] == "free"
    assert "X-Instance" in resp.headers


def test_denied_response_says_when_to_retry(api):
    for _ in range(FREE_LIMIT):
        api.get("/limited", headers={"X-API-Key": FREE_KEY})

    resp = api.get("/limited", headers={"X-API-Key": FREE_KEY})

    assert resp.status_code == 429
    assert resp.headers["X-RateLimit-Remaining"] == "0"
    assert int(resp.headers["Retry-After"]) >= 1
    assert resp.json()["retry_after_ms"] > 0


def test_not_degraded_while_redis_is_up(api):
    resp = api.get("/limited", headers={"X-API-Key": FREE_KEY})
    assert "X-RateLimit-Degraded" not in resp.headers


def test_metrics_report_allowed_and_denied(api):
    for _ in range(FREE_LIMIT + 3):
        api.get("/limited", headers={"X-API-Key": FREE_KEY})

    acme = api.get("/metrics").json()["clients"]["acme"]

    assert acme["allowed"] == FREE_LIMIT
    assert acme["denied"] == 3
    assert acme["total"] == FREE_LIMIT + 3
    assert acme["tier"] == "free"
    assert acme["in_window_now"] == FREE_LIMIT


def test_metrics_cover_every_registered_client(api):
    clients = api.get("/metrics").json()["clients"]
    assert set(clients) == {"acme", "globex", "initech", "umbrella"}


def test_metrics_reset_zeroes_the_counters(api):
    for _ in range(3):
        api.get("/limited", headers={"X-API-Key": FREE_KEY})

    assert api.post("/metrics/reset").status_code == 200

    acme = api.get("/metrics").json()["clients"]["acme"]
    assert acme["allowed"] == 0
    assert acme["denied"] == 0


def test_reset_clears_counters_but_not_the_limit(api):
    """Resetting metrics must not hand a throttled client a fresh budget."""
    for _ in range(FREE_LIMIT):
        api.get("/limited", headers={"X-API-Key": FREE_KEY})

    api.post("/metrics/reset")

    assert api.get("/limited", headers={"X-API-Key": FREE_KEY}).status_code == 429
