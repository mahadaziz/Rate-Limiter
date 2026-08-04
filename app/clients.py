"""Client registry and tier definitions.

An in-memory dict on purpose. A real deployment would read this from a database
or a config service, but the lookup is not what the project is demonstrating,
and keeping it here means no schema or migration sits between you and the
limiter. `lookup()` is the seam a database would slot into later.
"""

from typing import NamedTuple


class Tier(NamedTuple):
    name: str
    limit: int
    window_ms: int


TIERS: dict[str, Tier] = {
    "free": Tier(name="free", limit=10, window_ms=60_000),
    "pro": Tier(name="pro", limit=100, window_ms=60_000),
}


class Client(NamedTuple):
    client_id: str
    tier: Tier


# api key -> (client id, tier name)
_REGISTRY: dict[str, tuple[str, str]] = {
    "free-key-acme": ("acme", "free"),
    "free-key-globex": ("globex", "free"),
    "pro-key-initech": ("initech", "pro"),
    "pro-key-umbrella": ("umbrella", "pro"),
}


def lookup(api_key: str) -> Client | None:
    """Resolve an API key to a client, or None if the key is not registered."""
    entry = _REGISTRY.get(api_key)
    if entry is None:
        return None

    client_id, tier_name = entry
    return Client(client_id=client_id, tier=TIERS[tier_name])


def all_client_ids() -> list[str]:
    return [client_id for client_id, _ in _REGISTRY.values()]
