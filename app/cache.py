import json
from typing import Any

import redis.asyncio as redis

from . import config

_client: redis.Redis | None = None

OAUTH_STATE_TTL_SECONDS = 300
CACHE_ENTRY_TTL_SECONDS = 900  # safety net only - generation bumps invalidate immediately


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(config.REDIS_URL, decode_responses=True)
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def bump_generation(location_key: str) -> int:
    return await get_client().incr(f"cache:gen:{location_key}")


async def get_generation(location_key: str) -> int:
    value = await get_client().get(f"cache:gen:{location_key}")
    return int(value) if value is not None else 0


async def cache_get(key: str) -> Any | None:
    value = await get_client().get(key)
    return json.loads(value) if value is not None else None


async def cache_set(key: str, value: Any, ttl: int = CACHE_ENTRY_TTL_SECONDS) -> None:
    await get_client().set(key, json.dumps(value), ex=ttl)


async def oauth_state_set(state: str) -> None:
    """Record a freshly-issued OAuth state so any API replica can validate the callback,
    since /auth/login and /auth/callback may land on different replicas behind Traefik."""
    await get_client().set(f"oauth_state:{state}", "1", ex=OAUTH_STATE_TTL_SECONDS)


async def oauth_state_consume(state: str) -> bool:
    """Atomically check-and-delete so a state can only be used once."""
    deleted = await get_client().delete(f"oauth_state:{state}")
    return deleted > 0
