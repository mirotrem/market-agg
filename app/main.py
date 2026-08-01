import hashlib
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from . import cache, config, db, poller, sso

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # otherwise every single request gets its own INFO line
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_pool()
    await db.init_db()
    yield
    await db.close_pool()
    await cache.close()


app = FastAPI(title="Market Prices", lifespan=lifespan)


@app.get("/")
def index():
    return RedirectResponse("/docs")


@app.get("/auth/login")
async def auth_login():
    return RedirectResponse(await sso.build_login_url())


@app.get("/auth/callback")
async def auth_callback(code: str, state: str):
    if not await sso.consume_state(state):
        raise HTTPException(status_code=400, detail="Invalid or expired state parameter")
    character = await sso.complete_login(code)
    return {
        "authorized": True,
        "character_id": character["CharacterID"],
        "character_name": character["CharacterName"],
    }


@app.get("/api/locations")
async def list_locations():
    tokens = await db.list_sso_tokens()
    return {
        key: {**loc, "authorized_character": tokens[0]["character_name"] if (loc["kind"] == "structure" and tokens) else None}
        for key, loc in config.LOCATIONS.items()
    }


@app.get("/api/columns")
def list_columns():
    return {"columns": db.ALLOWED_COLUMNS}


async def _prices_response(
    location: str, columns: str, type_ids: list[int] | None, name: str | None
) -> dict:
    if location not in config.LOCATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown location {location!r}. Configured locations: {list(config.LOCATIONS)}",
        )
    requested = [c.strip() for c in columns.split(",") if c.strip()]
    invalid = [c for c in requested if c not in db.ALLOWED_COLUMNS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown column(s): {invalid}. Allowed: {db.ALLOWED_COLUMNS}",
        )

    # Cache key embeds the location's write generation, so a poller write (which bumps the
    # generation immediately after committing) makes every previously-cached entry for that
    # location unreachable right away - no explicit purge, no TTL-driven staleness window.
    generation = await cache.get_generation(location)
    key_material = json.dumps(
        [location, requested, sorted(type_ids) if type_ids else None, name], sort_keys=True
    )
    cache_key = f"cache:data:{location}:{generation}:{hashlib.sha256(key_material.encode()).hexdigest()}"

    cached = await cache.cache_get(cache_key)
    if cached is not None:
        return cached

    location_id = config.LOCATIONS[location]["location_id"]
    rows = await db.query_items(requested, type_ids=type_ids, name=name, location_id=location_id)
    result = {"count": len(rows), "columns": requested, "results": rows}
    await cache.cache_set(cache_key, result)
    return result


def _parse_type_id_param(type_id: str | None) -> list[int] | None:
    if type_id is None:
        return None
    raw_ids = [t.strip() for t in type_id.split(",") if t.strip()]
    try:
        return [int(t) for t in raw_ids]
    except ValueError:
        raise HTTPException(status_code=400, detail=f"type_id must be comma-separated integers, got: {type_id!r}")


@app.get("/api/prices")
async def prices(
    location: str = Query("jita", description="Location key from GET /api/locations"),
    columns: str = Query(
        ",".join(db.ALLOWED_COLUMNS),
        description="Comma-separated list of columns to return. GET /api/columns for the full list.",
    ),
    type_id: str | None = Query(None, description="One or more comma-separated item type_ids, e.g. 34,35,36"),
    name: str | None = Query(None, description="Filter by substring match on item name"),
):
    return await _prices_response(location, columns, _parse_type_id_param(type_id), name)


class PricesRequest(BaseModel):
    location: str = "jita"
    columns: str = ",".join(db.ALLOWED_COLUMNS)
    type_id: list[int] | None = None
    name: str | None = None


@app.post("/api/prices")
async def prices_post(body: PricesRequest):
    """Same as GET /api/prices, but takes type_id as a JSON array in the body instead of
    a query string - use this when the type_id list is long enough to hit a client-side
    URL length limit (e.g. Google Apps Script's UrlFetchApp)."""
    return await _prices_response(body.location, body.columns, body.type_id, body.name)


@app.get("/api/jita-prices")
async def jita_prices(
    columns: str = Query(",".join(db.ALLOWED_COLUMNS)),
    type_id: str | None = Query(None),
    name: str | None = Query(None),
):
    """Deprecated alias for GET /api/prices?location=jita, kept for backward compatibility."""
    return await _prices_response("jita", columns, _parse_type_id_param(type_id), name)


async def _adjusted_prices_response(type_ids: list[int] | None) -> dict:
    # Same generation-bump cache pattern as _prices_response, keyed under its own namespace
    # since this isn't a location - it's ESI's single global /markets/prices/ dataset.
    generation = await cache.get_generation(poller.ADJUSTED_PRICES_KEY)
    key_material = json.dumps(sorted(type_ids) if type_ids else None)
    cache_key = f"cache:data:{poller.ADJUSTED_PRICES_KEY}:{generation}:{hashlib.sha256(key_material.encode()).hexdigest()}"

    cached = await cache.cache_get(cache_key)
    if cached is not None:
        return cached

    rows = await db.get_adjusted_prices(type_ids)
    result = {"count": len(rows), "results": rows}
    await cache.cache_set(cache_key, result)
    return result


@app.get("/api/adjusted-prices")
async def adjusted_prices(
    type_id: str | None = Query(None, description="One or more comma-separated item type_ids, e.g. 34,35,36"),
):
    """adjusted_price/average_price from ESI's /markets/prices/ - a single global dataset (no
    location), used for industry job cost calculations, not a tradeable market price."""
    return await _adjusted_prices_response(_parse_type_id_param(type_id))


class AdjustedPricesRequest(BaseModel):
    type_id: list[int] | None = None


@app.post("/api/adjusted-prices")
async def adjusted_prices_post(body: AdjustedPricesRequest):
    """Same as GET /api/adjusted-prices, but takes type_id as a JSON array in the body -
    use this when the type_id list is long enough to hit a client-side URL length limit."""
    return await _adjusted_prices_response(body.type_id)


@app.get("/api/system-cost-index-columns")
def list_system_cost_index_columns():
    return {"columns": db.SYSTEM_COST_INDEX_COLUMNS}


async def _system_cost_indices_response(columns: str, solar_system_ids: list[int] | None) -> dict:
    requested = [c.strip() for c in columns.split(",") if c.strip()]
    invalid = [c for c in requested if c not in db.SYSTEM_COST_INDEX_COLUMNS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown column(s): {invalid}. Allowed: {db.SYSTEM_COST_INDEX_COLUMNS}",
        )

    # Same generation-bump cache pattern as _prices_response/_adjusted_prices_response, keyed
    # under its own namespace since this isn't a location either.
    generation = await cache.get_generation(poller.SYSTEM_COST_INDEX_KEY)
    key_material = json.dumps([requested, sorted(solar_system_ids) if solar_system_ids else None])
    cache_key = (
        f"cache:data:{poller.SYSTEM_COST_INDEX_KEY}:{generation}:"
        f"{hashlib.sha256(key_material.encode()).hexdigest()}"
    )

    cached = await cache.cache_get(cache_key)
    if cached is not None:
        return cached

    rows = await db.get_system_cost_indices(requested, solar_system_ids)
    result = {"count": len(rows), "columns": requested, "results": rows}
    await cache.cache_set(cache_key, result)
    return result


@app.get("/api/system-cost-indices")
async def system_cost_indices(
    columns: str = Query(
        ",".join(db.SYSTEM_COST_INDEX_COLUMNS),
        description="Comma-separated list of columns. GET /api/system-cost-index-columns for the full list.",
    ),
    solar_system_id: str | None = Query(
        None, description="One or more comma-separated solar_system_ids, e.g. 30000142,30020141"
    ),
):
    """System cost indices from ESI's /industry/systems/ - a single global dataset (no
    location), used for industry job cost calculations."""
    return await _system_cost_indices_response(columns, _parse_type_id_param(solar_system_id))


class SystemCostIndicesRequest(BaseModel):
    columns: str = ",".join(db.SYSTEM_COST_INDEX_COLUMNS)
    solar_system_id: list[int] | None = None


@app.post("/api/system-cost-indices")
async def system_cost_indices_post(body: SystemCostIndicesRequest):
    """Same as GET /api/system-cost-indices, but takes solar_system_id as a JSON array in the
    body - use this when the list is long enough to hit a client-side URL length limit."""
    return await _system_cost_indices_response(body.columns, body.solar_system_id)


@app.post("/api/refresh/orders")
async def trigger_refresh_orders(location: str | None = Query(None, description="Omit to refresh all locations")):
    if location is None:
        return await poller.refresh_all_orders()
    if location not in config.LOCATIONS:
        raise HTTPException(status_code=400, detail=f"Unknown location {location!r}")
    return {location: await poller.refresh_location_orders(location)}


@app.post("/api/refresh/history")
async def trigger_refresh_history(location: str | None = Query(None, description="Omit to refresh all locations")):
    if location is None:
        return await poller.refresh_all_history()
    if location not in config.LOCATIONS:
        raise HTTPException(status_code=400, detail=f"Unknown location {location!r}")
    return {location: await poller.refresh_location_history(location)}


@app.post("/api/refresh/adjusted-prices")
async def trigger_refresh_adjusted_prices():
    return {"adjusted_prices": await poller.refresh_adjusted_prices()}


@app.post("/api/refresh/system-cost-indices")
async def trigger_refresh_system_cost_indices():
    return {"system_cost_indices": await poller.refresh_system_cost_indices()}
