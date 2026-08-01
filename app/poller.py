import asyncio
import logging
from datetime import datetime, timezone

from . import cache, config, db, esi_client, sso

logger = logging.getLogger("poller")


def _aggregate_orders(orders: list[dict], location_filter: int | None) -> dict:
    """location_filter is required for station orders (the region feed covers every
    location in it) but None for structure orders, which are already scoped server-side."""
    by_type: dict[int, dict] = {}
    for o in orders:
        if location_filter is not None and o["location_id"] != location_filter:
            continue
        t = by_type.setdefault(
            o["type_id"],
            {"max_buy": None, "min_sell": None, "buy_listed": 0, "sell_listed": 0, "last_update": None},
        )
        if o["is_buy_order"]:
            if t["max_buy"] is None or o["price"] > t["max_buy"]:
                t["max_buy"] = o["price"]
            t["buy_listed"] += o["volume_remain"]
        else:
            if t["min_sell"] is None or o["price"] < t["min_sell"]:
                t["min_sell"] = o["price"]
            t["sell_listed"] += o["volume_remain"]
        if t["last_update"] is None or o["issued"] > t["last_update"]:
            t["last_update"] = o["issued"]
    return by_type


def _rows_from_aggregate(by_type: dict, names: dict[int, str]) -> list[dict]:
    return [
        {
            "type_id": type_id,
            "type_name": names.get(type_id),
            "max_buy": stats["max_buy"],
            "min_sell": stats["min_sell"],
            "buy_listed": stats["buy_listed"],
            "sell_listed": stats["sell_listed"],
            "last_update": stats["last_update"],
        }
        for type_id, stats in by_type.items()
    ]


async def _refresh_station_orders(location_key: str, loc: dict) -> int:
    location_id = loc["location_id"]
    state_key = f"orders_expires_at:{location_key}"
    known_expires = await db.get_poll_state(state_key)

    async with esi_client.make_client() as client:
        orders, expires = await esi_client.fetch_region_orders(client, loc["region_id"], known_expires)
        if orders is None:
            logger.info("[%s] orders cache unchanged (expires %s) - skipping refresh", location_key, expires)
            return 0
        logger.info("[%s] fetched %d region orders", location_key, len(orders))

        by_type = _aggregate_orders(orders, location_filter=location_id)
        logger.info("[%s] %d distinct types", location_key, len(by_type))

        names = await esi_client.fetch_names(client, list(by_type.keys()))

    await db.upsert_order_stats(location_id, _rows_from_aggregate(by_type, names))
    if expires:
        await db.set_poll_state(state_key, expires)
    await cache.bump_generation(location_key)
    return len(by_type)


async def _refresh_structure_orders(location_key: str, loc: dict) -> int:
    location_id = loc["location_id"]
    tokens = await db.list_sso_tokens()
    if not tokens:
        raise RuntimeError(f"[{location_key}] no SSO token on file - authorize via /auth/login first")
    character_id = tokens[0]["character_id"]
    access_token = await sso.get_valid_access_token(character_id)

    state_key = f"orders_expires_at:{location_key}"
    known_expires = await db.get_poll_state(state_key)

    async with esi_client.make_client() as client:
        orders, expires = await esi_client.fetch_structure_orders(client, location_id, access_token, known_expires)
        if orders is None:
            logger.info("[%s] structure orders cache unchanged (expires %s) - skipping", location_key, expires)
            return 0
        logger.info("[%s] fetched %d structure orders", location_key, len(orders))

        by_type = _aggregate_orders(orders, location_filter=None)
        logger.info("[%s] %d distinct types", location_key, len(by_type))

        names = await esi_client.fetch_names(client, list(by_type.keys()))

    await db.upsert_order_stats(location_id, _rows_from_aggregate(by_type, names))
    if expires:
        await db.set_poll_state(state_key, expires)

    await cache.bump_generation(location_key)
    return len(by_type)


async def refresh_location_orders(location_key: str) -> int:
    loc = config.LOCATIONS[location_key]
    if loc["kind"] == "station":
        return await _refresh_station_orders(location_key, loc)
    if loc["kind"] == "structure":
        return await _refresh_structure_orders(location_key, loc)
    raise ValueError(f"unknown location kind: {loc['kind']!r}")


def _weekly_stats(history: list[dict]) -> tuple[int, float | None]:
    last_7 = history[-7:] if history else []
    volume_7d = sum(day["volume"] for day in last_7)
    weekly_movement = None
    if last_7:
        week_low = min(day["lowest"] for day in last_7)
        week_high = max(day["highest"] for day in last_7)
        if week_low > 0:
            weekly_movement = (week_high - week_low) / week_low * 100
    return volume_7d, weekly_movement


async def refresh_location_history(location_key: str) -> int:
    loc = config.LOCATIONS[location_key]
    # Stations use their own region's history directly; structures only get real history if
    # they've explicitly opted in via history_region_id (see config.LOCATIONS for why that's
    # a per-location judgment call, not automatic).
    history_region_id = loc["region_id"] if loc["kind"] == "station" else loc.get("history_region_id")
    if history_region_id is None:
        logger.info("[%s] no ESI history endpoint exists for structures - skipping", location_key)
        return 0

    location_id = loc["location_id"]
    type_ids = await db.get_active_type_ids(location_id)
    if not type_ids:
        return 0
    state_key = f"history_expires_at:{location_key}"
    known_expires = await db.get_poll_state(state_key)
    now = datetime.now(timezone.utc).isoformat()

    async with esi_client.make_client() as client:
        # History resets once per day for the whole region at the same time regardless of
        # type_id, so a single cheap probe tells us whether the other ~18k calls are needed.
        probe_id, *remaining = type_ids
        probe_history, expires = await esi_client.fetch_type_history(client, history_region_id, probe_id)
        if known_expires is not None and expires == known_expires:
            logger.info("[%s] history cache unchanged (expires %s) - skipping refresh", location_key, expires)
            return 0

        volume_7d, weekly_movement = _weekly_stats(probe_history)
        await db.update_history(probe_id, location_id, volume_7d, weekly_movement, now)

        async def _one(type_id: int) -> None:
            history, _ = await esi_client.fetch_type_history(client, history_region_id, type_id)
            volume_7d, weekly_movement = _weekly_stats(history)
            await db.update_history(type_id, location_id, volume_7d, weekly_movement, now)

        await asyncio.gather(*[_one(t) for t in remaining])

    if expires:
        await db.set_poll_state(state_key, expires)
    await cache.bump_generation(location_key)
    return len(type_ids)


async def refresh_all_orders() -> dict[str, int]:
    return {key: await refresh_location_orders(key) for key in config.LOCATIONS}


async def refresh_all_history() -> dict[str, int]:
    return {key: await refresh_location_history(key) for key in config.LOCATIONS}


ADJUSTED_PRICES_KEY = "adjusted_prices"  # not a location - a single global ESI dataset


async def refresh_adjusted_prices() -> int:
    state_key = "adjusted_prices_expires_at"
    known_expires = await db.get_poll_state(state_key)
    now = datetime.now(timezone.utc).isoformat()

    async with esi_client.make_client() as client:
        rows, expires = await esi_client.fetch_adjusted_prices(client, known_expires)
        if rows is None:
            logger.info("[adjusted_prices] cache unchanged (expires %s) - skipping refresh", expires)
            return 0
        logger.info("[adjusted_prices] fetched %d types", len(rows))

    await db.upsert_adjusted_prices(rows, now)
    if expires:
        await db.set_poll_state(state_key, expires)
    await cache.bump_generation(ADJUSTED_PRICES_KEY)
    return len(rows)
