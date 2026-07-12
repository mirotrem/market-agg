from contextlib import asynccontextmanager

import asyncpg

from . import config

# Numeric columns are coalesced to 0 in query_items rather than returned as null - text/
# timestamp columns (type_name, last_update, history_updated_at) are left as null since 0
# wouldn't mean anything for them.
NUMERIC_COLUMNS = {
    "max_buy",
    "min_sell",
    "buy_listed",
    "sell_listed",
    "volume_7d",
    "volume_7d_min",
    "volume_7d_max",
    "weekly_movement",
}

ALLOWED_COLUMNS = [
    "type_id",
    "location_id",
    "type_name",
    "max_buy",
    "min_sell",
    "buy_listed",
    "sell_listed",
    "last_update",
    "volume_7d",
    "volume_7d_min",
    "volume_7d_max",
    "weekly_movement",
    "history_updated_at",
]

_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=10)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("db.init_pool() must be awaited before using the database")
    return _pool


@asynccontextmanager
async def get_conn():
    async with get_pool().acquire() as conn:
        yield conn


# NOTE on types: location_id/order_id/character_id and the volume-ish columns are BIGINT,
# not INTEGER - structure IDs (e.g. 1049588174021) and quantities/ISK totals we've actually
# observed (e.g. buy_listed of 14,853,982,625 for Tritanium at Jita) both overflow Postgres's
# 32-bit INTEGER. max_buy/min_sell/weekly_movement are DOUBLE PRECISION (8-byte), matching
# SQLite's REAL (which is always 8-byte regardless of declared affinity) - Postgres's REAL
# is only 4-byte and would silently lose precision on large ISK values.
_INIT_DB_LOCK_ID = 727001  # arbitrary constant; just needs to be the same across all callers


async def init_db() -> None:
    """Both the poller and every API replica call this on startup, so it must tolerate
    concurrent callers: CREATE TABLE IF NOT EXISTS is not actually atomic against a
    simultaneous creator on the same table (Postgres can raise a duplicate-key error on
    its internal pg_type catalog), so the whole thing runs under a session-level advisory
    lock to force callers to serialize instead of racing."""
    async with get_conn() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", _INIT_DB_LOCK_ID)
        try:
            await _create_tables(conn)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _INIT_DB_LOCK_ID)


async def _create_tables(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS item_stats (
            type_id INTEGER NOT NULL,
            location_id BIGINT NOT NULL,
            type_name TEXT,
            max_buy DOUBLE PRECISION,
            min_sell DOUBLE PRECISION,
            buy_listed BIGINT,
            sell_listed BIGINT,
            last_update TEXT,
            volume_7d BIGINT,
            volume_7d_min BIGINT,
            volume_7d_max BIGINT,
            weekly_movement DOUBLE PRECISION,
            history_updated_at TEXT,
            PRIMARY KEY (type_id, location_id)
        )
        """
    )
    await conn.execute("CREATE TABLE IF NOT EXISTS poll_state (key TEXT PRIMARY KEY, value TEXT)")
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sso_tokens (
            character_id BIGINT PRIMARY KEY,
            character_name TEXT,
            access_token TEXT,
            refresh_token TEXT,
            expires_at DOUBLE PRECISION
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS order_snapshots (
            order_id BIGINT PRIMARY KEY,
            location_id BIGINT NOT NULL,
            type_id INTEGER NOT NULL,
            volume_remain BIGINT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fill_events (
            id BIGSERIAL PRIMARY KEY,
            location_id BIGINT NOT NULL,
            type_id INTEGER NOT NULL,
            volume BIGINT NOT NULL,
            confirmed INTEGER NOT NULL,
            observed_at TEXT NOT NULL
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fill_events_lookup ON fill_events (location_id, observed_at)"
    )


async def get_poll_state(key: str) -> str | None:
    async with get_conn() as conn:
        return await conn.fetchval("SELECT value FROM poll_state WHERE key = $1", key)


async def set_poll_state(key: str, value: str) -> None:
    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO poll_state (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """,
            key,
            value,
        )


async def save_sso_token(
    character_id: int, character_name: str, access_token: str, refresh_token: str, expires_at: float
) -> None:
    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO sso_tokens (character_id, character_name, access_token, refresh_token, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (character_id) DO UPDATE SET
                character_name = excluded.character_name,
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                expires_at = excluded.expires_at
            """,
            character_id,
            character_name,
            access_token,
            refresh_token,
            expires_at,
        )


async def get_sso_token(character_id: int) -> dict | None:
    async with get_conn() as conn:
        row = await conn.fetchrow("SELECT * FROM sso_tokens WHERE character_id = $1", character_id)
        return dict(row) if row else None


async def list_sso_tokens() -> list[dict]:
    async with get_conn() as conn:
        return [dict(r) for r in await conn.fetch("SELECT * FROM sso_tokens")]


async def upsert_order_stats(location_id: int, rows: list[dict]) -> None:
    """rows: [{type_id, type_name, max_buy, min_sell, buy_listed, sell_listed, last_update}].
    Leaves volume_7d/weekly_movement untouched. Scoped to location_id so refreshing one
    location never touches another's rows."""
    async with get_conn() as conn, conn.transaction():
        if rows:
            await conn.executemany(
                """
                INSERT INTO item_stats
                    (type_id, location_id, type_name, max_buy, min_sell, buy_listed, sell_listed, last_update)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (type_id, location_id) DO UPDATE SET
                    type_name = excluded.type_name,
                    max_buy = excluded.max_buy,
                    min_sell = excluded.min_sell,
                    buy_listed = excluded.buy_listed,
                    sell_listed = excluded.sell_listed,
                    last_update = excluded.last_update
                """,
                [
                    (
                        r["type_id"],
                        location_id,
                        r["type_name"],
                        r["max_buy"],
                        r["min_sell"],
                        r["buy_listed"],
                        r["sell_listed"],
                        r["last_update"],
                    )
                    for r in rows
                ],
            )
            ids = [r["type_id"] for r in rows]
            await conn.execute(
                "DELETE FROM item_stats WHERE location_id = $1 AND NOT (type_id = ANY($2::int[]))",
                location_id,
                ids,
            )


async def update_history(
    type_id: int, location_id: int, volume_7d: int, weekly_movement: float | None, updated_at: str
) -> None:
    async with get_conn() as conn:
        await conn.execute(
            """
            UPDATE item_stats SET volume_7d = $1, weekly_movement = $2, history_updated_at = $3
            WHERE type_id = $4 AND location_id = $5
            """,
            volume_7d,
            weekly_movement,
            updated_at,
            type_id,
            location_id,
        )


async def apply_order_diff(location_id: int, current_orders: list[dict], now: str) -> None:
    """Diff current_orders against the last-seen snapshot for this location to infer trade
    volume, since ESI has no history endpoint for player structures.

    - An order whose volume_remain dropped since last seen (but still exists) is a
      *confirmed* partial fill for that difference - volume_remain cannot otherwise rise.
    - An order that vanished entirely since last seen is *ambiguous*: it may have fully
      filled, or the player may have just cancelled it. That's logged as an unconfirmed
      ("possible") fill of its last-known volume_remain, never as confirmed.
    """
    async with get_conn() as conn, conn.transaction():
        previous = {
            row["order_id"]: (row["type_id"], row["volume_remain"])
            for row in await conn.fetch(
                "SELECT order_id, type_id, volume_remain FROM order_snapshots WHERE location_id = $1",
                location_id,
            )
        }

        seen_order_ids: set[int] = set()
        fill_rows = []
        snapshot_rows = []

        for o in current_orders:
            order_id = o["order_id"]
            seen_order_ids.add(order_id)
            prev = previous.get(order_id)
            if prev is not None:
                prev_type_id, prev_remain = prev
                if o["volume_remain"] < prev_remain:
                    fill_rows.append((location_id, o["type_id"], prev_remain - o["volume_remain"], 1, now))
            snapshot_rows.append((order_id, location_id, o["type_id"], o["volume_remain"], now))

        for order_id, (prev_type_id, prev_remain) in previous.items():
            if order_id not in seen_order_ids:
                fill_rows.append((location_id, prev_type_id, prev_remain, 0, now))

        await conn.execute("DELETE FROM order_snapshots WHERE location_id = $1", location_id)
        if snapshot_rows:
            await conn.executemany(
                "INSERT INTO order_snapshots (order_id, location_id, type_id, volume_remain, last_seen_at) "
                "VALUES ($1, $2, $3, $4, $5)",
                snapshot_rows,
            )
        if fill_rows:
            await conn.executemany(
                "INSERT INTO fill_events (location_id, type_id, volume, confirmed, observed_at) "
                "VALUES ($1, $2, $3, $4, $5)",
                fill_rows,
            )


async def recompute_volume_estimates(location_id: int, since: str) -> None:
    """Recompute volume_7d_min (confirmed fills only) and volume_7d_max (confirmed +
    ambiguous disappearances) for every tracked item at this location, from fill_events
    observed since `since`. Items with no fill_events in the window are reset to 0 so the
    trailing window actually shrinks over time instead of holding stale non-zero values."""
    async with get_conn() as conn, conn.transaction():
        rows = await conn.fetch(
            """
            SELECT type_id,
                   SUM(CASE WHEN confirmed = 1 THEN volume ELSE 0 END) AS min_vol,
                   SUM(volume) AS max_vol
            FROM fill_events
            WHERE location_id = $1 AND observed_at >= $2
            GROUP BY type_id
            """,
            location_id,
            since,
        )
        by_type = {r["type_id"]: (r["min_vol"] or 0, r["max_vol"] or 0) for r in rows}

        active_type_ids = [
            r["type_id"]
            for r in await conn.fetch("SELECT type_id FROM item_stats WHERE location_id = $1", location_id)
        ]
        if active_type_ids:
            await conn.executemany(
                "UPDATE item_stats SET volume_7d_min = $1, volume_7d_max = $2 WHERE type_id = $3 AND location_id = $4",
                [(*by_type.get(type_id, (0, 0)), type_id, location_id) for type_id in active_type_ids],
            )


async def prune_fill_events(before: str) -> None:
    async with get_conn() as conn:
        await conn.execute("DELETE FROM fill_events WHERE observed_at < $1", before)


async def get_active_type_ids(location_id: int) -> list[int]:
    async with get_conn() as conn:
        rows = await conn.fetch("SELECT type_id FROM item_stats WHERE location_id = $1", location_id)
        return [r["type_id"] for r in rows]


async def query_items(
    columns: list[str],
    type_ids: list[int] | None = None,
    name: str | None = None,
    location_id: int | None = None,
) -> list[dict]:
    cols_sql = ", ".join(
        f"COALESCE({c}, 0) AS {c}" if c in NUMERIC_COLUMNS else c for c in columns
    )
    sql = f"SELECT {cols_sql} FROM item_stats"
    conditions = []
    params: list = []

    if type_ids:
        params.append(type_ids)
        conditions.append(f"type_id = ANY(${len(params)}::int[])")
    if name:
        params.append(f"%{name}%")
        conditions.append(f"type_name LIKE ${len(params)}")
    if location_id is not None:
        params.append(location_id)
        conditions.append(f"location_id = ${len(params)}")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY type_id"

    async with get_conn() as conn:
        return [dict(r) for r in await conn.fetch(sql, *params)]
