"""One-time migration of the old SQLite data file into Postgres.

Run this once, after the docker-compose stack is up, from inside a container attached
to the compose network (so the `postgres` hostname resolves) with the old SQLite file
bind-mounted in, e.g.:

    docker compose run --rm -v $(pwd)/data:/app/data:ro poller \\
        python -m scripts.migrate_sqlite_to_postgres
"""

import asyncio
import sqlite3

from app import config, db


def _read_sqlite_table(sqlite_conn: sqlite3.Connection, table: str) -> list[dict]:
    sqlite_conn.row_factory = sqlite3.Row
    return [dict(r) for r in sqlite_conn.execute(f"SELECT * FROM {table}")]


async def main() -> None:
    sqlite_conn = sqlite3.connect(config.DB_PATH)

    await db.init_pool()
    await db.init_db()
    pool = db.get_pool()

    item_stats = _read_sqlite_table(sqlite_conn, "item_stats")
    poll_state = _read_sqlite_table(sqlite_conn, "poll_state")
    sso_tokens = _read_sqlite_table(sqlite_conn, "sso_tokens")
    order_snapshots = _read_sqlite_table(sqlite_conn, "order_snapshots")
    fill_events = _read_sqlite_table(sqlite_conn, "fill_events")
    sqlite_conn.close()

    async with pool.acquire() as conn, conn.transaction():
        if item_stats:
            await conn.executemany(
                """
                INSERT INTO item_stats
                    (type_id, location_id, type_name, max_buy, min_sell, buy_listed, sell_listed,
                     last_update, volume_7d, volume_7d_min, volume_7d_max, weekly_movement, history_updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (type_id, location_id) DO UPDATE SET
                    type_name = excluded.type_name,
                    max_buy = excluded.max_buy,
                    min_sell = excluded.min_sell,
                    buy_listed = excluded.buy_listed,
                    sell_listed = excluded.sell_listed,
                    last_update = excluded.last_update,
                    volume_7d = excluded.volume_7d,
                    volume_7d_min = excluded.volume_7d_min,
                    volume_7d_max = excluded.volume_7d_max,
                    weekly_movement = excluded.weekly_movement,
                    history_updated_at = excluded.history_updated_at
                """,
                [
                    (
                        r["type_id"], r["location_id"], r["type_name"], r["max_buy"], r["min_sell"],
                        r["buy_listed"], r["sell_listed"], r["last_update"], r["volume_7d"],
                        r.get("volume_7d_min"), r.get("volume_7d_max"), r["weekly_movement"],
                        r["history_updated_at"],
                    )
                    for r in item_stats
                ],
            )

        if poll_state:
            await conn.executemany(
                """
                INSERT INTO poll_state (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = excluded.value
                """,
                [(r["key"], r["value"]) for r in poll_state],
            )

        if sso_tokens:
            await conn.executemany(
                """
                INSERT INTO sso_tokens (character_id, character_name, access_token, refresh_token, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (character_id) DO UPDATE SET
                    character_name = excluded.character_name,
                    access_token = excluded.access_token,
                    refresh_token = excluded.refresh_token,
                    expires_at = excluded.expires_at
                """,
                [
                    (r["character_id"], r["character_name"], r["access_token"], r["refresh_token"], r["expires_at"])
                    for r in sso_tokens
                ],
            )

        if order_snapshots:
            await conn.executemany(
                """
                INSERT INTO order_snapshots (order_id, location_id, type_id, volume_remain, last_seen_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (order_id) DO UPDATE SET
                    location_id = excluded.location_id,
                    type_id = excluded.type_id,
                    volume_remain = excluded.volume_remain,
                    last_seen_at = excluded.last_seen_at
                """,
                [
                    (r["order_id"], r["location_id"], r["type_id"], r["volume_remain"], r["last_seen_at"])
                    for r in order_snapshots
                ],
            )

        if fill_events:
            await conn.executemany(
                """
                INSERT INTO fill_events (location_id, type_id, volume, confirmed, observed_at)
                VALUES ($1, $2, $3, $4, $5)
                """,
                [
                    (r["location_id"], r["type_id"], r["volume"], r["confirmed"], r["observed_at"])
                    for r in fill_events
                ],
            )

    pg_counts = {}
    async with pool.acquire() as conn:
        for table in ("item_stats", "poll_state", "sso_tokens", "order_snapshots", "fill_events"):
            pg_counts[table] = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")

    print("SQLite row counts:")
    print(f"  item_stats:       {len(item_stats)}")
    print(f"  poll_state:       {len(poll_state)}")
    print(f"  sso_tokens:       {len(sso_tokens)}")
    print(f"  order_snapshots:  {len(order_snapshots)}")
    print(f"  fill_events:      {len(fill_events)}")
    print("Postgres row counts after migration:")
    for table, count in pg_counts.items():
        print(f"  {table}: {count}")

    await db.close_pool()


if __name__ == "__main__":
    asyncio.run(main())
