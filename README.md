# market-agg

A personal API that aggregates EVE Online market buy/sell prices across multiple locations — currently Jita 4-4 (the main trade hub) and a nullsec player-owned structure — by polling [ESI](https://developers.eveonline.com/docs/services/esi/), EVE Online's public API.

## Architecture

Runs as five Docker Compose services:

| Service | Role |
|---|---|
| `poller` | Singleton. Owns all ESI polling and every database write, on a schedule (orders every 5 min, history daily). Never scales — running more than one would double-hit ESI's rate limits and race on writes. |
| `api` | Stateless and horizontally scalable. Serves `/api/prices` by reading Postgres, with a Redis-backed cache in front. |
| `postgres` | Market data: current prices, quantities, and volume history per `(type_id, location_id)`. |
| `redis` | Two jobs: response caching (invalidated instantly on writes via a per-location generation counter, not a TTL), and shared OAuth state so `/auth/login` and `/auth/callback` work correctly across scaled `api` replicas. |
| `traefik` | Reverse proxy in front of `api`. |

### Locations

Two kinds of location are supported (`app/config.py`):

- **`station`** — an NPC station (e.g. Jita 4-4). Public data: orders come from ESI's region-orders endpoint, and real 7-day volume/price-movement history is available directly from ESI.
- **`structure`** — a player-owned citadel. Requires an EVE SSO-authenticated character with docking access (`/auth/login`) and is fetched via the authenticated structure-orders endpoint. ESI has **no** trade history endpoint for structures, so volume there is either:
  - self-derived by diffing order-book snapshots between polls (`volume_7d_min`/`volume_7d_max` — a confirmed/ambiguous bracket, not a single true number), or
  - if the structure dominates its region's trade enough to make regional history a reasonable proxy, opted in via `history_region_id` to use ESI's real region history instead (same mechanism as a station).

## Setup

1. Copy `.env.example` to `.env` and fill in:
   - `EVE_SSO_CLIENT_ID` / `EVE_SSO_CLIENT_SECRET` — register an app at [developers.eveonline.com/applications](https://developers.eveonline.com/applications) with scopes `esi-markets.structure_markets.v1` and `esi-universe.read_structures.v1`, callback URL matching `EVE_SSO_CALLBACK_URL`.
   - `POSTGRES_PASSWORD` — pick a strong password, and make sure `DATABASE_URL` uses the same one.
2. `docker-compose up -d --build`
3. If tracking a structure, visit `/auth/login` in a browser with a character that has docking access, and add the structure to `config.LOCATIONS`.

## API

```
GET  /api/prices?location=jita&type_id=34,627,585&columns=type_id,type_name,max_buy,min_sell
POST /api/prices        # same fields as JSON body - use this if type_id list is long enough to hit a URL length limit
GET  /api/locations
GET  /api/columns
POST /api/refresh/orders?location=jita   # manual trigger; omit location to refresh everything
POST /api/refresh/history?location=jita
```

## Scaling

`docker-compose up -d --scale api=N`. Traefik here uses a static file provider (`traefik/dynamic.yml`) rather than Docker-socket auto-discovery — depending on the host's Docker version, the socket-based provider may or may not work (see comments in `docker-compose.yml`). If using the static file provider, the replica list in `traefik/dynamic.yml` needs to be updated by hand to match whatever `N` you scale to.

## One-time migration tooling

`scripts/migrate_sqlite_to_postgres.py` was used once to move this project's original SQLite-backed prototype into Postgres. Not needed for a fresh setup.
