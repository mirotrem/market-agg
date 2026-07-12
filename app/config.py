import os

from dotenv import load_dotenv

load_dotenv()

REGION_ID = 10000002  # The Forge
STATION_ID = 60003760  # Jita IV - Moon 4 - Caldari Navy Assembly Plant

ESI_BASE = "https://esi.evetech.net/latest"
USER_AGENT = "market-agg/0.1 (contact: mirotrem@gmail.com)"

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://market:market@localhost:5432/market")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

ORDERS_POLL_SECONDS = 300  # region orders endpoint is cached ~5 min server-side
HISTORY_POLL_SECONDS = 24 * 60 * 60  # history is cached ~1 day server-side

ESI_CONCURRENCY = 10
ESI_ERROR_LIMIT_FLOOR = 20  # pause requests if remaining error budget drops below this

# Tracked market locations. "station" locations use the public region-orders endpoint
# filtered by location_id. "structure" locations are player-owned citadels: they require
# an authenticated character with docking access and are fetched directly by structure_id.
#
# ESI has no per-structure trade history endpoint. For most structures that means
# volume_7d/weekly_movement stay null. But a structure that dominates its region's trade
# can opt in to `history_region_id`: real ESI region history is then used for volume/movement
# (same mechanism as a "station"), instead of the self-derived order-diff estimate
# (volume_7d_min/volume_7d_max) - that's a judgment call per location, not automatic, since
# the region history is only a good proxy when one venue genuinely dominates the region.
# C-J6MT dominates Insmother (10000009): confirmed its buy orders make up the entire regional
# order book sample, and regional Tritanium volume (~1-5B/day) is implausible for ordinary
# nullsec NPC-station trade, so it can only be coming from C-J6MT.
LOCATIONS = {
    "jita": {"location_id": STATION_ID, "region_id": REGION_ID, "kind": "station"},
    "cj6mt": {
        "location_id": 1049588174021,  # C-J6MT - 1st Taj Mahgoon
        "kind": "structure",
        "history_region_id": 10000009,  # Insmother
    },
}

EVE_SSO_CLIENT_ID = os.environ.get("EVE_SSO_CLIENT_ID", "")
EVE_SSO_CLIENT_SECRET = os.environ.get("EVE_SSO_CLIENT_SECRET", "")
EVE_SSO_CALLBACK_URL = os.environ.get("EVE_SSO_CALLBACK_URL", "http://localhost:8000/auth/callback")
EVE_SSO_SCOPES = ["esi-markets.structure_markets.v1", "esi-universe.read_structures.v1"]
EVE_SSO_AUTHORIZE_URL = "https://login.eveonline.com/v2/oauth/authorize"
EVE_SSO_TOKEN_URL = "https://login.eveonline.com/v2/oauth/token"