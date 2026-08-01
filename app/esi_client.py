import asyncio
import logging

import httpx

from . import config

logger = logging.getLogger("esi_client")

_semaphore = asyncio.Semaphore(config.ESI_CONCURRENCY)


class _ErrorBudgetGate:
    """Shared across all requests so a low error budget pauses every caller, not just the one that noticed."""

    def __init__(self) -> None:
        self._pause_until = 0.0

    async def wait_if_paused(self) -> None:
        now = asyncio.get_event_loop().time()
        if now < self._pause_until:
            await asyncio.sleep(self._pause_until - now)

    def note_response(self, resp: httpx.Response) -> None:
        remain = resp.headers.get("X-ESI-Error-Limit-Remain")
        reset = resp.headers.get("X-ESI-Error-Limit-Reset")
        if remain is None:
            return
        if int(remain) < config.ESI_ERROR_LIMIT_FLOOR:
            pause_for = float(reset) if reset else 5.0
            now = asyncio.get_event_loop().time()
            new_pause_until = now + pause_for
            if new_pause_until > self._pause_until:
                logger.warning("ESI error limit low (%s remaining), pausing all requests %.1fs", remain, pause_for)
                self._pause_until = new_pause_until

    def note_ban(self, resp: httpx.Response) -> None:
        """ESI returned 420: the error budget hit zero and we're temporarily blocked."""
        retry_after = resp.headers.get("Retry-After") or resp.headers.get("X-ESI-Error-Limit-Reset")
        pause_for = float(retry_after) if retry_after else 60.0
        now = asyncio.get_event_loop().time()
        new_pause_until = now + pause_for
        if new_pause_until > self._pause_until:
            logger.error("ESI returned 420 (error limited) - pausing all requests %.1fs", pause_for)
            self._pause_until = new_pause_until


_error_gate = _ErrorBudgetGate()

_MAX_BAN_RETRIES = 5


async def _send(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
    """Rate-limit-aware request: waits on the shared error-budget gate, then holds the
    concurrency semaphore for the pause check + request together so throttling can't be
    bypassed by other coroutines grabbing a freed slot mid-backoff. Retries on a 420
    (error-limited ban) instead of surfacing it, since a ban is temporary and self-clears."""
    resp = None
    for _ in range(_MAX_BAN_RETRIES):
        async with _semaphore:
            await _error_gate.wait_if_paused()
            resp = await client.request(method, path, **kwargs)
        if resp.status_code == 420:
            _error_gate.note_ban(resp)
            continue
        _error_gate.note_response(resp)
        return resp
    return resp


async def fetch_region_orders(
    client: httpx.AsyncClient, region_id: int, known_expires: str | None = None
) -> tuple[list[dict] | None, str | None]:
    """Fetch all pages of /markets/{region_id}/orders/ (both buy and sell).

    Always fetches page 1 to read its Expires header. If that header is identical to
    known_expires, the upstream cache hasn't advanced since our last poll, so the
    remaining pages are skipped entirely and (None, expires) is returned.
    """
    first = await _send(client, "GET", f"/markets/{region_id}/orders/", params={"order_type": "all", "page": 1})
    first.raise_for_status()
    expires = first.headers.get("Expires")
    if known_expires is not None and expires == known_expires:
        return None, expires

    orders = first.json()
    total_pages = int(first.headers.get("X-Pages", 1))

    if total_pages > 1:
        results = await asyncio.gather(
            *[
                _send(client, "GET", f"/markets/{region_id}/orders/", params={"order_type": "all", "page": p})
                for p in range(2, total_pages + 1)
            ]
        )
        for r in results:
            r.raise_for_status()
            orders.extend(r.json())

    return orders, expires


async def fetch_type_history(
    client: httpx.AsyncClient, region_id: int, type_id: int
) -> tuple[list[dict], str | None]:
    """Fetch /markets/{region_id}/history/ for a single type_id. Returns ([], expires) if untraded or invalid."""
    resp = await _send(client, "GET", f"/markets/{region_id}/history/", params={"type_id": type_id})
    expires = resp.headers.get("Expires")
    if resp.status_code in (400, 404):
        return [], expires
    resp.raise_for_status()
    return resp.json(), expires


async def fetch_structure_orders(
    client: httpx.AsyncClient, structure_id: int, access_token: str, known_expires: str | None = None
) -> tuple[list[dict] | None, str | None]:
    """Fetch all pages of /markets/structures/{structure_id}/ (both buy and sell, already
    scoped to that structure - no location_id filtering needed). Requires a Bearer token
    from a character with docking access and the esi-markets.structure_markets.v1 scope.
    Same known_expires short-circuit as fetch_region_orders."""
    headers = {"Authorization": f"Bearer {access_token}"}
    first = await _send(client, "GET", f"/markets/structures/{structure_id}/", params={"page": 1}, headers=headers)
    first.raise_for_status()
    expires = first.headers.get("Expires")
    if known_expires is not None and expires == known_expires:
        return None, expires

    orders = first.json()
    total_pages = int(first.headers.get("X-Pages", 1))

    if total_pages > 1:
        results = await asyncio.gather(
            *[
                _send(client, "GET", f"/markets/structures/{structure_id}/", params={"page": p}, headers=headers)
                for p in range(2, total_pages + 1)
            ]
        )
        for r in results:
            r.raise_for_status()
            orders.extend(r.json())

    return orders, expires


async def fetch_structure_info(client: httpx.AsyncClient, structure_id: int, access_token: str) -> dict:
    """GET /universe/structures/{structure_id}/ - resolves the structure's name/system. Requires
    the same docking-access token as fetch_structure_orders."""
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = await _send(client, "GET", f"/universe/structures/{structure_id}/", headers=headers)
    resp.raise_for_status()
    return resp.json()


async def fetch_adjusted_prices(
    client: httpx.AsyncClient, known_expires: str | None = None
) -> tuple[list[dict] | None, str | None]:
    """Fetch /markets/prices/ - a single unpaginated, global (not per-region) list of
    {type_id, adjusted_price, average_price} for every marketable type. Same known_expires
    short-circuit as the other fetch_* functions; this endpoint's Expires is typically weeks
    out, so in practice this almost always skips."""
    resp = await _send(client, "GET", "/markets/prices/")
    resp.raise_for_status()
    expires = resp.headers.get("Expires")
    if known_expires is not None and expires == known_expires:
        return None, expires
    return resp.json(), expires


async def fetch_names(client: httpx.AsyncClient, ids: list[int]) -> dict[int, str]:
    """Resolve IDs to names via POST /universe/names/, batched at 1000 per call."""
    out: dict[int, str] = {}
    batches = [ids[i : i + 1000] for i in range(0, len(ids), 1000)]

    async def _post(batch: list[int]) -> None:
        resp = await _send(client, "POST", "/universe/names/", json=batch)
        resp.raise_for_status()
        for entry in resp.json():
            out[entry["id"]] = entry["name"]

    await asyncio.gather(*[_post(b) for b in batches])
    return out


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.ESI_BASE,
        headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"},
        timeout=30.0,
    )
