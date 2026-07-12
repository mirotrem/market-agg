import base64
import secrets
import time
from urllib.parse import urlencode

import httpx

from . import cache, config, db


async def build_login_url() -> str:
    state = secrets.token_urlsafe(16)
    await cache.oauth_state_set(state)
    params = {
        "response_type": "code",
        "redirect_uri": config.EVE_SSO_CALLBACK_URL,
        "client_id": config.EVE_SSO_CLIENT_ID,
        "scope": " ".join(config.EVE_SSO_SCOPES),
        "state": state,
    }
    return f"{config.EVE_SSO_AUTHORIZE_URL}?{urlencode(params)}"


async def consume_state(state: str) -> bool:
    return await cache.oauth_state_consume(state)


def _basic_auth_header() -> dict[str, str]:
    creds = f"{config.EVE_SSO_CLIENT_ID}:{config.EVE_SSO_CLIENT_SECRET}".encode()
    return {"Authorization": "Basic " + base64.b64encode(creds).decode()}


async def exchange_code(code: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            config.EVE_SSO_TOKEN_URL,
            headers={**_basic_auth_header(), "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "authorization_code", "code": code},
        )
        resp.raise_for_status()
        return resp.json()


async def _refresh(refresh_token_value: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            config.EVE_SSO_TOKEN_URL,
            headers={**_basic_auth_header(), "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": refresh_token_value},
        )
        resp.raise_for_status()
        return resp.json()


async def verify_character(access_token: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://login.eveonline.com/oauth/verify",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        resp.raise_for_status()
        return resp.json()


async def complete_login(code: str) -> dict:
    """Exchange the auth code, resolve the character, and persist the token."""
    tokens = await exchange_code(code)
    character = await verify_character(tokens["access_token"])
    expires_at = time.time() + tokens["expires_in"]
    await db.save_sso_token(
        character_id=character["CharacterID"],
        character_name=character["CharacterName"],
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        expires_at=expires_at,
    )
    return character


async def get_valid_access_token(character_id: int) -> str:
    """Return a non-expired access token for this character, refreshing if needed."""
    token = await db.get_sso_token(character_id)
    if token is None:
        raise ValueError(f"No stored SSO token for character_id {character_id}; authorize via /auth/login first")

    if time.time() < float(token["expires_at"]) - 60:
        return token["access_token"]

    tokens = await _refresh(token["refresh_token"])
    expires_at = time.time() + tokens["expires_in"]
    await db.save_sso_token(
        character_id=character_id,
        character_name=token["character_name"],
        access_token=tokens["access_token"],
        refresh_token=tokens.get("refresh_token", token["refresh_token"]),
        expires_at=expires_at,
    )
    return tokens["access_token"]
