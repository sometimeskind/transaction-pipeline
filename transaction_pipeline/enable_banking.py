"""Enable Banking API client (https://enablebanking.com/docs/api/reference/).

Thin on purpose: it returns the API's JSON as-is, so the flow can save every raw
transaction page to the state PVC before mapping. It never retries. Each
transactions call spends one of the 4 unattended requests per account per 24h
(PSD2 SCA RTS art. 36(5)), so a retry is the flow's decision, not the client's.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

BASE_URL = "https://api.enablebanking.com"
# The API rejects tokens that live longer than 24h; one hour is plenty for a run.
TOKEN_TTL_SECONDS = 3600
# Calls that reach the bank (ASPSP) can be slow.
TIMEOUT_SECONDS = 60.0

# A session in one of these states needs the operator to consent again.
SESSION_GONE_CODES = frozenset(
    {"EXPIRED_SESSION", "CLOSED_SESSION", "REVOKED_SESSION", "SESSION_DOES_NOT_EXIST"}
)


class EnableBankingError(Exception):
    """A non-2xx response. `error_code` is the API's own code, when it sent one."""

    def __init__(self, status_code: int, error_code: str | None, description: str | None):
        self.status_code = status_code
        self.error_code = error_code
        self.description = description
        super().__init__(f"Enable Banking {status_code} {error_code or ''}: {description or ''}".strip())


class SessionGoneError(EnableBankingError):
    """The session expired, was closed or revoked: re-consent is needed."""


class RateLimitedError(EnableBankingError):
    """The bank's request budget is spent. Retrying makes it worse."""


class ConsentRedirectError(Exception):
    """The redirect URL pasted back by the operator is unusable."""


@dataclass(frozen=True)
class AuthStart:
    url: str
    authorization_id: str


def load_private_key(path: Path) -> RSAPrivateKey:
    key = load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, RSAPrivateKey):
        raise ValueError(f"{path} is not an RSA private key (Enable Banking needs RS256)")
    return key


def make_jwt(app_id: str, private_key: RSAPrivateKey, now: int | None = None) -> str:
    iat = int(time.time()) if now is None else now
    return jwt.encode(
        {
            "iss": "enablebanking.com",
            "aud": "api.enablebanking.com",
            "iat": iat,
            "exp": iat + TOKEN_TTL_SECONDS,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": app_id},
    )


def code_from_redirect(redirect_url: str, expected_state: str) -> str:
    """Pull the authorization code out of the redirect URL the operator pastes back."""
    query = parse_qs(urlsplit(redirect_url).query)
    if "error" in query:
        detail = query.get("error_description", [""])[0]
        raise ConsentRedirectError(f"bank returned {query['error'][0]}: {detail}".rstrip(": "))
    if query.get("state", [None])[0] != expected_state:
        raise ConsentRedirectError("state does not match this consent request")
    code = query.get("code", [None])[0]
    if not code:
        raise ConsentRedirectError("no code in the redirect URL")
    return code


class EnableBankingClient:
    def __init__(
        self,
        app_id: str,
        private_key: RSAPrivateKey,
        *,
        base_url: str = BASE_URL,
    ):
        self._app_id = app_id
        self._private_key = private_key
        self._http = httpx.Client(base_url=base_url, timeout=TIMEOUT_SECONDS)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> EnableBankingClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> dict:
        # A fresh token per request: signing is cheap, and a long run never
        # outlives its token.
        headers = {"Authorization": f"Bearer {make_jwt(self._app_id, self._private_key)}"}
        response = self._http.request(method, path, headers=headers, **kwargs)
        if response.is_success:
            return response.json()
        raise _error_from(response)

    def list_aspsps(self, country: str, psu_type: str = "personal") -> list[dict]:
        return self._request("GET", "/aspsps", params={"country": country, "psu_type": psu_type})["aspsps"]

    def start_auth(
        self,
        *,
        aspsp_name: str,
        aspsp_country: str,
        redirect_url: str,
        valid_until: datetime,
        state: str,
        psu_type: str = "personal",
    ) -> AuthStart:
        body = self._request(
            "POST",
            "/auth",
            json={
                "access": {"valid_until": valid_until.isoformat()},
                "aspsp": {"name": aspsp_name, "country": aspsp_country},
                "state": state,
                "redirect_url": redirect_url,
                "psu_type": psu_type,
            },
        )
        return AuthStart(url=body["url"], authorization_id=body["authorization_id"])

    def create_session(self, code: str) -> dict:
        """Exchange the redirect's code for a session (accounts, `access.valid_until`)."""
        return self._request("POST", "/sessions", json={"code": code})

    def get_session(self, session_id: str) -> dict:
        return self._request("GET", f"/sessions/{session_id}")

    def transaction_pages(self, account_uid: str, date_from: date, date_to: date) -> Iterator[dict]:
        """Yield each raw page of booked transactions, following `continuation_key`.

        Pages are yielded unmodified so the caller can persist each one before the
        next request. Follow-up pages don't count against the PSD2 budget; the
        first request does.
        """
        params = {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "transaction_status": "BOOK",
        }
        seen_keys: set[str] = set()
        while True:
            page = self._request("GET", f"/accounts/{account_uid}/transactions", params=params)
            yield page
            key = page.get("continuation_key")
            if not key:
                return
            if key in seen_keys:
                raise EnableBankingError(200, None, f"continuation_key repeated after {len(seen_keys)} pages")
            seen_keys.add(key)
            params = {**params, "continuation_key": key}


def _error_from(response: httpx.Response) -> EnableBankingError:
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    code = body.get("error_code") or body.get("error")
    description = body.get("error_description") or body.get("message")
    if code in SESSION_GONE_CODES:
        return SessionGoneError(response.status_code, code, description)
    if code == "ASPSP_RATE_LIMIT_EXCEEDED" or response.status_code == 429:
        return RateLimitedError(response.status_code, code, description)
    return EnableBankingError(response.status_code, code, description)
