"""Monzo developer API client, and the adapter to the Enable Banking transaction shape.

https://docs.monzo.com/. The API is for personal use: a confidential OAuth client
of the account holder's own, approved in the Monzo app under Strong Customer
Authentication. The holder re-approves every REAUTH_DAYS; within five minutes
of an approval the whole history is readable, afterwards the last 90 days.

Like the Enable Banking client, this one returns the API's JSON as-is, so the
flow saves raw pages before mapping. `adapt` then turns saved Monzo transactions
into the shape `mapping.py` reads, so keys, rules and pairing are one code path
for every source. Requests are never retried; an expired access token is
refreshed once, and a refresh that fails (or a 403, the approval lapsed) means
the holder has to run `monzo-auth` again.

The token lives in the session file (STATE_DIR/sessions/<label>.json, see
`session`), not in a secret: the refresh token rotates on every refresh, so the
flow rewrites the file through `on_token`, and nothing in git changes per
re-approval. The client id and secret come from the environment.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlencode

import httpx

API_URL = "https://api.monzo.com"
AUTH_URL = "https://auth.monzo.com/"
PROVIDER = "monzo"
TIMEOUT_SECONDS = 60.0
PAGE_LIMIT = 100
# The holder re-approves access in the app every 90 days (SCA). The session's
# valid_until is set a little earlier, so the alert fires before Monzo cuts access.
REAUTH_DAYS = 90
REAUTH_MARGIN = timedelta(minutes=5)
# Refresh the access token this long before it expires.
EXPIRY_MARGIN = timedelta(seconds=60)
# The API filters on `created`, and only settled transactions are mapped, so a
# fetch starts this many days before the window: a card payment that settles
# late must still fall inside a later run's window. There is no request budget.
SETTLE_GRACE_DAYS = 30
POT_SCHEME = "uk_retail_pot"
# Minor units per currency; everything not listed has two decimals.
DECIMALS = {"JPY": 0, "KRW": 0, "ISK": 0, "CLP": 0, "VND": 0, "BHD": 3, "JOD": 3, "KWD": 3, "OMR": 3, "TND": 3}


class MonzoError(Exception):
    """A non-2xx response. `code` is the API's own code (`forbidden.insufficient_permissions`), when sent."""

    def __init__(self, status_code: int, code: str | None, message: str | None):
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(f"Monzo {status_code} {code or ''}: {message or ''}".strip().rstrip(":"))


class AccessGoneError(MonzoError):
    """No usable token: it can't be refreshed, or the holder's approval lapsed. Run `monzo-auth` again."""


@dataclass(frozen=True)
class Token:
    access_token: str
    refresh_token: str
    expires_at: datetime

    @classmethod
    def from_response(cls, body: dict, now: datetime | None = None) -> Token:
        if not body.get("refresh_token"):
            raise MonzoError(200, "no_refresh_token", "no refresh token in the response: the OAuth client must be confidential")
        now = now or datetime.now(UTC)
        return cls(body["access_token"], body["refresh_token"], now + timedelta(seconds=int(body["expires_in"])))

    @classmethod
    def from_json(cls, data: dict) -> Token:
        return cls(data["access_token"], data["refresh_token"], datetime.fromisoformat(data["expires_at"]))

    def to_json(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at.isoformat(),
        }

    def expired(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(UTC)) >= self.expires_at - EXPIRY_MARGIN


def auth_url(client_id: str, redirect_url: str, state: str) -> str:
    """Where the holder logs in (by emailed link); Monzo then redirects with `code` and `state`."""
    query = {"client_id": client_id, "redirect_uri": redirect_url, "response_type": "code", "state": state}
    return AUTH_URL + "?" + urlencode(query)


def rfc3339(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class MonzoClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token: Token | None = None,
        *,
        on_token: Callable[[Token], None] | None = None,
        base_url: str = API_URL,
    ):
        self.client_id = client_id
        self._client_secret = client_secret
        self._token = token
        # Called with every new token (exchange or refresh): the refresh token rotates.
        self.on_token = on_token
        self._http = httpx.Client(base_url=base_url, timeout=TIMEOUT_SECONDS)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> MonzoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def token(self) -> Token | None:
        return self._token

    def exchange_code(self, code: str, redirect_url: str) -> Token:
        """Turn the redirect's code into a token. Permissions follow once the holder approves in the app."""
        return self._grant({"grant_type": "authorization_code", "redirect_uri": redirect_url, "code": code})

    def refresh(self) -> Token:
        if self._token is None:
            raise AccessGoneError(0, None, "no token to refresh")
        try:
            return self._grant({"grant_type": "refresh_token", "refresh_token": self._token.refresh_token})
        except MonzoError as exc:
            raise AccessGoneError(exc.status_code, exc.code, exc.message) from exc

    def _grant(self, data: dict) -> Token:
        response = self._http.post(
            "/oauth2/token", data={"client_id": self.client_id, "client_secret": self._client_secret, **data}
        )
        if not response.is_success:
            raise _error_from(response)
        self._token = Token.from_response(response.json())
        if self.on_token is not None:
            self.on_token(self._token)
        return self._token

    def _request(self, method: str, path: str, **kwargs) -> dict:
        if self._token is None:
            raise AccessGoneError(0, None, "not authorised")
        if self._token.expired():
            self.refresh()
        response = self._send(method, path, **kwargs)
        if response.status_code == 401:
            # The token was invalidated early (a login elsewhere evicts it): one refresh, one retry.
            self.refresh()
            response = self._send(method, path, **kwargs)
        if response.is_success:
            return response.json()
        raise _error_from(response)

    def _send(self, method: str, path: str, **kwargs) -> httpx.Response:
        assert self._token is not None
        headers = {"Authorization": f"Bearer {self._token.access_token}"}
        return self._http.request(method, path, headers=headers, **kwargs)

    def whoami(self) -> dict:
        return self._request("GET", "/ping/whoami")

    def accounts(self) -> list[dict]:
        """Every account, closed ones included. 403 until the holder approves in the app."""
        return self._request("GET", "/accounts")["accounts"]

    def pots(self, account_id: str) -> list[dict]:
        return self._request("GET", "/pots", params={"current_account_id": account_id})["pots"]

    def transaction_pages(self, account_id: str, since: datetime, before: datetime) -> Iterator[dict]:
        """Yield each raw page of transactions created in [since, before), oldest first.

        Pages are yielded unmodified so the caller can persist each one before the
        next request. The API pages by `since=<last id>`; a page shorter than
        PAGE_LIMIT is the last.
        """
        params: dict = {
            "account_id": account_id,
            "since": rfc3339(since),
            "before": rfc3339(before),
            "limit": PAGE_LIMIT,
            "expand[]": "merchant",
        }
        first = True
        while True:
            page = self._request("GET", "/transactions", params=params)
            transactions = page.get("transactions") or []
            if transactions or first:
                yield page
            first = False
            if len(transactions) < PAGE_LIMIT:
                return
            last = transactions[-1]["id"]
            if last == params["since"]:
                raise MonzoError(200, None, f"pagination repeated transaction {last}")
            params = {**params, "since": last}


def _error_from(response: httpx.Response) -> MonzoError:
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    code = body.get("code") or body.get("error")
    message = body.get("message") or body.get("error_description")
    if response.status_code == 403:
        # No permissions on this token: the holder hasn't approved yet, or the 90 days are over.
        return AccessGoneError(response.status_code, code, message)
    return MonzoError(response.status_code, code, message)


# Session file: the shape flow.py reads for every provider, plus the token and the pots.


def account_entry(account: dict) -> dict:
    """A session account: `uid` is the Monzo account id, `number` the sort code and account number, digits only.

    The Firefly asset account carries `number` as its account number (no IBAN), and
    so does a counterparty that is this account on another own account's booking.
    """
    sort_code = "".join(ch for ch in (account.get("sort_code") or "") if ch.isdigit())
    number = "".join(ch for ch in (account.get("account_number") or "") if ch.isdigit())
    if not (sort_code and number):
        raise MonzoError(200, None, f"account {account.get('id')} has no sort code and account number")
    return {
        "uid": account["id"],
        "number": sort_code + number,
        "currency": account.get("currency") or "GBP",
        "type": account.get("type"),
        "created": account.get("created"),
    }


def pot_entry(pot: dict) -> dict:
    return {"id": pot["id"], "name": pot.get("name") or pot["id"], "deleted": bool(pot.get("deleted"))}


def session(token: Token, accounts: list[dict], pots: list[dict], authorised_at: datetime) -> dict:
    """The session file for a Monzo login, with `access.valid_until` the re-approval deadline."""
    valid_until = authorised_at + timedelta(days=REAUTH_DAYS) - REAUTH_MARGIN
    return {
        "provider": PROVIDER,
        "token": token.to_json(),
        "authorised_at": authorised_at.isoformat(),
        "access": {"valid_until": valid_until.isoformat()},
        "accounts": [account_entry(a) for a in accounts if not a.get("closed")],
        "pots": [pot_entry(p) for p in pots],
    }


# Adapter: one Monzo transaction to the Enable Banking transaction shape mapping.py reads.


def settled_day(tx: dict) -> str | None:
    """The settlement date, the booking date of the adapted transaction; None while pending."""
    settled = tx.get("settled") or ""
    return settled[:10] or None


def amount_text(minor: int, currency: str) -> str:
    """Unsigned major units from Monzo's integer minor units, as Enable Banking formats them."""
    return format(Decimal(abs(int(minor))).scaleb(-DECIMALS.get(currency.upper(), 2)), "f")


def pot_of(tx: dict) -> str | None:
    """The pot id of a move between the account and one of its pots, else None."""
    pot = (tx.get("metadata") or {}).get("pot_id")
    description = str(tx.get("description") or "")
    if pot:
        return pot
    if tx.get("scheme") == POT_SCHEME or description.startswith("pot_"):
        return description or None
    return None


def to_raw(tx: dict, pot_names: dict[str, str]) -> dict | None:
    """The Enable Banking shape of one Monzo transaction; None for one not to map.

    Declined and still-pending transactions are left out: a pending amount can
    still change, and the key must not. The `tx_` id becomes `entry_reference`
    (Firefly's internal_reference, never the key). The counterparty's sort code
    and account number, or the pot id, go in `other.identification`, where the
    mapping looks own accounts up by number. A card payment's `local_amount` in
    another currency becomes the instructed amount, Firefly's foreign amount.
    """
    day = settled_day(tx)
    if tx.get("decline_reason") or day is None:
        return None
    minor = int(tx["amount"])
    currency = tx["currency"]
    debit = minor < 0
    raw: dict = {
        "entry_reference": tx["id"],
        "status": "BOOK",
        "booking_date": day,
        "value_date": str(tx.get("created") or "")[:10] or day,
        "transaction_amount": {"amount": amount_text(minor, currency), "currency": currency},
        "credit_debit_indicator": "DBIT" if debit else "CRDT",
    }
    party = tx.get("counterparty") or {}
    merchant = tx.get("merchant") if isinstance(tx.get("merchant"), dict) else {}
    pot = pot_of(tx)
    sort_code = "".join(ch for ch in (party.get("sort_code") or "") if ch.isdigit())
    account_number = "".join(ch for ch in (party.get("account_number") or "") if ch.isdigit())
    name: str | None
    account: dict | None = None
    if pot:
        name = pot_names.get(pot) or pot
        account = {"other": {"identification": pot, "scheme_name": "MONZO_POT"}}
    else:
        name = (party.get("name") or party.get("preferred_name") or merchant.get("name") or "").strip() or None
        if sort_code and account_number:
            account = {"other": {"identification": sort_code + account_number, "scheme_name": "UK_SORT_CODE_ACCOUNT_NUMBER"}}
    side = "creditor" if debit else "debtor"
    if name:
        raw[side] = {"name": name}
    if account:
        raw[f"{side}_account"] = account
    # The holder's note, else the payment reference. A card payment's description
    # is the raw merchant string; the merchant's name says it better.
    text = str(tx.get("notes") or "").strip()
    if not text and not merchant and not pot:
        text = str(tx.get("description") or "").strip()
    if text:
        raw["remittance_information"] = [text]
    local_currency = str(tx.get("local_currency") or "").upper()
    if local_currency and tx.get("local_amount") is not None and local_currency != currency.upper():
        raw["exchange_rate"] = {
            "instructed_amount": {"amount": amount_text(int(tx["local_amount"]), local_currency), "currency": local_currency}
        }
    return raw


def adapt(transactions: list[dict], pot_names: dict[str, str]) -> list[dict]:
    """Saved Monzo transactions in the Enable Banking shape, in order, without the ones not to map."""
    adapted = []
    for tx in transactions:
        raw = to_raw(tx, pot_names)
        if raw is not None:
            adapted.append(raw)
    return adapted
