"""Firefly III API v1 client (https://api-docs.firefly-iii.org/).

Small on purpose: the flow needs the own accounts (to recognise transfers) and an
idempotent store. Idempotency lives in Firefly, not in Prefect state: every
imported transaction carries the Enable Banking `entry_reference` as its
`external_id`, and `store_if_absent` looks that up before creating anything.

Used by the flow:

    with FireflyClient(url, token) as firefly:
        accounts = firefly.accounts("asset")      # raw API `data` items
        result = firefly.store_if_absent(payload)  # payload from mapping.map_transaction
        result.created, result.group_id

An existing transaction is never updated. When one with the same `external_id`
exists but books a different amount, `ExternalIdConflictError` is raised instead
of skipping it quietly: either the bank reused a reference or the mapping
changed, and both need a human.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import httpx

TIMEOUT_SECONDS = 30.0
PAGE_LIMIT = 100


class FireflyError(Exception):
    """A non-2xx response. `errors` holds Firefly's per-field validation messages (422)."""

    def __init__(self, status_code: int, message: str, errors: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.errors = errors or {}
        super().__init__(f"Firefly {status_code}: {message} {self.errors or ''}".strip())


class ExternalIdConflictError(Exception):
    """Firefly already holds a different transaction under this external_id."""


@dataclass(frozen=True)
class StoreResult:
    created: bool
    group_id: str


class FireflyClient:
    def __init__(self, url: str, token: str, *, transport: httpx.BaseTransport | None = None):
        self._http = httpx.Client(
            base_url=url.rstrip("/") + "/api",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.api+json",
            },
            timeout=TIMEOUT_SECONDS,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> FireflyClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _request(self, method: str, path: str, **kwargs) -> dict:
        response = self._http.request(method, path, **kwargs)
        if response.is_success:
            return response.json()
        try:
            body = response.json()
        except ValueError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        raise FireflyError(response.status_code, body.get("message") or response.reason_phrase, body.get("errors"))

    def _paged(self, path: str, params: dict) -> list[dict]:
        items: list[dict] = []
        page = 1
        while True:
            body = self._request("GET", path, params={**params, "limit": PAGE_LIMIT, "page": page})
            items.extend(body.get("data", []))
            total_pages = body.get("meta", {}).get("pagination", {}).get("total_pages", 1)
            if page >= total_pages:
                return items
            page += 1

    def accounts(self, account_type: str = "asset") -> list[dict]:
        """All accounts of one type (`asset`, `liabilities`, …) as raw `data` items."""
        return self._paged("/v1/accounts", {"type": account_type})

    def find_by_external_id(self, external_id: str) -> dict | None:
        """The transaction group whose split carries exactly this external_id, or None.

        Firefly's search does the narrowing; the exact comparison happens here, so
        a looser search match never counts as a hit.
        """
        if '"' in external_id or "\\" in external_id:
            # Can't be quoted safely in a search query; a missed lookup would mean a duplicate.
            raise ValueError(f"external_id {external_id!r} contains a quote or backslash")
        groups = self._paged("/v1/search/transactions", {"query": f'external_id_is:"{external_id}"'})
        hits = [
            g
            for g in groups
            if any(s.get("external_id") == external_id for s in g["attributes"]["transactions"])
        ]
        if len(hits) > 1:
            ids = ", ".join(g["id"] for g in hits)
            raise ExternalIdConflictError(f"external_id {external_id!r} is on several groups: {ids}")
        return hits[0] if hits else None

    def store_if_absent(self, payload: dict) -> StoreResult:
        """Create the transaction unless one with its external_id exists. Never updates."""
        split = _single_split(payload)
        external_id = split.get("external_id")
        if not external_id:
            raise ValueError("payload has no external_id: refusing a store that can't be made idempotent")
        existing = self.find_by_external_id(external_id)
        if existing is not None:
            _check_same_booking(existing, split)
            return StoreResult(created=False, group_id=existing["id"])
        body = self._request("POST", "/v1/transactions", json=payload)
        return StoreResult(created=True, group_id=body["data"]["id"])


def _single_split(payload: dict) -> dict:
    splits = payload.get("transactions") or []
    if len(splits) != 1:
        raise ValueError(f"expected one split per transaction, got {len(splits)}")
    return splits[0]


def _check_same_booking(existing: dict, split: dict) -> None:
    # Only the amount is compared: descriptions, categories and the like are
    # edited by rules and by hand after import, and those edits must survive.
    found = next(s for s in existing["attributes"]["transactions"] if s.get("external_id") == split["external_id"])
    if Decimal(found["amount"]) != Decimal(split["amount"]):
        raise ExternalIdConflictError(
            f"external_id {split['external_id']!r} exists in group {existing['id']} "
            f"with amount {found['amount']}, the bank now says {split['amount']}"
        )
