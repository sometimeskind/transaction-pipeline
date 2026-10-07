"""Map one raw Enable Banking transaction to a Firefly III store payload.

Input is a transaction exactly as Enable Banking returns it (the `transactions`
items of GET /accounts/{uid}/transactions, see
https://enablebanking.com/docs/api/reference/), so the flow can map straight from
the raw pages saved on the state PVC. Output is the body for Firefly's
POST /v1/transactions, ready for `FireflyClient.store_if_absent`.

Used by the flow:

    book = AccountBook.from_firefly(firefly.accounts("asset"), imported_ibans=[...])
    own = book.get(session_account["account_id"]["iban"])   # this feed's account
    payload = map_transaction(raw_transaction, own, book)   # dict, or None to skip

`imported_ibans` are the IBANs of the accounts this run fetches from Enable
Banking. They decide which side of a transfer between own accounts is imported:

- A debit whose counterparty is an own account becomes one Firefly transfer.
- The matching credit on the other account returns None when that account is
  imported too, because its debit already makes the transfer. When the other
  account is only in Firefly (not fetched), the credit is the only side we ever
  see, so it becomes the transfer.

`map_transaction` also returns None for a zero amount, which Firefly rejects.
Anything it can't map safely (no `entry_reference`, no booking date, no
direction) raises `MappingError`, so the flow fails rather than guessing.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

log = logging.getLogger(__name__)

DESCRIPTION_MAX = 1000
NO_DESCRIPTION = "(no description)"


class MappingError(Exception):
    """The raw transaction can't be mapped without guessing."""


@dataclass(frozen=True)
class OwnAccount:
    firefly_id: str
    name: str
    iban: str
    imported: bool


class AccountBook:
    """Own Firefly accounts, indexed by normalised IBAN."""

    def __init__(self, accounts: Iterable[OwnAccount]):
        self._by_iban = {a.iban: a for a in accounts}

    @classmethod
    def from_firefly(cls, accounts: Iterable[dict], imported_ibans: Iterable[str]) -> AccountBook:
        """Build from Firefly `data` items (FireflyClient.accounts). Accounts without an IBAN are left out."""
        imported = {normalise_iban(i) for i in imported_ibans}
        own = []
        for item in accounts:
            iban = normalise_iban(item["attributes"].get("iban") or "")
            if iban:
                own.append(OwnAccount(item["id"], item["attributes"]["name"], iban, iban in imported))
        missing = imported - {a.iban for a in own}
        if missing:
            raise MappingError(f"{len(missing)} imported account(s) have no Firefly account with that IBAN")
        return cls(own)

    def get(self, iban: str | None) -> OwnAccount | None:
        return self._by_iban.get(normalise_iban(iban or ""))


def normalise_iban(iban: str) -> str:
    return "".join(iban.split()).upper()


def map_transaction(raw: dict, account: OwnAccount, book: AccountBook) -> dict | None:
    """The Firefly store payload for `raw`, booked on `account`; None to skip it (see module docstring)."""
    external_id = raw.get("entry_reference")
    if not external_id:
        raise MappingError("transaction has no entry_reference, so it can't be stored idempotently")
    booking_date = raw.get("booking_date") or raw.get("value_date")
    if not booking_date:
        raise MappingError(f"transaction {external_id} has no booking_date or value_date")

    money = raw["transaction_amount"]
    amount = Decimal(money["amount"])
    debit = _is_debit(raw.get("credit_debit_indicator"), amount, external_id)
    amount = abs(amount)
    if amount == 0:
        log.info("skipping %s: zero amount", external_id)
        return None

    party, party_account = (
        (raw.get("creditor"), raw.get("creditor_account"))
        if debit
        else (raw.get("debtor"), raw.get("debtor_account"))
    )
    party_name = ((party or {}).get("name") or "").strip() or None
    party_iban, party_number = _account_identifiers(party_account)
    other_own = book.get(party_iban)
    if other_own is not None and other_own.iban == account.iban:
        other_own = None

    split: dict = {
        "date": booking_date,
        "amount": str(amount),
        "currency_code": money["currency"],
        "description": _description(raw, party_name),
        "external_id": external_id,
    }
    split.update(_foreign(raw, money["currency"]))

    if other_own is not None:
        if not debit and other_own.imported:
            log.info("skipping %s: credit side of a transfer from %s, imported from its debit side", external_id, other_own.name)
            return None
        source, destination = (account, other_own) if debit else (other_own, account)
        split.update(type="transfer", source_id=source.firefly_id, destination_id=destination.firefly_id)
    elif debit:
        split.update(type="withdrawal", source_id=account.firefly_id)
        split.update(_counterparty("destination", party_name, party_iban, party_number))
    else:
        split.update(type="deposit", destination_id=account.firefly_id)
        split.update(_counterparty("source", party_name, party_iban, party_number))

    return {
        "error_if_duplicate_hash": False,
        "apply_rules": True,
        "fire_webhooks": True,
        "transactions": [split],
    }


def _is_debit(indicator: str | None, amount: Decimal, external_id: str) -> bool:
    if indicator == "DBIT":
        return True
    if indicator == "CRDT":
        return False
    # Without an indicator, only a signed amount says which way the money went.
    if amount < 0:
        return True
    raise MappingError(f"transaction {external_id} has no credit_debit_indicator and an unsigned amount")


def _account_identifiers(account: dict | None) -> tuple[str | None, str | None]:
    """(IBAN, other account number) from an AccountIdentification."""
    if not account:
        return None, None
    iban = normalise_iban(account.get("iban") or "") or None
    other = account.get("other") or {}
    number = other.get("identification") or account.get("identification") or None
    return iban, number


def _counterparty(side: str, name: str | None, iban: str | None, number: str | None) -> dict:
    # Firefly creates the expense/revenue account by name when it doesn't exist.
    # With no name at all it books against its cash account.
    fields = {f"{side}_name": name or iban or number}
    if iban:
        fields[f"{side}_iban"] = iban
    elif number:
        fields[f"{side}_number"] = number
    return {k: v for k, v in fields.items() if v}


def _description(raw: dict, party_name: str | None) -> str:
    """Stable: the same raw transaction always gives the same text."""
    remittance = raw.get("remittance_information") or []
    text = " ".join(" ".join(remittance).split())
    if not text:
        text = party_name or ((raw.get("bank_transaction_code") or {}).get("description") or "").strip()
    return (text or NO_DESCRIPTION)[:DESCRIPTION_MAX]


def _foreign(raw: dict, currency: str) -> dict:
    instructed = (raw.get("exchange_rate") or {}).get("instructed_amount") or {}
    if not instructed.get("amount") or not instructed.get("currency") or instructed["currency"] == currency:
        return {}
    return {
        "foreign_amount": str(abs(Decimal(instructed["amount"]))),
        "foreign_currency_code": instructed["currency"],
    }
