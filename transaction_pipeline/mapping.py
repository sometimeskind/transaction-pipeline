"""Map one raw Enable Banking transaction to a Firefly III store payload.

Input is a transaction exactly as Enable Banking returns it (the `transactions`
items of GET /accounts/{uid}/transactions, see
https://enablebanking.com/docs/api/reference/), so the flow can map straight from
the raw pages saved on the state PVC. Output is the body for Firefly's
POST /v1/transactions, ready for `FireflyClient.store_if_absent`.

Used by the flow:

    rules = load_rules()                                     # MAPPING_CONFIG; [] when the file is absent
    book = AccountBook.from_firefly(firefly.accounts("asset"), imported_ibans=[...], rules=rules)
    own = book.get(session_account["account_id"]["iban"])   # this feed's account
    payload = map_transaction(raw_transaction, own, book)   # dict, or None to skip

A counterparty is an own account, and the booking a transfer, when its IBAN is
on a Firefly asset account, or else when it matches a rule from the rules file.
Rules cover own accounts that bookings don't name by IBAN, such as sub-accounts
inside a bank account or a payment service in the middle of a transfer. The
file is TOML at `MAPPING_CONFIG` (default /config/mapping.toml), optional: no
file means no rules. The deployment mounts it from a secret; the real accounts
never go in this repo. Rules are checked in order:

    [[own_counterparty]]
    account = "Holiday pot"           # Firefly asset account name
    name = "^holiday pot$"            # regex on the counterparty name
    remittance = "pot transfer"       # regex on the remittance text
    iban = "XX00..."                  # optional, see below

A rule needs `account` and at least one of `name` and `remittance`; all of the
patterns it gives must match (searched, case-insensitive). The payer chooses the
name and remittance text, so a rule without `iban` only applies when the
counterparty shows no IBAN or the booking account's own: a stranger's payment
carries the stranger's IBAN and can't pose as a sub-account move. Give `iban`
for an intermediary whose IBAN does appear (the rule then needs that exact one).

`imported_ibans` are the IBANs of the accounts this run fetches from Enable
Banking. They decide which side of a transfer between own accounts is imported:

- A debit whose counterparty is an own account becomes one Firefly transfer.
- The matching credit on the other account returns None when that account is
  imported too, because its debit already makes the transfer. When the other
  account is only in Firefly (not fetched), the credit is the only side we ever
  see, so it becomes the transfer.

The Firefly `external_id`, the idempotency key, is the `entry_reference`, or the
`transaction_id` when the bank sends no `entry_reference`.

`map_transaction` also returns None for a zero amount, which Firefly rejects.
Anything it can't map safely (neither `entry_reference` nor `transaction_id`, no
booking date, no direction) raises `MappingError`, so the flow fails rather than
guessing.
"""

from __future__ import annotations

import logging
import os
import re
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_RULES_PATH = "/config/mapping.toml"
DESCRIPTION_MAX = 1000
NO_DESCRIPTION = "(no description)"


class MappingError(Exception):
    """The raw transaction can't be mapped without guessing."""


@dataclass(frozen=True)
class OwnAccount:
    firefly_id: str
    name: str
    iban: str | None
    imported: bool


@dataclass(frozen=True)
class CounterpartyRule:
    """A counterparty matching these patterns is the own account named `account`."""

    account: str
    name: re.Pattern | None = None
    remittance: re.Pattern | None = None
    iban: str | None = None

    def matches(self, party_name: str | None, remittance: str) -> bool:
        if self.name is not None and not self.name.search(party_name or ""):
            return False
        if self.remittance is not None and not self.remittance.search(remittance):
            return False
        return True


def load_rules(path: Path | None = None) -> list[CounterpartyRule]:
    """Rules from the TOML file at `path`, else at MAPPING_CONFIG (format in the module docstring).

    Returns [] when the file doesn't exist.
    """
    if path is None:
        path = Path(os.environ.get("MAPPING_CONFIG") or DEFAULT_RULES_PATH)
    if not path.exists():
        log.info("no mapping rules at %s", path)
        return []
    rules = []
    for i, entry in enumerate(tomllib.loads(path.read_text()).get("own_counterparty", [])):
        unknown = set(entry) - {"account", "name", "remittance", "iban"}
        if unknown or "account" not in entry or not ({"name", "remittance"} & set(entry)):
            raise MappingError(
                f"{path}: own_counterparty[{i}] needs account plus name and/or remittance"
                + (f", and has unknown keys {sorted(unknown)}" if unknown else "")
            )
        rules.append(
            CounterpartyRule(
                account=entry["account"],
                name=re.compile(entry["name"], re.IGNORECASE) if "name" in entry else None,
                remittance=re.compile(entry["remittance"], re.IGNORECASE) if "remittance" in entry else None,
                iban=normalise_iban(entry["iban"]) if "iban" in entry else None,
            )
        )
    return rules


class AccountBook:
    """Own Firefly accounts, found by normalised IBAN or by counterparty rule."""

    def __init__(self, accounts: Iterable[OwnAccount], rules: Iterable[tuple[CounterpartyRule, OwnAccount]] = ()):
        accounts = list(accounts)
        self._by_iban = {a.iban: a for a in accounts if a.iban}
        self._rules = list(rules)

    @classmethod
    def from_firefly(
        cls,
        accounts: Iterable[dict],
        imported_ibans: Iterable[str],
        rules: Iterable[CounterpartyRule] = (),
    ) -> AccountBook:
        """Build from Firefly `data` items (FireflyClient.accounts) and the rules from `load_rules`."""
        imported = {normalise_iban(i) for i in imported_ibans}
        own = []
        for item in accounts:
            iban = normalise_iban(item["attributes"].get("iban") or "") or None
            own.append(OwnAccount(item["id"], item["attributes"]["name"], iban, iban in imported))
        missing = imported - {a.iban for a in own}
        if missing:
            raise MappingError(f"{len(missing)} imported account(s) have no Firefly account with that IBAN")
        by_name = {a.name: a for a in own}
        resolved = []
        for rule in rules:
            if rule.account not in by_name:
                raise MappingError(f"rule names Firefly account {rule.account!r}, which doesn't exist")
            resolved.append((rule, by_name[rule.account]))
        return cls(own, resolved)

    def get(self, iban: str | None) -> OwnAccount | None:
        return self._by_iban.get(normalise_iban(iban or ""))

    def own_counterparty(
        self, account: OwnAccount, party_name: str | None, party_iban: str | None, remittance: str
    ) -> OwnAccount | None:
        """The own account on the other side of a booking on `account`, if it is one.

        A match on `account` itself doesn't count: some banks show the account's own
        IBAN on moves to and from its sub-accounts, and those are what rules are for.
        """
        by_iban = self.get(party_iban)
        if by_iban is not None and by_iban != account:
            return by_iban
        party_iban = normalise_iban(party_iban or "") or None
        for rule, own in self._rules:
            expected = {rule.iban} if rule.iban else {None, account.iban}
            if own != account and party_iban in expected and rule.matches(party_name, remittance):
                return own
        return None


def normalise_iban(iban: str) -> str:
    return "".join(iban.split()).upper()


def map_transaction(raw: dict, account: OwnAccount, book: AccountBook) -> dict | None:
    """The Firefly store payload for `raw`, booked on `account`; None to skip it (see module docstring)."""
    external_id = raw.get("entry_reference") or raw.get("transaction_id")
    if not external_id:
        raise MappingError(
            "transaction has neither entry_reference nor transaction_id, so it can't be stored idempotently"
        )
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
    other_own = book.own_counterparty(account, party_name, party_iban, _remittance(raw))

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


def _remittance(raw: dict) -> str:
    return " ".join(" ".join(raw.get("remittance_information") or []).split())


def _description(raw: dict, party_name: str | None) -> str:
    """Stable: the same raw transaction always gives the same text."""
    text = _remittance(raw)
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
