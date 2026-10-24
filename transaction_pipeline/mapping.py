"""Map one raw Enable Banking transaction to a Firefly III store payload.

Input is a transaction exactly as Enable Banking returns it (the `transactions`
items of GET /accounts/{uid}/transactions, see
https://enablebanking.com/docs/api/reference/), so the flow can map straight from
the raw pages saved on the state PVC. Output is the body for Firefly's
POST /v1/transactions, ready for `FireflyClient.store_if_absent`.

Used by the flow:

    rules = load_rules()                                     # MAPPING_CONFIG; [] when the file is absent
    book = AccountBook.from_firefly(firefly.accounts("asset"), imported_ibans=[...], rules=rules,
                                    imported_numbers=[...])
    own = book.get(iban) or book.get_by_number(identification_hash)  # this feed's account
    ids = external_ids(account_key, raw_transactions)         # one per booking, all of the run's pages
    pairing = pair_internal_moves(bookings)                   # Booking(own, raw, id) of every account
    payload = map_transaction(raw_transaction, own, book, ids[i], pairing.partner.get(ids[i]))

A counterparty is an own account, and the booking a transfer, when its IBAN is
on a Firefly asset account, or its other account identification (a sort code and
account number, a Monzo pot id) is a Firefly asset account's account number, else
when the booking pairs with an internal move on another imported account (below),
or else when it matches a rule from the rules file.
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

Two optional rule keys cover moves the bank shows on one side only:

    booked_on = "Round-up pot"        # only bookings on this Firefly account
    other_side_missing = true         # the credit is the transfer: no debit ever appears

Some banks book a move between two own accounts as a SEPA credit transfer
(`ICDT/ESCT` out, `RCDT/ESCT` in) whose counterparty has no IBAN or account
number, often not even a name, on both sides (N26 Spaces). Such an internal move
is identified by its other side, not its text: `pair_internal_moves` pairs an
outgoing and an incoming one on two accounts with the same booking_date, amount
and currency, and the same remittance text or none on one side. Where several
bookings could pair, each group of the same day, amount and currency is paired
as a maximum matching, preferring equal texts, and only when every such matching
gives the same transfers between accounts: which of two bookings on one account
goes with which credit then makes no difference. An internal move without a
counterparty name that neither pairs nor matches a rule raises
`UnpairedMoveError` instead of being booked against the cash account; one with
a name is mapped as usual.

A nameless booking with those codes that is no move at all (a bank's own
refund) gets a counterparty name from a `[[counterparty]]` rule, after
pairing and the `[[own_counterparty]]` rules, and is then mapped as usual:

    [[counterparty]]
    name = "Example Bank"             # the counterparty name to book it against
    remittance = "^fee refund"        # regex on the remittance text, required
    booked_on = "Checking"            # optional, as above

`imported_ibans` are the IBANs of the accounts this run fetches from Enable
Banking. Some banks list sub-accounts without an IBAN (N26 Spaces); those are
keyed on Enable Banking's `identification_hash`, which is stable across
sessions, and matched to the Firefly asset account whose account number is that
hash: `imported_numbers`. Together they decide which side of a transfer between
own accounts is imported:

- A debit whose counterparty is an own account becomes one Firefly transfer.
- The matching credit on the other account returns None when that account is
  imported too, because its debit already makes the transfer. When the other
  account is only in Firefly (not fetched), the credit is the only side we ever
  see, so it becomes the transfer.
- A transfer with an own account that is not imported and keeps another
  currency (a payment service in the middle, such as Wise between a GBP and a
  EUR account) is a *pass-through* transfer: Firefly needs the amount in that
  account's currency too (`foreign_amount`), and the bank data has only this
  side's. Unless the bank sent an instructed amount, `map_transaction` raises
  `PassThroughError`, and the flow calls it again with `leg_amount`: the amount
  of that account's other leg, found in Firefly (flow.store_pass_through). For a
  debit it becomes the foreign amount; for a credit the leg amount is the
  transfer's amount, in the source account's currency, and the booked amount the
  foreign one. Two imported accounts in different currencies are not handled:
  Firefly would reject the transfer, loudly.

The Firefly `external_id`, the idempotency key, is derived from the booking's
content by `external_ids`: `tp1:` and a SHA-256 hex digest of the account key
(IBAN, else identification_hash), booking_date, amount, currency, direction, and
the booking's ordinal among the bookings with those same fields on that account
and day, counted across all of the run's pages in the order they list them.
Only fields that can't change once a booking is final go in; counterparty and
remittance text, value_date and bank codes stay out. The bank's references
don't go in either: N26 sends no `entry_reference` for most bookings, and a
booking fetched once with one and once without would get two keys. The
`entry_reference`, when there is one, goes into the Firefly `internal_reference`.

`map_transaction` also returns None for a zero amount, which Firefly rejects.
Anything it can't key or map safely (no booking date, no amount, no direction)
raises `MappingError`, so the flow fails rather than guessing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tomllib
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_RULES_PATH = "/config/mapping.toml"
DESCRIPTION_MAX = 1000
# Bump when the content key changes, so keys of the old scheme are recognisable.
KEY_VERSION = "tp1"
NO_DESCRIPTION = "(no description)"


class MappingError(Exception):
    """The raw transaction can't be mapped without guessing."""


class UnpairedMoveError(MappingError):
    """An internal move without a counterparty name matched no other booking and no rule."""


class PassThroughError(MappingError):
    """A transfer with a pass-through account needs that account's amount: call again with `leg_amount`."""

    def __init__(self, account: OwnAccount, debit: bool, external_id: str):
        self.account = account
        self.debit = debit
        super().__init__(
            f"{external_id}: transfer {'to' if debit else 'from'} {account.name} ({account.currency}) needs the "
            f"amount in {account.currency}, which this booking doesn't carry"
        )


@dataclass(frozen=True)
class OwnAccount:
    firefly_id: str
    name: str
    iban: str | None
    imported: bool
    # Firefly's account_number: for an account without an IBAN, an identification_hash,
    # a sort code and account number, or a Monzo pot id.
    number: str | None = None
    # Firefly's currency_code; decides whether a transfer with this account is a pass-through one.
    currency: str | None = None


@dataclass(frozen=True)
class CounterpartyRule:
    """A counterparty matching these patterns is the own account named `account`.

    A `[[counterparty]]` rule has no `account`: it gives a nameless booking the
    counterparty name `counterparty` instead, so it is booked as usual.
    """

    account: str | None
    name: re.Pattern | None = None
    remittance: re.Pattern | None = None
    iban: str | None = None
    # Firefly name of the only account whose bookings the rule applies to.
    booked_on: str | None = None
    # The other side never shows in the bank data, so a matching credit is the transfer.
    other_side_missing: bool = False
    counterparty: str | None = None

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
    config = tomllib.loads(path.read_text())
    rules = []
    for i, entry in enumerate(config.get("own_counterparty", [])):
        unknown = set(entry) - {"account", "name", "remittance", "iban", "booked_on", "other_side_missing"}
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
                booked_on=entry.get("booked_on"),
                other_side_missing=entry.get("other_side_missing") is True,
            )
        )
    for i, entry in enumerate(config.get("counterparty", [])):
        unknown = set(entry) - {"name", "remittance", "booked_on"}
        if unknown or not {"name", "remittance"} <= set(entry):
            raise MappingError(
                f"{path}: counterparty[{i}] needs name and remittance"
                + (f", and has unknown keys {sorted(unknown)}" if unknown else "")
            )
        rules.append(
            CounterpartyRule(
                account=None,
                remittance=re.compile(entry["remittance"], re.IGNORECASE),
                booked_on=entry.get("booked_on"),
                counterparty=entry["name"],
            )
        )
    return rules


class AccountBook:
    """Own Firefly accounts, found by normalised IBAN, account number or counterparty rule."""

    def __init__(self, accounts: Iterable[OwnAccount], rules: Iterable[tuple[CounterpartyRule, OwnAccount]] = ()):
        accounts = list(accounts)
        self._by_iban = {a.iban: a for a in accounts if a.iban}
        self._by_number = {a.number: a for a in accounts if a.number}
        self._rules = list(rules)

    @classmethod
    def from_firefly(
        cls,
        accounts: Iterable[dict],
        imported_ibans: Iterable[str],
        rules: Iterable[CounterpartyRule] = (),
        imported_numbers: Iterable[str] = (),
    ) -> AccountBook:
        """Build from Firefly `data` items (FireflyClient.accounts) and the rules from `load_rules`.

        An imported account is matched by IBAN, or by account number for one without an IBAN.
        """
        imported = {normalise_iban(i) for i in imported_ibans}
        imported_numbers = {n.strip() for n in imported_numbers}
        own = []
        for item in accounts:
            iban = normalise_iban(item["attributes"].get("iban") or "") or None
            number = (item["attributes"].get("account_number") or "").strip() or None
            own.append(OwnAccount(
                item["id"], item["attributes"]["name"], iban,
                iban in imported or number in imported_numbers, number,
                (item["attributes"].get("currency_code") or "").strip().upper() or None,
            ))
        missing_ibans = imported - {a.iban for a in own}
        missing_numbers = imported_numbers - {a.number for a in own}
        if missing_ibans:
            raise MappingError(f"{len(missing_ibans)} imported account(s) have no Firefly account with that IBAN")
        if missing_numbers:
            raise MappingError(
                f"{len(missing_numbers)} imported account(s) without an IBAN have no Firefly account with their "
                "number (identification_hash, or sort code and account number) as its account number"
            )
        by_name = {a.name: a for a in own}
        resolved = []
        for rule in rules:
            for name in (rule.account, rule.booked_on):
                if name is not None and name not in by_name:
                    raise MappingError(f"rule names Firefly account {name!r}, which doesn't exist")
            resolved.append((rule, by_name[rule.account] if rule.account is not None else None))
        return cls(own, resolved)

    def get(self, iban: str | None) -> OwnAccount | None:
        return self._by_iban.get(normalise_iban(iban or ""))

    def get_by_number(self, number: str | None) -> OwnAccount | None:
        return self._by_number.get((number or "").strip())

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
        found = self.rule_for(account, party_name, party_iban, remittance)
        return found[1] if found else None

    def rule_for(
        self, account: OwnAccount, party_name: str | None, party_iban: str | None, remittance: str
    ) -> tuple[CounterpartyRule, OwnAccount | None] | None:
        """The first rule matching a booking on `account`, with the own account it names.

        A `[[counterparty]]` rule names no own account and only matches a booking without a counterparty name.
        """
        party_iban = normalise_iban(party_iban or "") or None
        for rule, own in self._rules:
            expected = {rule.iban} if rule.iban else {None, account.iban}
            if rule.booked_on is not None and rule.booked_on != account.name:
                continue
            if rule.counterparty is not None and party_name is not None:
                continue
            if own != account and party_iban in expected and rule.matches(party_name, remittance):
                return rule, own
        return None


def normalise_iban(iban: str) -> str:
    return "".join(iban.split()).upper()


def external_ids(account_key: tuple[str, str], transactions: Iterable[dict]) -> list[str]:
    """The content key of each raw transaction, in order (see module docstring).

    `transactions` are all of one account's bookings in a run, in page order:
    the ordinal counts across pages, so a page boundary can't change a key.
    `account_key` is ("iban", IBAN) or ("number", identification_hash).
    """
    seen: Counter[tuple] = Counter()
    keys = []
    for raw in transactions:
        fields = _key_fields(raw)
        seen[fields] += 1
        content = json.dumps([*account_key, *fields, seen[fields]], separators=(",", ":"))
        keys.append(f"{KEY_VERSION}:{hashlib.sha256(content.encode()).hexdigest()}")
    return keys


def booking_day(raw: dict) -> date:
    """The booking_date; a booking without one can't be keyed."""
    try:
        return date.fromisoformat(raw["booking_date"])
    except (KeyError, TypeError, ValueError):
        raise MappingError("transaction has no valid booking_date, so it can't be keyed") from None


def _key_fields(raw: dict) -> tuple[str, str, str, str]:
    """(booking_date, unsigned amount, currency, "DBIT"/"CRDT"), normalised so formatting can't change a key."""
    day = booking_day(raw)
    money = raw.get("transaction_amount") or {}
    try:
        amount = Decimal(money["amount"])
    except (KeyError, TypeError, InvalidOperation):
        raise MappingError(f"transaction booked {day} has no valid amount, so it can't be keyed") from None
    currency = (money.get("currency") or "").strip().upper()
    if not currency:
        raise MappingError(f"transaction booked {day} has no currency, so it can't be keyed")
    direction = "DBIT" if _is_debit(raw.get("credit_debit_indicator"), amount, f"booked {day}") else "CRDT"
    return day.isoformat(), format(abs(amount).normalize(), "f"), currency, direction


INTERNAL_MOVE_CODES = {("ICDT", "ESCT"), ("RCDT", "ESCT")}


def is_internal_move(raw: dict) -> bool:
    """A SEPA credit transfer whose counterparty shows no account: possibly a move between own accounts."""
    code = raw.get("bank_transaction_code") or {}
    if (code.get("code"), code.get("sub_code")) not in INTERNAL_MOVE_CODES:
        return False
    _, party_account = _party(raw, _key_fields(raw)[3] == "DBIT")
    return _account_identifiers(party_account) == (None, None)


@dataclass(frozen=True, eq=False)
class Booking:
    """A raw transaction on an own account, with its key from `external_ids`."""

    account: OwnAccount
    raw: dict
    external_id: str


@dataclass
class Pairing:
    # external_id of a paired booking -> the own account on its other side
    partner: dict[str, OwnAccount] = field(default_factory=dict)
    # Internal moves that pair with nothing
    unpaired: list[Booking] = field(default_factory=list)


def pair_internal_moves(bookings: Iterable[Booking]) -> Pairing:
    """Pair the internal moves among `bookings`, every imported account's (see module docstring).

    Within each group of the same booking_date, amount and currency, a debit and a
    credit can pair when they are on different accounts and have the same
    remittance text or one has none. The group is paired as a maximum matching,
    preferring equal texts, and only when every such matching gives the same
    transfers between accounts; then which of two bookings on one account goes
    with which credit makes no difference. Otherwise nothing in the group pairs.
    The result depends only on the bookings, not their order, so the same pages
    always give the same transfers.
    """
    groups: defaultdict[tuple, tuple[list[Booking], list[Booking]]] = defaultdict(lambda: ([], []))
    for b in bookings:
        if is_internal_move(b.raw):
            day, amount, currency, direction = _key_fields(b.raw)
            groups[(day, amount, currency)][direction == "CRDT"].append(b)
    pairing = Pairing()
    for debits, credits in groups.values():
        _pair_group(pairing, debits, credits)
    return pairing


# Search budget per group; a group that needs more stays unpaired (and fails loudly).
MATCHING_SEARCH_LIMIT = 100_000


class _SearchTooLarge(Exception):
    pass


def _classes(bookings: list[Booking]) -> list[list[Booking]]:
    """Bookings by account and remittance text, the interchangeable ones together, lowest key first."""
    by: defaultdict[tuple[str, str], list[Booking]] = defaultdict(list)
    for b in bookings:
        by[(b.account.firefly_id, _remittance(b.raw))].append(b)
    return [sorted(by[k], key=lambda b: b.external_id) for k in sorted(by)]


def _pair_group(pairing: Pairing, debits: list[Booking], credits: list[Booking]) -> None:
    d_classes, c_classes = _classes(debits), _classes(credits)
    edges = []
    for i, d in enumerate(d_classes):
        for j, c in enumerate(c_classes):
            td, tc = _remittance(d[0].raw), _remittance(c[0].raw)
            if d[0].account != c[0].account and (td == tc or not td or not tc):
                edges.append((i, j, td == tc))
    try:
        best = _best_matchings(edges, [len(d) for d in d_classes], [len(c) for c in c_classes])
    except _SearchTooLarge:
        log.warning("%d internal moves of one day and amount have too many ways to pair; left unpaired",
                    len(debits) + len(credits))
        best = []
    outcomes = {frozenset((+_transfers(edges, counts, d_classes, c_classes)).items()) for counts in best}
    if len(outcomes) != 1:
        pairing.unpaired += debits + credits
        return
    taken: set[int] = set()
    d_next = [0] * len(d_classes)
    c_next = [0] * len(c_classes)
    for (i, j, _), n in zip(edges, best[0], strict=True):
        for _ in range(n):
            a, b = d_classes[i][d_next[i]], c_classes[j][c_next[j]]
            d_next[i] += 1
            c_next[j] += 1
            pairing.partner[a.external_id] = b.account
            pairing.partner[b.external_id] = a.account
            taken |= {id(a), id(b)}
    pairing.unpaired += [b for b in debits + credits if id(b) not in taken]


def _transfers(edges, counts, d_classes, c_classes) -> Counter:
    """How many transfers a matching makes from each account to each other one."""
    out: Counter = Counter()
    for (i, j, _), n in zip(edges, counts, strict=True):
        out[(d_classes[i][0].account, c_classes[j][0].account)] += n
    return out


def _best_matchings(edges: list[tuple[int, int, bool]], rows: list[int], cols: list[int]) -> list[tuple[int, ...]]:
    """Every way to pair the most bookings, then the most with equal text: a count per edge.

    `edges` link a debit class to a credit class; `rows` and `cols` are how many
    bookings each class has. Raises _SearchTooLarge past MATCHING_SEARCH_LIMIT steps.
    """
    rows, cols = list(rows), list(cols)
    best: tuple[int, int] = (-1, -1)
    found: list[tuple[int, ...]] = []
    counts = [0] * len(edges)
    steps = 0

    def visit(k: int, size: int, equal: int) -> None:
        nonlocal best, found, steps
        steps += 1
        if steps > MATCHING_SEARCH_LIMIT:
            raise _SearchTooLarge
        if size + min(sum(rows), sum(cols)) < best[0]:
            return
        if k == len(edges):
            if (size, equal) > best:
                best, found = (size, equal), [tuple(counts)]
            elif (size, equal) == best:
                found.append(tuple(counts))
            return
        i, j, same = edges[k]
        for n in range(min(rows[i], cols[j]), -1, -1):
            counts[k] = n
            rows[i] -= n
            cols[j] -= n
            visit(k + 1, size + n, equal + n * same)
            rows[i] += n
            cols[j] += n
        counts[k] = 0

    visit(0, 0, 0)
    return found


def map_transaction(
    raw: dict,
    account: OwnAccount,
    book: AccountBook,
    external_id: str,
    paired: OwnAccount | None = None,
    leg_amount: str | None = None,
) -> dict | None:
    """The Firefly store payload for `raw`, booked on `account`; None to skip it (see module docstring).

    `external_id` is the booking's key from `external_ids`; `paired` is the own
    account on the other side when `pair_internal_moves` paired the booking;
    `leg_amount` is the other side's amount of a pass-through transfer, in the
    pass-through account's currency, without which such a booking raises
    `PassThroughError`.
    """
    booking_date = booking_day(raw).isoformat()
    money = raw["transaction_amount"]
    amount = Decimal(money["amount"])
    debit = _is_debit(raw.get("credit_debit_indicator"), amount, external_id)
    amount = abs(amount)
    if amount == 0:
        log.info("skipping %s: zero amount", external_id)
        return None

    party, party_account = _party(raw, debit)
    party_name = ((party or {}).get("name") or "").strip() or None
    party_iban, party_number = _account_identifiers(party_account)
    other_own = book.get(party_iban)
    if other_own is None and party_iban is None:
        other_own = book.get_by_number(party_number)
    one_sided = False
    if other_own == account:
        other_own = None
    if other_own is None:
        other_own = paired
    if other_own is None:
        found = book.rule_for(account, party_name, party_iban, _remittance(raw))
        if found is not None:
            rule, other_own = found
            one_sided = rule.other_side_missing
            if rule.counterparty is not None:
                party_name = rule.counterparty
    if other_own is None and party_name is None and is_internal_move(raw):
        raise UnpairedMoveError(f"{external_id}: internal move booked {booking_date} has no other side")

    split: dict = {
        "date": booking_date,
        "amount": str(amount),
        "currency_code": money["currency"],
        "description": _description(raw, party_name),
        "external_id": external_id,
    }
    if raw.get("entry_reference"):
        # For tracing a booking back to the bank, never for idempotency.
        split["internal_reference"] = raw["entry_reference"]
    split.update(_foreign(raw, money["currency"]))

    if other_own is not None:
        if not debit and other_own.imported and not one_sided:
            log.info("skipping %s: credit side of a transfer from %s, imported from its debit side", external_id, other_own.name)
            return None
        source, destination = (account, other_own) if debit else (other_own, account)
        split.update(type="transfer", source_id=source.firefly_id, destination_id=destination.firefly_id)
        if is_pass_through(other_own, money["currency"], split):
            if leg_amount is None:
                raise PassThroughError(other_own, debit, external_id)
            booked = split["amount"]
            if debit:
                split.update(foreign_amount=leg_amount, foreign_currency_code=other_own.currency)
            else:
                # Firefly books a transfer in the source account's currency; the booked side is the foreign one.
                split.update(amount=leg_amount, currency_code=other_own.currency,
                             foreign_amount=booked, foreign_currency_code=money["currency"])
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


def is_pass_through(other: OwnAccount, currency: str, split: dict) -> bool:
    """A transfer with an own account we don't import, in another currency, whose amount the bank can't tell us."""
    return (
        not other.imported
        and other.currency is not None
        and other.currency.upper() != (currency or "").upper()
        and "foreign_amount" not in split
    )


def _party(raw: dict, debit: bool) -> tuple[dict | None, dict | None]:
    """The counterparty and its account: the creditor of a debit, the debtor of a credit."""
    if debit:
        return raw.get("creditor"), raw.get("creditor_account")
    return raw.get("debtor"), raw.get("debtor_account")


def _is_debit(indicator: str | None, amount: Decimal, which: str) -> bool:
    if indicator == "DBIT":
        return True
    if indicator == "CRDT":
        return False
    # Without an indicator, only a signed amount says which way the money went.
    if amount < 0:
        return True
    raise MappingError(f"transaction {which} has no credit_debit_indicator and an unsigned amount")


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
