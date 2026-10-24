"""The daily import: Enable Banking and Monzo → raw pages on the state PVC → Firefly III.

Prefect schedules and runs it, and is never the source of truth (homelab#1981):

- The bank fetch has no retries. Each run spends one of the 4 unattended requests
  per account per 24h (PSD2 SCA RTS art. 36(5)).
- Every bank login has its own session, STATE_DIR/sessions/<label>.json (written
  by `consent <label>`, or `monzo-auth <label>` for a Monzo login, whose session
  says `"provider": "monzo"`); the flow fetches every account of every session.
  Monzo pages are saved as the API returns them and adapted to the Enable
  Banking shape when they are read back (monzo.adapt), so the mapping is one.
- Every raw page is written to STATE_DIR/raw/<flow-run-id>/<account-uid>/page-<n>.json
  before anything is mapped, and the store step reads the pages back from disk. So
  the store step can retry without touching the bank.
- Idempotency lives in Firefly: external_id is a key derived from each booking's
  content (mapping.external_ids). The window overlaps the previous runs on
  purpose: a failed run is covered by the next one.

Everything under STATE_DIR is personal financial data (the session reads every
consented account; raw pages carry IBANs, counterparties and amounts). So files
are owner-only, and none of it passes through Prefect: tasks take and return
paths and dates only, and no result is persisted to Prefect's result storage.
A run's raw pages are deleted once they are stored; a failed run's pages stay
as evidence for RAW_KEEP_FAILED_DAYS, then the next run deletes them.

`replay_run=<run-id>` (or a list of run ids, mapped together) maps and stores
saved run directories without fetching anything: the go-live backfill is
fetched once per login in save-only mode right after consent, and the backfills
and the daily runs since are replayed together after the clean start, so moves
between the logins' accounts pair. `since` leaves out bookings before a date. It is not a failure-recovery
mechanism (the overlapping window is) and never prunes. A replay that writes
deletes the replayed pages once stored, like any successful run.

A save-only run marks its directory with a SAVE_ONLY_MARKER file, and pruning
never touches a marked directory: the backfill must survive until it is
replayed, however long go-live takes. The operator deletes the save-only runs
left over after go-live.

FIREFLY_WRITE (default false) is the go-live switch. Until it is "true", runs
only fetch and save raw pages, which are kept (they are what the mapping rules
are written from), and push no success timestamp, so the staleness alert keeps
firing until the first real import.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from transaction_pipeline import firefly, mapping, metrics, monzo
from transaction_pipeline.enable_banking import (
    EnableBankingClient,
    EnableBankingError,
    RateLimitedError,
    SessionGoneError,
    load_private_key,
)

log = logging.getLogger(__name__)
# Prefect's logging config leaves the root logger at WARNING. The image sets
# PREFECT_LOGGING_EXTRA_LOGGERS to this module, which gives it Prefect's API
# handler; at INFO its lines then reach the run log, and the pod log through
# the root console handler. Only this module: mapping.py logs per transaction.
log.setLevel(logging.INFO)

# Days back from today. Firefly dedupes the overlap, and the request count is
# the same whatever the window, so a missed run or two needs no backfill.
WINDOW_DAYS = 7
# Store retries only: they read the pages from disk, never from the bank.
STORE_RETRIES = 2
STORE_RETRY_DELAY_SECONDS = 30
# Raw pages left behind by a failed run, kept for debugging, then deleted.
RAW_KEEP_FAILED_DAYS = 7

CONSENT_HINT = "run `python -m transaction_pipeline consent {label} ...` in the transaction-pipeline pod"
MONZO_HINT = "run `python -m transaction_pipeline monzo-auth {label} --redirect-url ...` in the transaction-pipeline pod"
# A session label names a file and a Prometheus label value: keep it plain.
LABEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
SAVE_ONLY_MARKER = "save-only"
# Flow run ids are UUIDs; runs outside Prefect are "manual-<timestamp>".
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


class ConsentNeededError(RuntimeError):
    """No usable Enable Banking session: the operator has to consent (again)."""


class BankBudgetSpentError(RuntimeError):
    """The bank refused with a rate limit. Retrying today only spends more of the budget."""


def state_dir() -> Path:
    return Path(os.environ.get("STATE_DIR", "/state"))


def sessions_dir() -> Path:
    return state_dir() / "sessions"


def session_path(label: str) -> Path:
    if not LABEL_PATTERN.fullmatch(label):
        raise ValueError(f"session label {label!r} must be lowercase letters, digits and dashes")
    return sessions_dir() / f"{label}.json"


def raw_root() -> Path:
    return state_dir() / "raw"


def firefly_write() -> bool:
    value = os.environ.get("FIREFLY_WRITE", "false").strip().lower()
    if value not in ("true", "false"):
        # A typo must not silently write, nor silently not write.
        raise ValueError(f"FIREFLY_WRITE must be true or false, not {value!r}")
    return value == "true"


def mapping_config() -> Path:
    return Path(os.environ.get("MAPPING_CONFIG", "/config/mapping.toml"))


def write_json(path: Path, data: object) -> Path:
    """Write owner-only via a temp file and rename, so a crash never leaves half a file behind."""
    for directory in reversed([d for d in (path.parent, *path.parent.parents) if not d.exists()]):
        directory.mkdir(mode=0o700, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(path)
    return path


def load_sessions(directory: Path) -> dict[str, dict]:
    """Every session in `directory`, by label (the file name without .json)."""
    sessions = {p.stem: json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))}
    if not sessions:
        raise ConsentNeededError(f"no Enable Banking sessions in {directory}: " + CONSENT_HINT.format(label="<label>"))
    return sessions


def consent_valid_until(session: dict) -> float:
    return datetime.fromisoformat(session["access"]["valid_until"]).timestamp()


def provider(session: dict) -> str:
    """Which API a session is for: "enable_banking" (no `provider` key) or "monzo"."""
    return session.get("provider") or "enable_banking"


def session_of(sessions: dict[str, dict], uid: str) -> dict:
    for session in sessions.values():
        if any(a["uid"] == uid for a in session["accounts"]):
            return session
    raise KeyError(uid)


def account_key(account: dict) -> tuple[str, str]:
    """How a session account is matched to Firefly: ("iban", its IBAN), else ("number", an account number).

    Banks list some sub-accounts without an IBAN (N26 Spaces). Enable Banking's
    identification_hash is the same for an account in every session, also one
    authorised by another login, so it survives re-consent (the uid doesn't) and
    a shared account seen through two logins has one key. A Monzo account has no
    IBAN either; its `number` is the sort code and account number (monzo.account_entry).
    """
    iban = (account.get("account_id") or {}).get("iban")
    if iban:
        return "iban", mapping.normalise_iban(iban)
    if account.get("number"):
        return "number", account["number"]
    if account.get("identification_hash"):
        return "number", account["identification_hash"]
    raise mapping.MappingError(
        f"session account {account['uid']} has neither an IBAN nor an identification_hash, "
        "so it can't be matched to Firefly"
    )


def all_accounts(sessions: dict[str, dict]) -> list[dict]:
    return [account for session in sessions.values() for account in session["accounts"]]


def enable_banking_client() -> EnableBankingClient:
    return EnableBankingClient(
        os.environ["ENABLE_BANKING_APP_ID"],
        load_private_key(Path(os.environ["ENABLE_BANKING_PRIVATE_KEY_PATH"])),
    )


def firefly_client() -> firefly.FireflyClient:
    return firefly.FireflyClient(os.environ["FIREFLY_URL"], os.environ["FIREFLY_TOKEN"])


def monzo_credentials() -> tuple[str, str]:
    """The OAuth client id and secret, from the environment (a sealed secret in the cluster)."""
    try:
        return os.environ["MONZO_CLIENT_ID"], os.environ["MONZO_CLIENT_SECRET"]
    except KeyError as exc:
        raise ConsentNeededError(f"{exc.args[0]} is not set, so a Monzo login can't be used") from None


def monzo_client(label: str, session: dict) -> monzo.MonzoClient:
    """A client on the session's token that writes every new token back to the session file."""
    client_id, client_secret = monzo_credentials()
    if not session.get("token"):
        raise ConsentNeededError(f"session {label} has no Monzo token: " + MONZO_HINT.format(label=label))

    def persist(token: monzo.Token) -> None:
        session["token"] = token.to_json()
        write_json(session_path(label), session)

    return monzo.MonzoClient(client_id, client_secret, monzo.Token.from_json(session["token"]), on_token=persist)


@dataclass
class Fetched:
    """Pages per account uid for the accounts fetched in full, and what failed."""

    pages: dict[str, list[Path]] = field(default_factory=dict)
    errors: list[Exception] = field(default_factory=list)


MonzoClients = Callable[[str, dict], monzo.MonzoClient]


def save_pages(
    client: EnableBankingClient,
    sessions: dict[str, dict],
    run_dir: Path,
    date_from: date,
    date_to: date,
    monzo_clients: MonzoClients = monzo_client,
) -> Fetched:
    """Fetch every account's booked transactions, writing each page as it arrives.

    One bank login failing doesn't stop the others: a gone session skips the rest
    of that session's accounts, any other error skips that account. Pages of a
    failed account stay on disk but are not stored; the caller fails the run.
    Monzo sessions are fetched from their own API (`monzo_clients` makes the
    client for a label and session; the default reads the token from the session).
    """
    fetched = Fetched()
    for label, session in sessions.items():
        if provider(session) == monzo.PROVIDER:
            _save_monzo_pages(label, session, run_dir, date_from, date_to, fetched, monzo_clients)
            continue
        for account in session["accounts"]:
            uid = account["uid"]
            paths: list[Path] = []
            booking_dates: list[str] = []
            try:
                for n, page in enumerate(client.transaction_pages(uid, date_from, date_to), start=1):
                    paths.append(write_json(run_dir / uid / f"page-{n}.json", page))
                    booking_dates += [raw.get("booking_date") or "" for raw in page.get("transactions") or []]
            except SessionGoneError as exc:
                fetched.errors.append(ConsentNeededError(
                    f"session {label}: Enable Banking session is gone ({exc.error_code}): "
                    + CONSENT_HINT.format(label=label)
                ))
                log.error("%s", fetched.errors[-1])
                break
            except RateLimitedError as exc:
                fetched.errors.append(BankBudgetSpentError(
                    f"session {label}: bank rate limit on account {uid} ({exc.error_code or exc.status_code}): "
                    "the PSD2 budget of 4 unattended requests per account per 24h is spent; don't re-run "
                    "today, the next scheduled run's overlapping window catches up"
                ))
                log.error("%s", fetched.errors[-1])
                continue
            except EnableBankingError as exc:
                fetched.errors.append(exc)
                log.error("session %s: account %s: %s", label, uid, exc)
                continue
            fetched.pages[uid] = paths
            log.info("session %s: account %s: %d page(s) saved, %s", label, uid, len(paths), fetch_summary(booking_dates))
    return fetched


def _save_monzo_pages(
    label: str, session: dict, run_dir: Path, date_from: date, date_to: date, fetched: Fetched, make_client: MonzoClients
) -> None:
    """One Monzo login's accounts. The window starts SETTLE_GRACE_DAYS early (monzo.py)."""
    since = datetime.combine(date_from - timedelta(days=monzo.SETTLE_GRACE_DAYS), datetime.min.time(), UTC)
    before = datetime.combine(date_to + timedelta(days=1), datetime.min.time(), UTC)
    try:
        client = make_client(label, session)
    except ConsentNeededError as exc:
        fetched.errors.append(exc)
        log.error("%s", exc)
        return
    with client:
        for account in session["accounts"]:
            uid = account["uid"]
            paths: list[Path] = []
            booking_dates: list[str] = []
            try:
                for n, page in enumerate(client.transaction_pages(uid, since, before), start=1):
                    paths.append(write_json(run_dir / uid / f"page-{n}.json", page))
                    booking_dates += [monzo.settled_day(tx) or "" for tx in page.get("transactions") or []]
            except monzo.AccessGoneError as exc:
                fetched.errors.append(ConsentNeededError(
                    f"session {label}: Monzo access is gone ({exc.code or exc.status_code}): the token can't be "
                    "refreshed or the 90-day approval lapsed; " + MONZO_HINT.format(label=label)
                ))
                log.error("%s", fetched.errors[-1])
                break
            except monzo.MonzoError as exc:
                fetched.errors.append(exc)
                log.error("session %s: account %s: %s", label, uid, exc)
                continue
            fetched.pages[uid] = paths
            log.info("session %s: account %s: %d page(s) saved, %s", label, uid, len(paths), fetch_summary(booking_dates))


def fetch_summary(booking_dates: list[str]) -> str:
    """Transaction count and booking-date range: what an account's fetch got, without its content."""
    dates = sorted(d for d in booking_dates if d)
    summary = f"{len(booking_dates)} transaction(s)"
    if dates:
        summary += f" booked {dates[0]} to {dates[-1]}"
    if len(dates) < len(booking_dates):
        summary += f" ({len(booking_dates) - len(dates)} without a booking_date)"
    return summary


def saved_pages(run_dir: Path, sessions: dict[str, dict]) -> dict[str, list[Path]]:
    """The pages a run saved, per account uid, in page order.

    Fails on an account directory that no session knows: Enable Banking gives
    accounts new uids on re-consent, and silently replaying nothing for them
    would look like success.
    """
    if not run_dir.is_dir():
        raise FileNotFoundError(f"no saved run at {run_dir}")
    known = {a["uid"] for a in all_accounts(sessions)}
    pages = {
        d.name: sorted(d.glob("page-*.json"), key=lambda p: int(p.stem.removeprefix("page-")))
        for d in sorted(run_dir.iterdir())
        if d.is_dir()
    }
    unknown = sorted(set(pages) - known)
    if unknown:
        raise RuntimeError(
            f"run {run_dir.name} has pages for {len(unknown)} account(s) no current session knows "
            f"({', '.join(unknown)}); re-consent gives accounts new uids, so this run can't be replayed"
        )
    return pages


def load_transactions(paths: list[Path]) -> list[dict]:
    """One account's transactions from its saved pages, in page order, as the API returned them."""
    return [raw for path in paths for raw in json.loads(path.read_text()).get("transactions") or []]


def transactions_of(session: dict, paths: list[Path]) -> list[dict]:
    """One account's bookings from its saved pages, in the Enable Banking shape whatever the provider."""
    transactions = load_transactions(paths)
    if provider(session) == monzo.PROVIDER:
        return monzo.adapt(transactions, {pot["id"]: pot["name"] for pot in session.get("pots", [])})
    return transactions


@dataclass(frozen=True)
class StoreCounts:
    created: int = 0
    existing: int = 0
    skipped: int = 0
    # Mapped but not stored, because FIREFLY_WRITE is off (replay only).
    not_written: int = 0
    # Internal moves left for the next run, because an account has no pages this run.
    deferred: int = 0
    # Internal moves without a name that pair with nothing and match no rule.
    unpaired: int = 0

    def __add__(self, other: StoreCounts) -> StoreCounts:
        return StoreCounts(
            self.created + other.created,
            self.existing + other.existing,
            self.skipped + other.skipped,
            self.not_written + other.not_written,
            self.deferred + other.deferred,
            self.unpaired + other.unpaired,
        )

    def __str__(self) -> str:
        text = f"{self.created} created, {self.existing} existing, {self.skipped} skipped, {self.not_written} not written"
        for name in ("deferred", "unpaired"):
            if getattr(self, name):
                text += f", {getattr(self, name)} {name}"
        return text


Pages = dict[str, list[Path]]


def store_from_disk(
    client: firefly.FireflyClient,
    sessions: dict[str, dict],
    pages: Pages | list[Pages],
    rules: list | None = None,
    *,
    write: bool = True,
    since: date | None = None,
) -> StoreCounts:
    """Map every saved transaction and store it in Firefly unless its external_id is there already.

    `pages` is one run's pages per account uid, or a list of several runs'.

    Every session account counts as imported, fetched this run or not, so the
    side of a transfer between two banks is chosen the same way every run.

    An account's bookings are the union, by key, of what every run fetched for it
    under every session. Keys hash the account key, not the uid, so an account
    shared between two logins has the same keys under both, and so do the days
    two overlapping runs share. Bookings before `since` are left out.

    Internal moves are paired across all accounts (mapping.pair_internal_moves).
    When an account has no pages at all (its fetch failed), the other side of a
    move may be missing, so no internal move is stored: they are counted as
    deferred, and the next run's overlapping window covers them. The same goes for
    internal moves on the latest booking day in the pages: a run may have seen
    only part of that day, and pairing a partial day could pair its
    interchangeable bookings differently from the run that sees all of it, which
    would attach a stored transfer to the wrong account. Internal moves
    without a name that pair with nothing and match no rule aren't stored either;
    once everything else is stored, the store raises UnpairedMoveError.
    """
    runs = [pages] if isinstance(pages, dict) else pages
    accounts = all_accounts(sessions)
    keys = {account["uid"]: account_key(account) for account in accounts}
    book = mapping.AccountBook.from_firefly(
        client.accounts("asset"),
        imported_ibans=[value for kind, value in keys.values() if kind == "iban"],
        rules=rules or [],
        imported_numbers=[value for kind, value in keys.values() if kind == "number"],
    )
    check_pots(sessions, book)
    first_uid: dict[tuple[str, str], str] = {}
    owns: dict[tuple[str, str], mapping.OwnAccount] = {}
    bookings: dict[tuple[str, str], list[mapping.Booking]] = {}
    seen: dict[tuple[str, str], set[str]] = {}
    for run in runs:
        for account in accounts:
            uid = account["uid"]
            if uid not in run:
                continue
            key = keys[uid]
            if key not in first_uid:
                kind, value = key
                own = book.get(value) if kind == "iban" else book.get_by_number(value)
                if own is None:
                    raise mapping.MappingError(f"session account {uid} has no Firefly asset account")
                first_uid[key], owns[key], bookings[key], seen[key] = uid, own, [], set()
            elif uid != first_uid[key]:
                log.info("account %s is account %s under another session, mapped together", uid, first_uid[key])
            transactions = transactions_of(session_of(sessions, uid), run[uid])
            for raw, external_id in zip(transactions, mapping.external_ids(key, transactions), strict=True):
                if external_id in seen[key]:
                    continue
                seen[key].add(external_id)
                if since is None or mapping.booking_day(raw) >= since:
                    bookings[key].append(mapping.Booking(owns[key], raw, external_id))
    complete = set(keys.values()) <= set(bookings)
    pairing = mapping.pair_internal_moves(b for bs in bookings.values() for b in bs) if complete else None
    if not complete:
        log.warning("not every account has pages this run, so internal moves are left for the next run")
    last_day = max((mapping.booking_day(b.raw) for bs in bookings.values() for b in bs), default=None)
    total = StoreCounts()
    for key, uid in first_uid.items():
        created = existing = skipped = not_written = deferred = unpaired = 0
        for b in bookings[key]:
            if mapping.is_internal_move(b.raw) and (pairing is None or mapping.booking_day(b.raw) == last_day):
                deferred += 1
                continue
            try:
                payload = mapping.map_transaction(
                    b.raw, b.account, book, b.external_id, pairing.partner.get(b.external_id) if pairing else None)
            except mapping.UnpairedMoveError:
                unpaired += 1
                continue
            if payload is None:
                skipped += 1
            elif not write:
                not_written += 1
            elif client.store_if_absent(payload).created:
                created += 1
            else:
                existing += 1
        counts = StoreCounts(created, existing, skipped, not_written, deferred, unpaired)
        log.info("account %s: %s", uid, counts)
        total += counts
    if total.unpaired:
        raise mapping.UnpairedMoveError(
            f"{total.unpaired} internal move(s) without a name pair with no other booking and match no rule "
            "(counts per account above); everything else is stored"
        )
    return total


def check_pots(sessions: dict[str, dict], book: mapping.AccountBook) -> None:
    """Every pot of every Monzo session needs a Firefly asset account with the pot id as its account number.

    Pots are sub-accounts like N26 Spaces, but have no feed of their own: their
    moves appear on the current account, naming the pot by id. Without an account
    for the pot, those moves would be booked against an expense named after it.
    A deleted pot is not required; its old moves map to an expense of its name.
    """
    missing = [
        f"session {label}: pot {pot['id']} ({pot['name']})"
        for label, session in sessions.items()
        for pot in session.get("pots", [])
        if not pot.get("deleted") and book.get_by_number(pot["id"]) is None
    ]
    if missing:
        raise mapping.MappingError(
            f"{len(missing)} pot(s) have no Firefly asset account with the pot id as its account number: "
            + "; ".join(missing)
        )


def is_transient(exc: BaseException) -> bool:
    """Retry Firefly being unreachable or erroring, never a mapping or data problem."""
    if isinstance(exc, httpx.TransportError):
        return True
    return isinstance(exc, firefly.FireflyError) and exc.status_code >= 500


def _retry_store(task, task_run, state) -> bool:
    try:
        state.result()
    except Exception as exc:
        return is_transient(exc)
    return False


# retries=0 is the default; spelled out because it's a constraint, not an oversight.
@task(retries=0, cache_policy=NO_CACHE, persist_result=False)
def fetch_pages(run_dir: Path, date_from: date, date_to: date) -> Fetched:
    sessions = load_sessions(sessions_dir())
    with enable_banking_client() as client:
        return save_pages(client, sessions, run_dir, date_from, date_to)


@task(
    retries=STORE_RETRIES,
    retry_delay_seconds=STORE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_store,
    cache_policy=NO_CACHE,
    persist_result=False,
)
def store_pages(pages: Pages | list[Pages], write: bool = True, since: date | None = None) -> StoreCounts:
    sessions = load_sessions(sessions_dir())
    rules = mapping.load_rules(mapping_config())
    with firefly_client() as client:
        counts = store_from_disk(client, sessions, pages, rules, write=write, since=since)
    log.info("all accounts: %s", counts)
    return counts


def prune_failed_runs(root: Path, now: float | None = None) -> None:
    """Delete raw run dirs that a failed run left behind more than RAW_KEEP_FAILED_DAYS ago.

    Save-only runs (marked with SAVE_ONLY_MARKER) are never pruned.
    """
    if not root.exists():
        return
    cutoff = (time.time() if now is None else now) - RAW_KEEP_FAILED_DAYS * 86400
    for run_dir in root.iterdir():
        if (run_dir / SAVE_ONLY_MARKER).exists():
            continue
        if run_dir.is_dir() and run_dir.stat().st_mtime < cutoff:
            shutil.rmtree(run_dir)
            log.info("deleted raw pages of failed run %s", run_dir.name)


def run_id() -> str:
    return flow_run.id or "manual-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


@flow(name="transaction-import", persist_result=False)
def import_flow(
    window_days: int = WINDOW_DAYS, replay_run: str | list[str] | None = None, since: str | None = None
) -> None:
    """Fetch booked transactions from Enable Banking and write them to Firefly III.

    With `replay_run` (one run id or a list), map and store those saved runs'
    pages together instead of fetching, leaving out bookings before `since`
    (an ISO date). It is a string, not a `date`: Prefect pickles this module by
    value for a served run, which loses names used only in annotations, so a
    `date` parameter fails validation before the flow starts.
    """
    if since is not None and replay_run is None:
        raise ValueError("since only applies to a replay_run")
    since_date = date.fromisoformat(since) if since is not None else None
    write = firefly_write()
    sessions = load_sessions(sessions_dir())
    metrics.push_consent_valid_until({label: consent_valid_until(s) for label, s in sessions.items()})
    if replay_run is not None:
        replay(replay_run, sessions, write, since_date)
        return
    if write:
        prune_failed_runs(raw_root())
    today = datetime.now(UTC).date()
    run_dir = raw_root() / run_id()
    if not write:
        # Before the fetch, so even a run that dies mid-fetch is protected from pruning.
        write_json(run_dir / SAVE_ONLY_MARKER, {"saved_at": datetime.now(UTC).isoformat()})
    fetched = fetch_pages(run_dir, today - timedelta(days=window_days), today)
    if not write:
        log.warning("FIREFLY_WRITE is not true: saved raw pages to %s, stored nothing in Firefly", run_dir)
    else:
        store_pages(fetched.pages)
    if len(fetched.errors) == 1:
        raise fetched.errors[0]
    if fetched.errors:
        raise ExceptionGroup(f"{len(fetched.errors)} Enable Banking fetches failed", fetched.errors)
    if write:
        # Stored, so Firefly has it all: don't keep a copy of the bank feed at rest.
        shutil.rmtree(run_dir, ignore_errors=True)
        metrics.push_success()


def replay(replay_run: str | list[str], sessions: dict[str, dict], write: bool, since: date | None = None) -> None:
    """Map (and with `write`, store and then delete) saved runs, in one pass. No fetch, no pruning.

    Several runs are mapped together so that moves pair across them: each login's
    go-live backfill is its own run. Pushes no success timestamp: that says the
    daily import is current, and a replay fetches nothing new.
    """
    runs = [replay_run] if isinstance(replay_run, str) else list(replay_run)
    if not runs:
        raise ValueError("replay_run names no run")
    for run in runs:
        if not RUN_ID_PATTERN.fullmatch(run):
            raise ValueError(f"replay_run {run!r} is not a run id")
    pages = [saved_pages(raw_root() / run, sessions) for run in runs]
    if not write:
        log.warning("FIREFLY_WRITE is not true: replaying run(s) %s maps only, stores nothing", ", ".join(runs))
    store_pages(pages, write, since)
    if write:
        for run in runs:
            shutil.rmtree(raw_root() / run, ignore_errors=True)
