"""The daily import: Enable Banking → raw pages on the state PVC → Firefly III.

Prefect schedules and runs it, and is never the source of truth (homelab#1981):

- The bank fetch has no retries. Each run spends one of the 4 unattended requests
  per account per 24h (PSD2 SCA RTS art. 36(5)).
- Every bank login has its own session, STATE_DIR/sessions/<label>.json (written
  by `consent <label>`); the flow fetches every account of every session.
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

`replay_run=<run-id>` maps and stores a saved run directory without fetching
anything: the go-live backfill is fetched once in save-only mode right after
consent, and replayed after the clean start. It is not a failure-recovery
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
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from transaction_pipeline import firefly, mapping, metrics
from transaction_pipeline.enable_banking import (
    EnableBankingClient,
    EnableBankingError,
    RateLimitedError,
    SessionGoneError,
    load_private_key,
)

log = logging.getLogger(__name__)

# Days back from today. Firefly dedupes the overlap, and the request count is
# the same whatever the window, so a missed run or two needs no backfill.
WINDOW_DAYS = 7
# Store retries only: they read the pages from disk, never from the bank.
STORE_RETRIES = 2
STORE_RETRY_DELAY_SECONDS = 30
# Raw pages left behind by a failed run, kept for debugging, then deleted.
RAW_KEEP_FAILED_DAYS = 7

CONSENT_HINT = "run `python -m transaction_pipeline consent {label} ...` in the transaction-pipeline pod"
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


def account_key(account: dict) -> tuple[str, str]:
    """How a session account is matched to Firefly: ("iban", its IBAN), else ("number", its identification_hash).

    Banks list some sub-accounts without an IBAN (N26 Spaces). Enable Banking's
    identification_hash is the same for an account in every session, also one
    authorised by another login, so it survives re-consent (the uid doesn't) and
    a shared account seen through two logins has one key.
    """
    iban = (account.get("account_id") or {}).get("iban")
    if iban:
        return "iban", mapping.normalise_iban(iban)
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


@dataclass
class Fetched:
    """Pages per account uid for the accounts fetched in full, and what failed."""

    pages: dict[str, list[Path]] = field(default_factory=dict)
    errors: list[Exception] = field(default_factory=list)


def save_pages(
    client: EnableBankingClient, sessions: dict[str, dict], run_dir: Path, date_from: date, date_to: date
) -> Fetched:
    """Fetch every account's booked transactions, writing each page as it arrives.

    One bank login failing doesn't stop the others: a gone session skips the rest
    of that session's accounts, any other error skips that account. Pages of a
    failed account stay on disk but are not stored; the caller fails the run.
    """
    fetched = Fetched()
    for label, session in sessions.items():
        for account in session["accounts"]:
            uid = account["uid"]
            paths: list[Path] = []
            try:
                for n, page in enumerate(client.transaction_pages(uid, date_from, date_to), start=1):
                    paths.append(write_json(run_dir / uid / f"page-{n}.json", page))
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
            log.info("session %s: account %s: %d page(s) saved", label, uid, len(paths))
    return fetched


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
    """One account's transactions from its saved pages, in page order."""
    return [raw for path in paths for raw in json.loads(path.read_text()).get("transactions") or []]


@dataclass(frozen=True)
class StoreCounts:
    created: int = 0
    existing: int = 0
    skipped: int = 0
    # Mapped but not stored, because FIREFLY_WRITE is off (replay only).
    not_written: int = 0


def store_from_disk(
    client: firefly.FireflyClient,
    sessions: dict[str, dict],
    pages: dict[str, list[Path]],
    rules: list | None = None,
    *,
    write: bool = True,
) -> StoreCounts:
    """Map every saved transaction and store it in Firefly unless its external_id is there already.

    Every session account counts as imported, fetched this run or not, so the
    side of a transfer between two banks is chosen the same way every run.

    An account listed by several sessions (shared between two logins) is mapped
    once, from the first session that has pages for it. Its keys are the same
    under every session (they hash the account key, not the uid), so mapping it
    twice would only look up every booking twice.
    """
    accounts = all_accounts(sessions)
    keys = {account["uid"]: account_key(account) for account in accounts}
    book = mapping.AccountBook.from_firefly(
        client.accounts("asset"),
        imported_ibans=[value for kind, value in keys.values() if kind == "iban"],
        rules=rules or [],
        imported_numbers=[value for kind, value in keys.values() if kind == "number"],
    )
    created = existing = skipped = not_written = 0
    mapped: dict[tuple[str, str], str] = {}
    for account in accounts:
        if account["uid"] not in pages:
            continue
        key = keys[account["uid"]]
        if key in mapped:
            log.info("account %s is account %s under another session, mapped once", account["uid"], mapped[key])
            continue
        mapped[key] = account["uid"]
        kind, value = key
        own = book.get(value) if kind == "iban" else book.get_by_number(value)
        if own is None:
            raise mapping.MappingError(f"session account {account['uid']} has no Firefly asset account")
        transactions = load_transactions(pages[account["uid"]])
        for raw, external_id in zip(transactions, mapping.external_ids(key, transactions), strict=True):
            payload = mapping.map_transaction(raw, own, book, external_id)
            if payload is None:
                skipped += 1
            elif not write:
                not_written += 1
            elif client.store_if_absent(payload).created:
                created += 1
            else:
                existing += 1
    return StoreCounts(created, existing, skipped, not_written)


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
def store_pages(pages: dict[str, list[Path]], write: bool = True) -> StoreCounts:
    sessions = load_sessions(sessions_dir())
    rules = mapping.load_rules(mapping_config())
    with firefly_client() as client:
        counts = store_from_disk(client, sessions, pages, rules, write=write)
    log.info(
        "stored %d new, %d already in Firefly, %d skipped, %d mapped but not written",
        counts.created, counts.existing, counts.skipped, counts.not_written,
    )
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
def import_flow(window_days: int = WINDOW_DAYS, replay_run: str | None = None) -> None:
    """Fetch booked transactions from Enable Banking and write them to Firefly III.

    With `replay_run`, map and store that saved run's pages instead of fetching.
    """
    write = firefly_write()
    sessions = load_sessions(sessions_dir())
    metrics.push_consent_valid_until({label: consent_valid_until(s) for label, s in sessions.items()})
    if replay_run is not None:
        replay(replay_run, sessions, write)
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


def replay(replay_run: str, sessions: dict[str, dict], write: bool) -> None:
    """Map (and with `write`, store and then delete) a saved run. No fetch, no pruning.

    Pushes no success timestamp: that says the daily import is current, and a
    replay fetches nothing new.
    """
    if not RUN_ID_PATTERN.fullmatch(replay_run):
        raise ValueError(f"replay_run {replay_run!r} is not a run id")
    run_dir = raw_root() / replay_run
    pages = saved_pages(run_dir, sessions)
    if not write:
        log.warning("FIREFLY_WRITE is not true: replaying run %s maps only, stores nothing", replay_run)
    store_pages(pages, write)
    if write:
        shutil.rmtree(run_dir, ignore_errors=True)
