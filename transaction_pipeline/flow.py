"""The daily import: Enable Banking → raw pages on the state PVC → Firefly III.

Prefect schedules and runs it, and is never the source of truth (homelab#1981):

- The bank fetch has no retries. Each run spends one of the 4 unattended requests
  per account per 24h (PSD2 SCA RTS art. 36(5)).
- Every raw page is written to STATE_DIR/raw/<flow-run-id>/<account-uid>/page-<n>.json
  before anything is mapped, and the store step reads the pages back from disk. So
  the store step can retry without touching the bank.
- Idempotency lives in Firefly (entry_reference → external_id). The window
  overlaps the previous runs on purpose: a failed run is covered by the next one.

Everything under STATE_DIR is personal financial data (the session reads every
consented account; raw pages carry IBANs, counterparties and amounts). So files
are owner-only, and none of it passes through Prefect: tasks take and return
paths and dates only, and no result is persisted to Prefect's result storage.
A run's raw pages are deleted once they are stored; a failed run's pages stay
as evidence for RAW_KEEP_FAILED_DAYS, then the next run deletes them.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from transaction_pipeline import firefly, mapping, metrics
from transaction_pipeline.enable_banking import (
    EnableBankingClient,
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

CONSENT_HINT = "run `python -m transaction_pipeline consent` in the transaction-pipeline pod"


class ConsentNeededError(RuntimeError):
    """No usable Enable Banking session: the operator has to consent (again)."""


class BankBudgetSpentError(RuntimeError):
    """The bank refused with a rate limit. Retrying today only spends more of the budget."""


def state_dir() -> Path:
    return Path(os.environ.get("STATE_DIR", "/state"))


def session_path() -> Path:
    return state_dir() / "session.json"


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


def write_session(path: Path, session: dict) -> None:
    write_json(path, session)


def load_session(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        raise ConsentNeededError(f"no Enable Banking session at {path}: {CONSENT_HINT}") from None


def consent_valid_until(session: dict) -> float:
    return datetime.fromisoformat(session["access"]["valid_until"]).timestamp()


def account_iban(account: dict) -> str:
    iban = (account.get("account_id") or {}).get("iban")
    if not iban:
        raise mapping.MappingError(f"session account {account['uid']} has no IBAN, so it can't be matched to Firefly")
    return iban


def enable_banking_client() -> EnableBankingClient:
    return EnableBankingClient(
        os.environ["ENABLE_BANKING_APP_ID"],
        load_private_key(Path(os.environ["ENABLE_BANKING_PRIVATE_KEY_PATH"])),
    )


def firefly_client() -> firefly.FireflyClient:
    return firefly.FireflyClient(os.environ["FIREFLY_URL"], os.environ["FIREFLY_TOKEN"])


def save_pages(
    client: EnableBankingClient, session: dict, run_dir: Path, date_from: date, date_to: date
) -> dict[str, list[Path]]:
    """Fetch every account's booked transactions, writing each page as it arrives.

    Any error stops the run before anything is stored; the pages written so far
    stay on disk for debugging.
    """
    pages: dict[str, list[Path]] = {}
    for account in session["accounts"]:
        uid = account["uid"]
        paths = pages[uid] = []
        try:
            for n, page in enumerate(client.transaction_pages(uid, date_from, date_to), start=1):
                paths.append(write_json(run_dir / uid / f"page-{n}.json", page))
        except SessionGoneError as exc:
            raise ConsentNeededError(
                f"Enable Banking session is gone ({exc.error_code}): {CONSENT_HINT}"
            ) from exc
        except RateLimitedError as exc:
            raise BankBudgetSpentError(
                f"bank rate limit on account {uid} ({exc.error_code or exc.status_code}): the PSD2 budget "
                "of 4 unattended requests per account per 24h is spent; don't re-run today, "
                "the next scheduled run's overlapping window catches up"
            ) from exc
        log.info("account %s: %d page(s) saved", uid, len(paths))
    return pages


@dataclass(frozen=True)
class StoreCounts:
    created: int = 0
    existing: int = 0
    skipped: int = 0


def store_from_disk(client: firefly.FireflyClient, session: dict, pages: dict[str, list[Path]]) -> StoreCounts:
    """Map every saved transaction and store it in Firefly unless its external_id is there already."""
    book = mapping.AccountBook.from_firefly(
        client.accounts("asset"), imported_ibans=[account_iban(a) for a in session["accounts"]]
    )
    created = existing = skipped = 0
    for account in session["accounts"]:
        own = book.get(account_iban(account))
        if own is None:
            raise mapping.MappingError(f"session account {account['uid']} has no Firefly asset account")
        for path in pages.get(account["uid"], []):
            for raw in json.loads(path.read_text()).get("transactions") or []:
                payload = mapping.map_transaction(raw, own, book)
                if payload is None:
                    skipped += 1
                elif client.store_if_absent(payload).created:
                    created += 1
                else:
                    existing += 1
    return StoreCounts(created, existing, skipped)


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
def fetch_pages(run_dir: Path, date_from: date, date_to: date) -> dict[str, list[Path]]:
    session = load_session(session_path())
    with enable_banking_client() as client:
        return save_pages(client, session, run_dir, date_from, date_to)


@task(
    retries=STORE_RETRIES,
    retry_delay_seconds=STORE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_store,
    cache_policy=NO_CACHE,
    persist_result=False,
)
def store_pages(pages: dict[str, list[Path]]) -> StoreCounts:
    session = load_session(session_path())
    with firefly_client() as client:
        counts = store_from_disk(client, session, pages)
    log.info("stored %d new, %d already in Firefly, %d skipped", counts.created, counts.existing, counts.skipped)
    return counts


def raw_root() -> Path:
    return state_dir() / "raw"


def prune_failed_runs(root: Path, now: float | None = None) -> None:
    """Delete raw run dirs that a failed run left behind more than RAW_KEEP_FAILED_DAYS ago."""
    if not root.exists():
        return
    cutoff = (time.time() if now is None else now) - RAW_KEEP_FAILED_DAYS * 86400
    for run_dir in root.iterdir():
        if run_dir.is_dir() and run_dir.stat().st_mtime < cutoff:
            shutil.rmtree(run_dir)
            log.info("deleted raw pages of failed run %s", run_dir.name)


def run_id() -> str:
    return flow_run.id or "manual-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


@flow(name="transaction-import", persist_result=False)
def import_flow(window_days: int = WINDOW_DAYS) -> None:
    """Fetch booked transactions from Enable Banking and write them to Firefly III."""
    session = load_session(session_path())
    metrics.push_consent_valid_until(consent_valid_until(session))
    prune_failed_runs(raw_root())
    today = datetime.now(UTC).date()
    run_dir = raw_root() / run_id()
    pages = fetch_pages(run_dir, today - timedelta(days=window_days), today)
    store_pages(pages)
    # Stored, so Firefly has it all: don't keep a copy of the bank feed at rest.
    shutil.rmtree(run_dir, ignore_errors=True)
    metrics.push_success()
