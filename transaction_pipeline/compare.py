"""`compare-keys RUN_A RUN_B`: do two saved runs give the same keys for the days both cover?

Checks the content key (mapping.external_ids) against real data without
printing any of it: per account, only counts. Run in the pod on two saved runs,
e.g. the go-live backfill and a later daily run; zero keys only in A and zero
only in B for every account means the key is stable between fetches.

The days compared, per account, run from the later of the two runs' first
booking dates to the earlier of their last booking dates, minus that last day:
the earlier run may have been fetched during it, and bookings that arrived
afterwards aren't a key mismatch. An account with pages in only one run is
listed but not compared.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from transaction_pipeline import flow, mapping

UID_PREFIX = 8


@dataclass(frozen=True)
class AccountComparison:
    uid: str
    # None when the account has pages in only one of the runs.
    days: int | None = None
    both: int = 0
    only_a: int = 0
    only_b: int = 0

    def line(self) -> str:
        name = f"account {self.uid[:UID_PREFIX]}"
        if self.days is None:
            return f"{name}: not in both runs, not compared"
        return (
            f"{name}: {self.both} keys in both, {self.only_a} only in A, {self.only_b} only in B "
            f"({self.days} days compared)"
        )


def keys_by_day(sessions: dict[str, dict], uid: str, paths: list[Path]) -> dict[date, set[str]]:
    account = next(a for a in flow.all_accounts(sessions) if a["uid"] == uid)
    transactions = flow.load_transactions(paths)
    days: dict[date, set[str]] = {}
    for raw, key in zip(transactions, mapping.external_ids(flow.account_key(account), transactions), strict=True):
        days.setdefault(mapping.booking_day(raw), set()).add(key)
    return days


def compare_account(uid: str, a: dict[date, set[str]], b: dict[date, set[str]]) -> AccountComparison:
    if not a or not b:
        # No bookings at all: no covered days to compare.
        return AccountComparison(uid, days=0)
    first = max(min(a), min(b))
    last = min(max(a), max(b)) - timedelta(days=1)
    days = [first + timedelta(days=n) for n in range((last - first).days + 1)]
    keys_a = set().union(*(a.get(d, set()) for d in days))
    keys_b = set().union(*(b.get(d, set()) for d in days))
    return AccountComparison(uid, len(days), len(keys_a & keys_b), len(keys_a - keys_b), len(keys_b - keys_a))


def compare_runs(run_a: str, run_b: str, sessions: dict[str, dict]) -> list[AccountComparison]:
    pages = []
    for run in (run_a, run_b):
        if not flow.RUN_ID_PATTERN.fullmatch(run):
            raise ValueError(f"{run!r} is not a run id")
        pages.append(flow.saved_pages(flow.raw_root() / run, sessions))
    pages_a, pages_b = pages
    results = []
    for uid in sorted(set(pages_a) | set(pages_b)):
        if uid not in pages_a or uid not in pages_b:
            results.append(AccountComparison(uid))
            continue
        results.append(compare_account(
            uid, keys_by_day(sessions, uid, pages_a[uid]), keys_by_day(sessions, uid, pages_b[uid])
        ))
    return results
