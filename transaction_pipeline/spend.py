"""The spend burn-up: Firefly III insight sums → one JSON payload → Home Assistant (homelab#2030).

Once a day, after the import, this flow turns the last 30 days of categorised
spending into the series the wall tablet charts: per category, for bookings with
no category, and overall, the cumulative spend since the window began, plus two
markers, a target and a six-window average. Home Assistant receives one webhook
POST with those series and shows them with the y-axis hidden, so the tablet shows
how spend sits against the markers, never an amount.

What reaches Home Assistant is daily aggregates only, never a transaction. The flow
itself sees no transaction either: it reads Firefly's insight endpoints, which
return sums per category for a date range (one request per day of the window and
one per baseline window), and the budget list for the targets.

- **Window**: the 30 days ending today, both inclusive. `cumulative[i]` is the
  spend from the window's first day through day `i`.
- **Spend** is withdrawals (Firefly's `insight/expense`), so transfers, moves
  between sub-accounts included, never count. A categorised deposit nets against
  its category (a refund), using `insight/income/category`. Uncategorised is
  expense only: income without a category is salary, not a refund.
- **Overall** is the sum of every category's net plus the uncategorised expense,
  so the per-category charts add up to it.
- **Target**: the budget named like the category (case-insensitive). A monthly
  auto-budget's amount is the target as it stands; any other budget contributes
  the limit that covers today, scaled to 30 days by the limit's length. No
  budget, or no limit today, means no target (`null`). The overall target is the
  budget named `SPEND_TOTAL_BUDGET` (default `Total`).
- **Average**: the mean spend of the six 30-day windows before this one (days
  -210 to -31), so the marker is a baseline the current window is compared with,
  not one it drags along.
- Only `SPEND_CURRENCY` (default `EUR`) rows count; Firefly reports a row per
  currency.

Categories come from Firefly's category list, so a category with nothing spent
this month still gets a flat series. Nothing is persisted: a run is a pure
function of Firefly's state on the day, and the next run replaces the sensor's
attributes in Home Assistant. A completed run pushes
`transaction_pipeline_spend_last_success_timestamp` to the Pushgateway, which the
homelab alerts on.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
from prefect import flow

from transaction_pipeline import firefly, metrics

log = logging.getLogger(__name__)

WINDOW_DAYS = 30
BASELINE_WINDOWS = 6
UNCATEGORISED = "uncategorised"
OVERALL = "overall"
WEBHOOK_TIMEOUT_SECONDS = 30.0


def spend_currency() -> str:
    return os.environ.get("SPEND_CURRENCY", "EUR")


def total_budget_name() -> str:
    return os.environ.get("SPEND_TOTAL_BUDGET", "Total")


@dataclass
class Series:
    """One chart: the cumulative curve and its two markers. Amounts as floats, for JSON."""

    cumulative: list[float]
    average: float
    target: float | None = None

    def as_dict(self) -> dict:
        return {"cumulative": self.cumulative, "average": self.average, "target": self.target}


@dataclass
class DayTotals:
    """Net spend per category on one day (or over one range), and the uncategorised expense."""

    categories: dict[str, Decimal] = field(default_factory=dict)
    uncategorised: Decimal = Decimal(0)


def _norm(name: str) -> str:
    return " ".join(name.split()).casefold()


def _amount(entry: dict) -> Decimal:
    """The absolute amount of an insight row. Firefly signs expenses negative."""
    return abs(Decimal(str(entry.get("difference") or "0")))


def _rows(entries: list[dict], currency: str) -> list[dict]:
    return [e for e in entries if e.get("currency_code") == currency]


def day_totals(client: firefly.FireflyClient, start: date, end: date, currency: str) -> DayTotals:
    """Net spend per category and the uncategorised expense over [start, end]."""
    totals = DayTotals()
    for entry in _rows(client.insight("expense", "category", start, end), currency):
        totals.categories[entry["name"]] = totals.categories.get(entry["name"], Decimal(0)) + _amount(entry)
    for entry in _rows(client.insight("income", "category", start, end), currency):
        totals.categories[entry["name"]] = totals.categories.get(entry["name"], Decimal(0)) - _amount(entry)
    for entry in _rows(client.insight("expense", "no-category", start, end), currency):
        totals.uncategorised += _amount(entry)
    return totals


def cumulative(days: list[DayTotals], names: list[str]) -> dict[str, list[Decimal]]:
    """Running totals per chart key over the window, every category present every day."""
    running = {name: Decimal(0) for name in names}
    running[UNCATEGORISED] = Decimal(0)
    running[OVERALL] = Decimal(0)
    curves: dict[str, list[Decimal]] = {key: [] for key in running}
    for day in days:
        for name in names:
            running[name] += day.categories.get(name, Decimal(0))
            curves[name].append(running[name])
        running[UNCATEGORISED] += day.uncategorised
        curves[UNCATEGORISED].append(running[UNCATEGORISED])
        running[OVERALL] += sum(day.categories.values(), Decimal(0)) + day.uncategorised
        curves[OVERALL].append(running[OVERALL])
    return curves


def targets(client: firefly.FireflyClient, today: date, currency: str) -> dict[str, Decimal]:
    """The 30-day target per budget name (normalised), from the budget list.

    A monthly auto-budget's amount stands as it is. Any other budget contributes
    the limit covering today, scaled to 30 days by the limit's length; a budget
    with no limit today is left out, so its chart gets only the average marker.
    """
    result: dict[str, Decimal] = {}
    for budget in client.budgets():
        attrs = budget["attributes"]
        if not attrs.get("active", True):
            continue
        key = _norm(attrs["name"])
        if attrs.get("auto_budget_period") == "monthly" and attrs.get("auto_budget_amount"):
            if (attrs.get("currency_code") or currency) == currency:
                result[key] = Decimal(str(attrs["auto_budget_amount"]))
            continue
        for limit in client.budget_limits(budget["id"], today, today):
            la = limit["attributes"]
            if la.get("currency_code", currency) != currency:
                continue
            start = date.fromisoformat(la["start"][:10])
            end = date.fromisoformat(la["end"][:10])
            length = (end - start).days + 1
            if length <= 0:
                continue
            result[key] = Decimal(str(la["amount"])) * WINDOW_DAYS / length
            break
    return result


def build_payload(client: firefly.FireflyClient, today: date) -> dict:
    currency = spend_currency()
    window_start = today - timedelta(days=WINDOW_DAYS - 1)
    days = [window_start + timedelta(days=i) for i in range(WINDOW_DAYS)]
    names = sorted({c["attributes"]["name"] for c in client.categories()})

    per_day = [day_totals(client, d, d, currency) for d in days]
    # Categories that have spend but are gone from the list (deleted mid-window)
    # still have to add up into overall, so they are charted too.
    seen = {name for day in per_day for name in day.categories}
    names = sorted(set(names) | seen)
    curves = cumulative(per_day, names)

    baseline = DayTotals()
    for i in range(1, BASELINE_WINDOWS + 1):
        end = window_start - timedelta(days=WINDOW_DAYS * (i - 1) + 1)
        start = end - timedelta(days=WINDOW_DAYS - 1)
        window = day_totals(client, start, end, currency)
        for name, amount in window.categories.items():
            baseline.categories[name] = baseline.categories.get(name, Decimal(0)) + amount
        baseline.uncategorised += window.uncategorised

    def average(key: str) -> Decimal:
        if key == OVERALL:
            total = sum(baseline.categories.values(), Decimal(0)) + baseline.uncategorised
        elif key == UNCATEGORISED:
            total = baseline.uncategorised
        else:
            total = baseline.categories.get(key, Decimal(0))
        return total / BASELINE_WINDOWS

    target_by_name = targets(client, today, currency)

    def series(key: str, target_key: str | None) -> Series:
        target = target_by_name.get(target_key) if target_key else None
        return Series(
            cumulative=[round(float(v), 2) for v in curves[key]],
            average=round(float(average(key)), 2),
            target=round(float(target), 2) if target is not None else None,
        )

    return {
        "as_of": today.isoformat(),
        "window_start": window_start.isoformat(),
        "days": [d.isoformat() for d in days],
        "currency": currency,
        "overall": series(OVERALL, _norm(total_budget_name())).as_dict(),
        "uncategorised": series(UNCATEGORISED, None).as_dict(),
        "categories": {name: series(name, _norm(name)).as_dict() for name in names},
    }


def post_to_home_assistant(payload: dict) -> None:
    """One POST to the webhook URL. HA answers 200 whether or not a trigger listened, so log the size."""
    url = os.environ["HA_WEBHOOK_URL"]
    response = httpx.post(url, json=payload, timeout=WEBHOOK_TIMEOUT_SECONDS)
    response.raise_for_status()
    log.info("posted %d categories for %s to Home Assistant", len(payload["categories"]), payload["as_of"])


def firefly_client() -> firefly.FireflyClient:
    return firefly.FireflyClient(os.environ["FIREFLY_URL"], os.environ["FIREFLY_TOKEN"])


@flow(name="spend-burn-up", persist_result=False)
def spend_flow(as_of: str | None = None) -> None:
    """Build the 30-day burn-up series from Firefly's insight sums and post them to Home Assistant.

    `as_of` (an ISO date; default today) is for re-rendering a past day.
    """
    today = date.fromisoformat(as_of) if as_of else datetime.now(UTC).date()
    with firefly_client() as client:
        payload = build_payload(client, today)
    post_to_home_assistant(payload)
    metrics.push_spend_success()
