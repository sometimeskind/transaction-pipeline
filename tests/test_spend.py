"""Spend burn-up tests. Every amount and name here is synthetic (see the README's data rule)."""

from datetime import date, timedelta
from decimal import Decimal

import httpx
import pytest
import respx

from transaction_pipeline import metrics, spend
from transaction_pipeline.firefly import FireflyClient

URL = "https://firefly.example.test"
API = f"{URL}/api/v1"
TODAY = date(2026, 3, 31)
WINDOW_START = date(2026, 3, 2)


def _page(data: list) -> dict:
    return {"data": data, "meta": {"pagination": {"current_page": 1, "total_pages": 1}}}


def _row(name: str | None, amount: str, currency: str = "EUR") -> dict:
    row = {"difference": amount, "difference_float": float(amount), "currency_id": "1", "currency_code": currency}
    if name is not None:
        row.update({"id": "9", "name": name})
    return row


def _budget(budget_id: str, name: str, period: str | None = None, amount: str | None = None, active=True) -> dict:
    return {
        "type": "budgets",
        "id": budget_id,
        "attributes": {
            "name": name,
            "active": active,
            "auto_budget_type": "reset" if period else "none",
            "auto_budget_period": period,
            "auto_budget_amount": amount,
            "currency_code": "EUR",
        },
    }


def _limit(start: str, end: str, amount: str) -> dict:
    return {"type": "budget_limits", "id": "3", "attributes": {"start": start, "end": end, "amount": amount, "currency_code": "EUR"}}


@pytest.fixture
def client():
    with FireflyClient(URL, "synthetic-token") as c:
        yield c


class FakeFirefly:
    """Routes the insight calls by their date range, so a test states what each day and window holds."""

    def __init__(self):
        self.expense: dict[tuple[str, str], list[dict]] = {}
        self.income: dict[tuple[str, str], list[dict]] = {}
        self.no_category: dict[tuple[str, str], list[dict]] = {}
        self.calls: list[tuple[str, str, str]] = []

    def on(self, day: date, expense=(), income=(), no_category=()):
        key = (day.isoformat(), day.isoformat())
        self.expense[key] = list(expense)
        self.income[key] = list(income)
        self.no_category[key] = list(no_category)

    def on_range(self, start: date, end: date, expense=(), income=(), no_category=()):
        key = (start.isoformat(), end.isoformat())
        self.expense[key] = list(expense)
        self.income[key] = list(income)
        self.no_category[key] = list(no_category)

    def handler(self, table_name: str):
        table = getattr(self, table_name)

        def respond(request: httpx.Request) -> httpx.Response:
            key = (request.url.params["start"], request.url.params["end"])
            self.calls.append((table_name, *key))
            return httpx.Response(200, json={"data": table.get(key, [])})

        return respond

    def mount(self):
        respx.get(f"{API}/insight/expense/category").mock(side_effect=self.handler("expense"))
        respx.get(f"{API}/insight/income/category").mock(side_effect=self.handler("income"))
        respx.get(f"{API}/insight/expense/no-category").mock(side_effect=self.handler("no_category"))


@respx.mock
def test_payload_has_the_window_the_curves_and_the_markers(client, monkeypatch):
    monkeypatch.delenv("SPEND_CURRENCY", raising=False)
    monkeypatch.delenv("SPEND_TOTAL_BUDGET", raising=False)
    fake = FakeFirefly()
    fake.on(WINDOW_START, expense=[_row("Groceries", "-10.00"), _row("Fun", "-5.00")], no_category=[_row(None, "-1.00")])
    fake.on(WINDOW_START.replace(day=3), expense=[_row("Groceries", "-20.00")], income=[_row("Groceries", "4.00")])
    fake.on(TODAY, expense=[_row("Fun", "-2.50")], no_category=[_row(None, "-0.50")])
    # Baseline: six windows of 30 days ending the day before the window starts.
    for i in range(6):
        end = WINDOW_START.replace(day=1) - timedelta(days=30 * i)
        start = end - timedelta(days=29)
        fake.on_range(start, end, expense=[_row("Groceries", "-60.00")], no_category=[_row(None, "-6.00")])
    fake.mount()
    respx.get(f"{API}/categories").respond(json=_page([
        {"type": "categories", "id": "1", "attributes": {"name": "Groceries"}},
        {"type": "categories", "id": "2", "attributes": {"name": "Fun"}},
        {"type": "categories", "id": "3", "attributes": {"name": "Health"}},
    ]))
    respx.get(f"{API}/budgets").respond(json=_page([
        _budget("1", "groceries", "monthly", "300.00"),
        _budget("2", "Total", "monthly", "900.00"),
    ]))

    payload = spend.build_payload(client, TODAY)

    assert payload["as_of"] == "2026-03-31"
    assert payload["window_start"] == "2026-03-02"
    assert len(payload["days"]) == 30 and payload["days"][0] == "2026-03-02" and payload["days"][-1] == "2026-03-31"
    assert payload["currency"] == "EUR"
    groceries = payload["categories"]["Groceries"]
    # Refund nets: 10 + (20 - 4) = 26 from day two on.
    assert groceries["cumulative"][0] == 10.0
    assert groceries["cumulative"][1] == 26.0
    assert groceries["cumulative"][-1] == 26.0
    assert groceries["target"] == 300.0
    assert groceries["average"] == 60.0
    fun = payload["categories"]["Fun"]
    assert fun["cumulative"][-1] == 7.5 and fun["target"] is None and fun["average"] == 0.0
    # A category with nothing spent still gets a flat series.
    assert payload["categories"]["Health"]["cumulative"] == [0.0] * 30
    assert payload["uncategorised"]["cumulative"][0] == 1.0
    assert payload["uncategorised"]["cumulative"][-1] == 1.5
    assert payload["uncategorised"]["average"] == 6.0
    assert payload["uncategorised"]["target"] is None
    overall = payload["overall"]
    assert overall["cumulative"][0] == 16.0
    assert overall["cumulative"][-1] == 26.0 + 7.5 + 1.5
    assert overall["target"] == 900.0
    assert overall["average"] == 66.0
    # One call per table per day of the window, plus one per baseline window.
    assert len([c for c in fake.calls if c[0] == "expense"]) == 36
    assert ("expense", "2026-01-31", "2026-03-01") in fake.calls
    assert ("expense", "2025-09-03", "2025-10-02") in fake.calls


@respx.mock
def test_other_currencies_are_ignored(client, monkeypatch):
    monkeypatch.delenv("SPEND_CURRENCY", raising=False)
    fake = FakeFirefly()
    fake.on(TODAY, expense=[_row("Fun", "-100.00", currency="GBP"), _row("Fun", "-1.00")])
    fake.mount()
    respx.get(f"{API}/categories").respond(json=_page([]))
    respx.get(f"{API}/budgets").respond(json=_page([]))
    payload = spend.build_payload(client, TODAY)
    assert payload["categories"]["Fun"]["cumulative"][-1] == 1.0


@respx.mock
def test_a_non_monthly_limit_is_scaled_to_thirty_days(client, monkeypatch):
    monkeypatch.delenv("SPEND_CURRENCY", raising=False)
    FakeFirefly().mount()
    respx.get(f"{API}/categories").respond(json=_page([{"type": "categories", "id": "1", "attributes": {"name": "Fun"}}]))
    respx.get(f"{API}/budgets").respond(json=_page([
        _budget("7", "Fun"),
        _budget("8", "Idle"),
        _budget("9", "Old", active=False),
    ]))
    # A quarter: 90 days at 900 is 300 per 30 days.
    limits_fun = respx.get(f"{API}/budgets/7/limits").respond(
        json={"data": [_limit("2026-01-01T00:00:00+01:00", "2026-03-31T00:00:00+02:00", "900.00")]}
    )
    limits_idle = respx.get(f"{API}/budgets/8/limits").respond(json={"data": []})
    limits_old = respx.get(f"{API}/budgets/9/limits")
    payload = spend.build_payload(client, TODAY)
    assert payload["categories"]["Fun"]["target"] == 300.0
    assert limits_fun.calls.last.request.url.params["start"] == "2026-03-31"
    assert limits_idle.called and not limits_old.called


def test_targets_match_budget_names_loosely(monkeypatch):
    assert spend._norm("  Eating   Out ") == "eating out"
    assert spend._norm("GROCERIES") == spend._norm("groceries")


def test_cumulative_sums_every_key_every_day():
    days = [
        spend.DayTotals({"a": Decimal("1")}, Decimal("0.5")),
        spend.DayTotals({"b": Decimal("2")}, Decimal("0")),
    ]
    curves = spend.cumulative(days, ["a", "b"])
    assert curves["a"] == [Decimal("1"), Decimal("1")]
    assert curves["b"] == [Decimal("0"), Decimal("2")]
    assert curves[spend.UNCATEGORISED] == [Decimal("0.5"), Decimal("0.5")]
    assert curves[spend.OVERALL] == [Decimal("1.5"), Decimal("3.5")]


@respx.mock
def test_post_sends_the_payload_as_json_and_fails_loudly(monkeypatch):
    monkeypatch.setenv("HA_WEBHOOK_URL", "http://ha.example.test:8123/api/webhook/synthetic-id")
    hook = respx.post("http://ha.example.test:8123/api/webhook/synthetic-id").respond(200)
    spend.post_to_home_assistant({"as_of": "2026-03-31", "categories": {}})
    assert hook.calls.last.request.headers["content-type"] == "application/json"
    hook.respond(500)
    with pytest.raises(httpx.HTTPStatusError):
        spend.post_to_home_assistant({"as_of": "2026-03-31", "categories": {}})


def test_flow_posts_then_pushes_success(monkeypatch):
    events = []
    monkeypatch.setattr(spend, "firefly_client", lambda: _NullClient())
    monkeypatch.setattr(spend, "build_payload", lambda client, today: events.append(("build", today)) or {"as_of": today.isoformat(), "categories": {}})
    monkeypatch.setattr(spend, "post_to_home_assistant", lambda payload: events.append(("post", payload["as_of"])))
    monkeypatch.setattr(metrics, "push_spend_success", lambda: events.append("success"))
    spend.spend_flow.fn(as_of="2026-03-31")
    assert events == [("build", TODAY), ("post", "2026-03-31"), "success"]


def test_spend_success_goes_to_the_pipeline_job(monkeypatch):
    pushed = []
    monkeypatch.setenv("PUSHGATEWAY_URL", "pushgateway.example.test:9091")
    monkeypatch.setattr(metrics, "pushadd_to_gateway", lambda url, job, registry, timeout: pushed.append(
        (job, [s.name for m in registry.collect() for s in m.samples])))
    metrics.push_spend_success()
    assert pushed == [("transaction-pipeline", ["transaction_pipeline_spend_last_success_timestamp"])]


class _NullClient:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None
