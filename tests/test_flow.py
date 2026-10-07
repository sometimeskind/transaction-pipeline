"""Flow tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import json
import os
from datetime import date

import httpx
import pytest

from transaction_pipeline import flow, mapping, metrics
from transaction_pipeline.enable_banking import EnableBankingError, RateLimitedError, SessionGoneError
from transaction_pipeline.firefly import ExternalIdConflictError, FireflyError, StoreResult
from transaction_pipeline.flow import BankBudgetSpentError, ConsentNeededError, import_flow

UID_A = "00000000-0000-4000-8000-00000000000a"
UID_B = "00000000-0000-4000-8000-00000000000b"
IBAN_A = "XX00SYNTHETIC0000000001"
IBAN_B = "XX00SYNTHETIC0000000002"
SESSION = {
    "session_id": "00000000-0000-4000-8000-0000000000c1",
    "accounts": [
        {"uid": UID_A, "account_id": {"iban": IBAN_A}, "currency": "EUR"},
        {"uid": UID_B, "account_id": {"iban": IBAN_B}, "currency": "EUR"},
    ],
    "aspsp": {"name": "Synthetic Bank", "country": "XX"},
    "access": {"valid_until": "2027-01-01T00:00:00+00:00"},
}
FROM, TO = date(2026, 1, 1), date(2026, 1, 8)


def tx(ref: str) -> dict:
    return {
        "entry_reference": ref,
        "booking_date": "2026-01-02",
        "transaction_amount": {"amount": "12.34", "currency": "EUR"},
        "credit_debit_indicator": "DBIT",
    }


class FakeBank:
    """Yields the given pages per account; an exception in the list is raised in its place."""

    def __init__(self, pages: dict[str, list]):
        self.pages = pages
        self.calls: list[tuple] = []

    def transaction_pages(self, uid, date_from, date_to):
        self.calls.append((uid, date_from, date_to))
        for page in self.pages[uid]:
            if isinstance(page, Exception):
                raise page
            yield page


def test_flow_is_registered_under_its_deployment_name():
    assert import_flow.name == "transaction-import"


def test_fetch_has_no_retries_and_store_retries():
    assert flow.fetch_pages.retries == 0
    assert flow.store_pages.retries == flow.STORE_RETRIES > 0


def test_nothing_is_persisted_to_prefect_result_storage():
    assert flow.fetch_pages.persist_result is False
    assert flow.store_pages.persist_result is False
    assert import_flow.persist_result is False


def test_tasks_take_no_session_or_transaction_data():
    assert set(flow.fetch_pages.fn.__annotations__) - {"return"} == {"run_dir", "date_from", "date_to"}
    assert set(flow.store_pages.fn.__annotations__) - {"return"} == {"pages"}


def test_state_dir_comes_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    assert flow.session_path() == tmp_path / "session.json"


def test_session_round_trips_and_is_owner_only(tmp_path):
    path = tmp_path / "session.json"
    flow.write_session(path, SESSION)
    assert flow.load_session(path) == SESSION
    assert os.stat(path).st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob("*.tmp"))


def test_a_missing_session_asks_for_consent(tmp_path):
    with pytest.raises(ConsentNeededError, match="consent"):
        flow.load_session(tmp_path / "session.json")


def test_consent_valid_until_is_unix_seconds():
    assert flow.consent_valid_until(SESSION) == 1798761600.0


def test_every_page_is_saved_per_account(tmp_path):
    bank = FakeBank({
        UID_A: [{"transactions": [tx("a1")], "continuation_key": "k1"}, {"transactions": [tx("a2")]}],
        UID_B: [{"transactions": []}],
    })
    pages = flow.save_pages(bank, SESSION, tmp_path, FROM, TO)

    assert pages == {
        UID_A: [tmp_path / UID_A / "page-1.json", tmp_path / UID_A / "page-2.json"],
        UID_B: [tmp_path / UID_B / "page-1.json"],
    }
    assert json.loads(pages[UID_A][1].read_text()) == {"transactions": [tx("a2")]}
    assert os.stat(pages[UID_A][0]).st_mode & 0o777 == 0o600
    assert os.stat(tmp_path / UID_A).st_mode & 0o777 == 0o700
    assert bank.calls == [(UID_A, FROM, TO), (UID_B, FROM, TO)]


def test_pages_before_a_failure_stay_on_disk(tmp_path):
    bank = FakeBank({UID_A: [{"transactions": [tx("a1")], "continuation_key": "k1"}, EnableBankingError(500, None, "boom")]})
    with pytest.raises(EnableBankingError):
        flow.save_pages(bank, SESSION, tmp_path, FROM, TO)
    assert (tmp_path / UID_A / "page-1.json").exists()


def test_a_gone_session_fails_the_run_asking_for_consent(tmp_path):
    bank = FakeBank({UID_A: [SessionGoneError(401, "EXPIRED_SESSION", "expired")]})
    with pytest.raises(ConsentNeededError, match="EXPIRED_SESSION.*consent"):
        flow.save_pages(bank, SESSION, tmp_path, FROM, TO)


def test_a_rate_limit_fails_the_run_with_a_clear_message(tmp_path):
    bank = FakeBank({UID_A: [{"transactions": []}], UID_B: [RateLimitedError(429, "ASPSP_RATE_LIMIT_EXCEEDED", None)]})
    with pytest.raises(BankBudgetSpentError, match="don't re-run today"):
        flow.save_pages(bank, SESSION, tmp_path, FROM, TO)


class FakeFirefly:
    def __init__(self, existing: set[str] = frozenset()):
        self.existing = set(existing)
        self.stored: list[dict] = []

    def accounts(self, account_type="asset"):
        return [{"id": "1", "attributes": {"iban": IBAN_A}}, {"id": "2", "attributes": {"iban": IBAN_B}}]

    def store_if_absent(self, payload):
        ref = payload["transactions"][0]["external_id"]
        if ref in self.existing:
            return StoreResult(created=False, group_id="old")
        self.existing.add(ref)
        self.stored.append(payload)
        return StoreResult(created=True, group_id="new")


@pytest.fixture
def fake_mapping(monkeypatch):
    """Stand-in for mapping.py, so these tests cover the flow's plumbing only."""
    book = mapping.AccountBook([
        mapping.OwnAccount("1", "Synthetic A", IBAN_A, True),
        mapping.OwnAccount("2", "Synthetic B", IBAN_B, True),
    ])
    seen: list[tuple[str, str]] = []

    def map_transaction(raw, account, book):
        seen.append((raw["entry_reference"], account.iban))
        if raw["entry_reference"].startswith("skip"):
            return None
        return {"transactions": [{"external_id": raw["entry_reference"]}]}

    monkeypatch.setattr(mapping.AccountBook, "from_firefly", classmethod(lambda cls, accounts, imported_ibans: book))
    monkeypatch.setattr(mapping, "map_transaction", map_transaction)
    return seen


def test_store_maps_from_the_pages_on_disk(tmp_path, fake_mapping):
    pages = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], UID_B: [{"transactions": [tx("b1")]}]}),
        SESSION, tmp_path, FROM, TO,
    )
    firefly = FakeFirefly(existing={"b1"})

    counts = flow.store_from_disk(firefly, SESSION, pages)

    assert counts == flow.StoreCounts(created=1, existing=1, skipped=1)
    assert fake_mapping == [("a1", IBAN_A), ("skip1", IBAN_A), ("b1", IBAN_B)]
    assert [p["transactions"][0]["external_id"] for p in firefly.stored] == ["a1"]


def test_a_rerun_from_the_same_pages_creates_nothing(tmp_path, fake_mapping):
    pages = flow.save_pages(FakeBank({UID_A: [{"transactions": [tx("a1")]}], UID_B: [{"transactions": []}]}),
                            SESSION, tmp_path, FROM, TO)
    firefly = FakeFirefly()
    flow.store_from_disk(firefly, SESSION, pages)
    assert flow.store_from_disk(firefly, SESSION, pages) == flow.StoreCounts(existing=1)


def test_failed_runs_are_pruned_after_the_keep_period(tmp_path):
    now = 1_800_000_000
    old, recent = tmp_path / "old-run", tmp_path / "recent-run"
    for run_dir, age_days in ((old, flow.RAW_KEEP_FAILED_DAYS + 1), (recent, flow.RAW_KEEP_FAILED_DAYS - 1)):
        flow.write_json(run_dir / UID_A / "page-1.json", {"transactions": []})
        os.utime(run_dir, (now - age_days * 86400,) * 2)

    flow.prune_failed_runs(tmp_path, now=now)

    assert not old.exists()
    assert (recent / UID_A / "page-1.json").exists()


def test_pruning_without_a_raw_dir_is_a_no_op(tmp_path):
    flow.prune_failed_runs(tmp_path / "raw")


def test_a_session_account_without_iban_fails(tmp_path):
    session = {**SESSION, "accounts": [{"uid": UID_A, "account_id": {}}]}
    with pytest.raises(mapping.MappingError, match="no IBAN"):
        flow.store_from_disk(FakeFirefly(), session, {})


@pytest.mark.parametrize(
    ("exc", "retry"),
    [
        (httpx.ConnectError("refused"), True),
        (FireflyError(503, "down"), True),
        (FireflyError(422, "invalid"), False),
        (ExternalIdConflictError("reused"), False),
        (mapping.MappingError("no entry_reference"), False),
    ],
)
def test_only_transient_firefly_errors_are_retried(exc, retry):
    assert flow.is_transient(exc) is retry


def test_metrics_are_a_no_op_without_a_pushgateway(monkeypatch):
    monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
    monkeypatch.setattr(metrics, "pushadd_to_gateway", lambda *a, **k: pytest.fail("pushed"))
    metrics.push_success()
    metrics.push_consent_valid_until(1.0)


def test_metrics_push_to_the_pipeline_job(monkeypatch):
    pushed = []
    monkeypatch.setenv("PUSHGATEWAY_URL", "pushgateway.example.test:9091")
    monkeypatch.setattr(metrics, "pushadd_to_gateway", lambda url, job, registry, timeout: pushed.append(
        (job, {m.name: m.samples[0].value for m in registry.collect()})))
    metrics.push_consent_valid_until(1798761600.0)
    metrics.push_success()
    assert pushed[0] == ("transaction-pipeline", {"transaction_pipeline_consent_valid_until_timestamp": 1798761600.0})
    assert pushed[1][0] == "transaction-pipeline"
    assert set(pushed[1][1]) == {"transaction_pipeline_last_success_timestamp"}
