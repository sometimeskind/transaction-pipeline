"""Flow tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import json
import os
from datetime import date

import httpx
import pytest

from transaction_pipeline import flow, mapping, metrics
from transaction_pipeline.enable_banking import EnableBankingError, RateLimitedError, SessionGoneError
from transaction_pipeline.firefly import ExternalIdConflictError, FireflyError, StoreResult
from transaction_pipeline.flow import BankBudgetSpentError, ConsentNeededError, Fetched, import_flow

UID_A = "00000000-0000-4000-8000-00000000000a"
UID_B = "00000000-0000-4000-8000-00000000000b"
UID_C = "00000000-0000-4000-8000-00000000000c"
IBAN_A = "XX00SYNTHETIC0000000001"
IBAN_B = "XX00SYNTHETIC0000000002"
IBAN_C = "XX00SYNTHETIC0000000003"
SESSIONS = {
    "bank-one": {
        "session_id": "00000000-0000-4000-8000-0000000000c1",
        "accounts": [
            {"uid": UID_A, "account_id": {"iban": IBAN_A}, "currency": "EUR"},
            {"uid": UID_B, "account_id": {"iban": IBAN_B}, "currency": "EUR"},
        ],
        "aspsp": {"name": "Synthetic Bank", "country": "XX"},
        "access": {"valid_until": "2027-01-01T00:00:00+00:00"},
    },
    "bank-two": {
        "session_id": "00000000-0000-4000-8000-0000000000c2",
        "accounts": [{"uid": UID_C, "account_id": {"iban": IBAN_C}, "currency": "GBP"}],
        "aspsp": {"name": "Other Synthetic Bank", "country": "YY"},
        "access": {"valid_until": "2027-02-01T00:00:00+00:00"},
    },
}
FROM, TO = date(2026, 1, 1), date(2026, 1, 8)


def tx(ref: str) -> dict:
    return {
        "entry_reference": ref,
        "booking_date": "2026-01-02",
        "transaction_amount": {"amount": "12.34", "currency": "EUR"},
        "credit_debit_indicator": "DBIT",
    }


def empty(*uids):
    return {uid: [{"transactions": []}] for uid in uids}


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


@pytest.fixture
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    for label, session in SESSIONS.items():
        flow.write_json(flow.session_path(label), session)
    return tmp_path


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


def test_sessions_live_one_file_per_label(state):
    assert flow.session_path("bank-one") == state / "sessions" / "bank-one.json"
    assert flow.load_sessions(flow.sessions_dir()) == SESSIONS
    assert os.stat(flow.session_path("bank-one")).st_mode & 0o777 == 0o600
    assert not list((state / "sessions").glob("*.tmp"))


@pytest.mark.parametrize("label", ["", "Bank", "../evil", "a b", "-x", "x" * 64])
def test_a_label_must_be_a_plain_name(label):
    with pytest.raises(ValueError):
        flow.session_path(label)


def test_no_sessions_asks_for_consent(tmp_path):
    with pytest.raises(ConsentNeededError, match="consent <label>"):
        flow.load_sessions(tmp_path / "sessions")


def test_consent_valid_until_is_unix_seconds():
    assert flow.consent_valid_until(SESSIONS["bank-one"]) == 1798761600.0


@pytest.mark.parametrize(("value", "write"), [(None, False), ("false", False), ("true", True), (" TRUE ", True)])
def test_firefly_write_defaults_to_false(monkeypatch, value, write):
    if value is None:
        monkeypatch.delenv("FIREFLY_WRITE", raising=False)
    else:
        monkeypatch.setenv("FIREFLY_WRITE", value)
    assert flow.firefly_write() is write


def test_a_firefly_write_typo_fails(monkeypatch):
    monkeypatch.setenv("FIREFLY_WRITE", "yes")
    with pytest.raises(ValueError, match="FIREFLY_WRITE"):
        flow.firefly_write()


def test_every_page_of_every_session_is_saved(tmp_path):
    bank = FakeBank({
        UID_A: [{"transactions": [tx("a1")], "continuation_key": "k1"}, {"transactions": [tx("a2")]}],
        **empty(UID_B, UID_C),
    })
    fetched = flow.save_pages(bank, SESSIONS, tmp_path, FROM, TO)

    assert fetched.errors == []
    assert fetched.pages == {
        UID_A: [tmp_path / UID_A / "page-1.json", tmp_path / UID_A / "page-2.json"],
        UID_B: [tmp_path / UID_B / "page-1.json"],
        UID_C: [tmp_path / UID_C / "page-1.json"],
    }
    assert json.loads(fetched.pages[UID_A][1].read_text()) == {"transactions": [tx("a2")]}
    assert os.stat(fetched.pages[UID_A][0]).st_mode & 0o777 == 0o600
    assert os.stat(tmp_path / UID_A).st_mode & 0o777 == 0o700
    assert bank.calls == [(UID_A, FROM, TO), (UID_B, FROM, TO), (UID_C, FROM, TO)]


def test_a_failed_account_keeps_its_pages_on_disk_but_out_of_the_store(tmp_path):
    bank = FakeBank({
        UID_A: [{"transactions": [tx("a1")], "continuation_key": "k1"}, EnableBankingError(500, None, "boom")],
        **empty(UID_B, UID_C),
    })
    fetched = flow.save_pages(bank, SESSIONS, tmp_path, FROM, TO)
    assert (tmp_path / UID_A / "page-1.json").exists()
    assert set(fetched.pages) == {UID_B, UID_C}
    assert [str(e) for e in fetched.errors] == ["Enable Banking 500 : boom"]


def test_a_gone_session_skips_its_accounts_and_not_the_other_banks(tmp_path):
    bank = FakeBank({UID_A: [SessionGoneError(401, "EXPIRED_SESSION", "expired")], **empty(UID_B, UID_C)})
    fetched = flow.save_pages(bank, SESSIONS, tmp_path, FROM, TO)

    assert set(fetched.pages) == {UID_C}
    assert [c[0] for c in bank.calls] == [UID_A, UID_C]
    [error] = fetched.errors
    assert isinstance(error, ConsentNeededError)
    assert "session bank-one" in str(error) and "EXPIRED_SESSION" in str(error)
    assert "consent bank-one" in str(error)


def test_a_rate_limit_skips_only_that_account_with_a_clear_message(tmp_path):
    bank = FakeBank({**empty(UID_A, UID_C), UID_B: [RateLimitedError(429, "ASPSP_RATE_LIMIT_EXCEEDED", None)]})
    fetched = flow.save_pages(bank, SESSIONS, tmp_path, FROM, TO)
    assert set(fetched.pages) == {UID_A, UID_C}
    [error] = fetched.errors
    assert isinstance(error, BankBudgetSpentError)
    assert "don't re-run today" in str(error)


class FakeFirefly:
    def __init__(self, existing: set[str] = frozenset()):
        self.existing = set(existing)
        self.stored: list[dict] = []

    def accounts(self, account_type="asset"):
        return [{"id": str(i), "attributes": {"iban": iban}} for i, iban in enumerate((IBAN_A, IBAN_B, IBAN_C))]

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
        mapping.OwnAccount("3", "Synthetic C", IBAN_C, True),
    ])
    calls = {"mapped": [], "imported": None, "rules": None}

    def from_firefly(cls, accounts, imported_ibans, rules=()):
        calls["imported"], calls["rules"] = list(imported_ibans), rules
        return book

    def map_transaction(raw, account, book):
        calls["mapped"].append((raw["entry_reference"], account.iban))
        if raw["entry_reference"].startswith("skip"):
            return None
        return {"transactions": [{"external_id": raw["entry_reference"]}]}

    monkeypatch.setattr(mapping.AccountBook, "from_firefly", classmethod(from_firefly))
    monkeypatch.setattr(mapping, "map_transaction", map_transaction)
    return calls


def test_store_maps_from_the_pages_on_disk(tmp_path, fake_mapping):
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], UID_B: [{"transactions": [tx("b1")]}],
                  UID_C: [{"transactions": [tx("c1")]}]}),
        SESSIONS, tmp_path, FROM, TO,
    )
    firefly = FakeFirefly(existing={"b1"})

    counts = flow.store_from_disk(firefly, SESSIONS, fetched.pages, rules=["rule"])

    assert counts == flow.StoreCounts(created=2, existing=1, skipped=1)
    assert fake_mapping["mapped"] == [("a1", IBAN_A), ("skip1", IBAN_A), ("b1", IBAN_B), ("c1", IBAN_C)]
    assert fake_mapping["rules"] == ["rule"]
    assert [p["transactions"][0]["external_id"] for p in firefly.stored] == ["a1", "c1"]


def test_every_session_account_counts_as_imported_even_when_its_fetch_failed(tmp_path, fake_mapping):
    flow.store_from_disk(FakeFirefly(), SESSIONS, {UID_C: []})
    assert fake_mapping["imported"] == [IBAN_A, IBAN_B, IBAN_C]
    assert fake_mapping["mapped"] == []


def test_a_rerun_from_the_same_pages_creates_nothing(tmp_path, fake_mapping):
    fetched = flow.save_pages(FakeBank({UID_A: [{"transactions": [tx("a1")]}], **empty(UID_B, UID_C)}),
                              SESSIONS, tmp_path, FROM, TO)
    firefly = FakeFirefly()
    flow.store_from_disk(firefly, SESSIONS, fetched.pages)
    assert flow.store_from_disk(firefly, SESSIONS, fetched.pages) == flow.StoreCounts(existing=1)


def test_a_session_account_without_iban_fails():
    sessions = {"bank-one": {**SESSIONS["bank-one"], "accounts": [{"uid": UID_A, "account_id": {}}]}}
    with pytest.raises(mapping.MappingError, match="no IBAN"):
        flow.store_from_disk(FakeFirefly(), sessions, {})


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


class FlowHarness:
    """Runs the flow body with its tasks and metrics replaced, so no Prefect server is needed."""

    def __init__(self, monkeypatch, errors=()):
        self.events: list = []
        self.run_dir = None

        def fetch_pages(run_dir, date_from, date_to):
            self.run_dir = run_dir
            flow.write_json(run_dir / UID_A / "page-1.json", {"transactions": []})
            return Fetched({UID_A: [run_dir / UID_A / "page-1.json"]}, list(errors))

        monkeypatch.setattr(flow, "fetch_pages", fetch_pages)
        monkeypatch.setattr(flow, "store_pages", lambda pages: self.events.append(("store", sorted(pages))))
        monkeypatch.setattr(flow, "prune_failed_runs", lambda root: self.events.append("prune"))
        monkeypatch.setattr(flow, "run_id", lambda: "run-1")
        monkeypatch.setattr(metrics, "push_consent_valid_until", lambda v: self.events.append(("consent", v)))
        monkeypatch.setattr(metrics, "push_success", lambda: self.events.append("success"))

    def run(self):
        import_flow.fn()


CONSENT = ("consent", {"bank-one": 1798761600.0, "bank-two": 1801440000.0})


def test_save_only_keeps_pages_stores_nothing_and_pushes_no_success(state, monkeypatch):
    monkeypatch.delenv("FIREFLY_WRITE", raising=False)
    harness = FlowHarness(monkeypatch)
    harness.run()
    assert harness.events == [CONSENT]
    assert (state / "raw" / "run-1" / UID_A / "page-1.json").exists()


def test_write_stores_deletes_the_pages_and_pushes_success(state, monkeypatch):
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch)
    harness.run()
    assert harness.events == [CONSENT, "prune", ("store", [UID_A]), "success"]
    assert not (state / "raw" / "run-1").exists()


def test_a_fetch_error_stores_the_rest_then_fails_and_keeps_the_pages(state, monkeypatch):
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch, errors=[BankBudgetSpentError("budget spent")])
    with pytest.raises(BankBudgetSpentError):
        harness.run()
    assert harness.events == [CONSENT, "prune", ("store", [UID_A])]
    assert (state / "raw" / "run-1").exists()


def test_several_fetch_errors_fail_together(state, monkeypatch):
    harness = FlowHarness(monkeypatch, errors=[BankBudgetSpentError("a"), ConsentNeededError("b")])
    with pytest.raises(ExceptionGroup) as exc:
        harness.run()
    assert len(exc.value.exceptions) == 2


def test_metrics_are_a_no_op_without_a_pushgateway(monkeypatch):
    monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
    monkeypatch.setattr(metrics, "pushadd_to_gateway", lambda *a, **k: pytest.fail("pushed"))
    metrics.push_success()
    metrics.push_consent_valid_until({"bank-one": 1.0})


def test_metrics_push_to_the_pipeline_job_with_a_session_label(monkeypatch):
    pushed = []
    monkeypatch.setenv("PUSHGATEWAY_URL", "pushgateway.example.test:9091")
    monkeypatch.setattr(metrics, "pushadd_to_gateway", lambda url, job, registry, timeout: pushed.append(
        (job, {(s.name, tuple(sorted(s.labels.items()))): s.value for m in registry.collect() for s in m.samples})))
    metrics.push_consent_valid_until({"bank-one": 1798761600.0, "bank-two": 1801440000.0})
    metrics.push_success()
    assert pushed[0] == ("transaction-pipeline", {
        ("transaction_pipeline_consent_valid_until_timestamp", (("session", "bank-one"),)): 1798761600.0,
        ("transaction_pipeline_consent_valid_until_timestamp", (("session", "bank-two"),)): 1801440000.0,
    })
    assert pushed[1][0] == "transaction-pipeline"
    assert [name for name, _ in pushed[1][1]] == ["transaction_pipeline_last_success_timestamp"]
