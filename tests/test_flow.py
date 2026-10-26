"""Flow tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import contextlib
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import cloudpickle
import httpx
import pytest

from transaction_pipeline import compare, flow, mapping, metrics, monzo
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


def keys(kind: str, value: str, *raws: dict) -> list[str]:
    return mapping.external_ids((kind, value), list(raws))


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


def test_deployment_parameters_validate_after_pickling_by_value():
    # A served run gets the flow pickled by value (prefect.bundles), which drops globals
    # used only in annotations; .fn() in the other tests skips validation altogether.
    cloudpickle.register_pickle_by_value(flow)
    try:
        pickled = cloudpickle.loads(cloudpickle.dumps(import_flow))
    finally:
        cloudpickle.unregister_pickle_by_value(flow)
    params = pickled.validate_parameters({"replay_run": ["run-a", "run-b"], "since": "2023-10-13"})
    assert params["since"] == "2023-10-13"
    assert params["replay_run"] == ["run-a", "run-b"]


def test_fetch_has_no_retries_and_store_retries():
    assert flow.fetch_pages.retries == 0
    assert flow.store_pages.retries == flow.STORE_RETRIES > 0


def test_nothing_is_persisted_to_prefect_result_storage():
    assert flow.fetch_pages.persist_result is False
    assert flow.store_pages.persist_result is False
    assert import_flow.persist_result is False


def test_tasks_take_no_session_or_transaction_data():
    assert set(flow.fetch_pages.fn.__annotations__) - {"return"} == {"run_dir", "date_from", "date_to"}
    assert set(flow.store_pages.fn.__annotations__) - {"return"} == {"pages", "write", "since"}


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

    def find_by_external_id(self, external_id):
        return {"id": "old", "attributes": {"transactions": [{"external_id": external_id}]}} if external_id in self.existing else None

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

    def from_firefly(cls, accounts, imported_ibans, rules=(), imported_numbers=()):
        calls["imported"], calls["rules"] = list(imported_ibans), rules
        calls["imported_numbers"] = list(imported_numbers)
        return book

    def map_transaction(raw, account, book, external_id, paired=None):
        calls["mapped"].append((raw["entry_reference"], account.iban))
        if raw["entry_reference"].startswith("skip"):
            return None
        return {"transactions": [{"external_id": external_id}]}

    monkeypatch.setattr(mapping.AccountBook, "from_firefly", classmethod(from_firefly))
    monkeypatch.setattr(mapping, "map_transaction", map_transaction)
    return calls


def test_store_maps_from_the_pages_on_disk(tmp_path, fake_mapping):
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], UID_B: [{"transactions": [tx("b1")]}],
                  UID_C: [{"transactions": [tx("c1")]}]}),
        SESSIONS, tmp_path, FROM, TO,
    )
    firefly = FakeFirefly(existing=set(keys("iban", IBAN_B, tx("b1"))))

    counts = flow.store_from_disk(firefly, SESSIONS, fetched.pages, rules=["rule"])

    assert counts == flow.StoreCounts(created=2, existing=1, skipped=1)
    assert fake_mapping["mapped"] == [("a1", IBAN_A), ("skip1", IBAN_A), ("b1", IBAN_B), ("c1", IBAN_C)]
    assert fake_mapping["rules"] == ["rule"]
    assert [p["transactions"][0]["external_id"] for p in firefly.stored] == [
        keys("iban", IBAN_A, tx("a1"))[0], keys("iban", IBAN_C, tx("c1"))[0]]


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


def test_store_without_write_maps_but_stores_nothing(tmp_path, fake_mapping):
    fetched = flow.save_pages(FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], **empty(UID_B, UID_C)}),
                              SESSIONS, tmp_path, FROM, TO)
    firefly = FakeFirefly()
    assert flow.store_from_disk(firefly, SESSIONS, fetched.pages, write=False) == flow.StoreCounts(
        skipped=1, not_written=1)
    assert firefly.stored == []


def test_saved_pages_come_back_per_account_in_page_order(tmp_path):
    for n in (1, 2, 10):
        flow.write_json(tmp_path / "run-1" / UID_A / f"page-{n}.json", {"transactions": []})
    flow.write_json(tmp_path / "run-1" / UID_C / "page-1.json", {"transactions": []})
    pages = flow.saved_pages(tmp_path / "run-1", SESSIONS)
    assert [p.name for p in pages[UID_A]] == ["page-1.json", "page-2.json", "page-10.json"]
    assert set(pages) == {UID_A, UID_C}


def test_a_saved_run_with_accounts_no_session_knows_is_refused(tmp_path):
    flow.write_json(tmp_path / "run-1" / "00000000-0000-4000-8000-0000000000ff" / "page-1.json", {})
    with pytest.raises(RuntimeError, match="re-consent"):
        flow.saved_pages(tmp_path / "run-1", SESSIONS)


def test_a_missing_saved_run_is_refused(tmp_path):
    with pytest.raises(FileNotFoundError):
        flow.saved_pages(tmp_path / "run-1", SESSIONS)


def test_a_session_account_with_neither_iban_nor_identification_hash_fails():
    sessions = {"bank-one": {**SESSIONS["bank-one"], "accounts": [{"uid": UID_A, "account_id": {}}]}}
    with pytest.raises(mapping.MappingError, match="neither an IBAN nor an identification_hash"):
        flow.store_from_disk(FakeFirefly(), sessions, {})


def test_an_account_shared_by_two_sessions_is_mapped_once(tmp_path, fake_mapping):
    shared = {"uid": UID_C, "account_id": {"iban": IBAN_A}, "currency": "EUR"}
    sessions = {**SESSIONS, "bank-two": {**SESSIONS["bank-two"], "accounts": [shared]}}
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1")]}], **empty(UID_B), UID_C: [{"transactions": [tx("c1")]}]}),
        sessions, tmp_path, FROM, TO,
    )
    flow.store_from_disk(FakeFirefly(), sessions, fetched.pages)
    assert fake_mapping["mapped"] == [("a1", IBAN_A)]


def test_a_shared_account_is_mapped_from_the_other_session_when_one_fetch_failed(tmp_path, fake_mapping):
    shared = {"uid": UID_C, "account_id": {"iban": IBAN_A}, "currency": "EUR"}
    sessions = {**SESSIONS, "bank-two": {**SESSIONS["bank-two"], "accounts": [shared]}}
    pages = {UID_C: [flow.write_json(tmp_path / UID_C / "page-1.json", {"transactions": [tx("c1")]})]}
    firefly = FakeFirefly()
    flow.store_from_disk(firefly, sessions, pages)
    assert fake_mapping["mapped"] == [("c1", IBAN_A)]
    # The same keys as from the first session: they hash the IBAN, not the uid.
    assert [p["transactions"][0]["external_id"] for p in firefly.stored] == keys("iban", IBAN_A, tx("a1"))


SPACE_HASH = "synthetic-identification-hash-" + "0" * 100
UID_SPACE = "00000000-0000-4000-8000-00000000005a"
UID_SPACE_TOO = "00000000-0000-4000-8000-00000000005b"


class SpaceFirefly(FakeFirefly):
    """Firefly with named asset accounts, one of them a sub-account keyed by account number."""

    def accounts(self, account_type="asset"):
        return [{"id": str(i), "attributes": {"iban": iban, "name": f"Synthetic {i}"}}
                for i, iban in enumerate((IBAN_A, IBAN_B, IBAN_C))] + [
            {"id": "9", "attributes": {"iban": None, "account_number": SPACE_HASH, "name": "Synthetic space"}}]


def space(uid: str) -> dict:
    return {"uid": uid, "account_id": {"iban": None}, "identification_hash": SPACE_HASH, "currency": "EUR"}


def test_an_account_without_iban_is_matched_by_identification_hash(tmp_path):
    sessions = {"bank-one": {**SESSIONS["bank-one"], "accounts": [*SESSIONS["bank-one"]["accounts"], space(UID_SPACE)]}}
    fetched = flow.save_pages(FakeBank({UID_SPACE: [{"transactions": [tx("s1")]}], **empty(UID_A, UID_B)}),
                              sessions, tmp_path, FROM, TO)
    firefly = SpaceFirefly()
    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(created=1)
    [split] = firefly.stored[0]["transactions"]
    assert (split["external_id"], split["type"], split["source_id"]) == (
        keys("number", SPACE_HASH, tx("s1"))[0], "withdrawal", "9")


def test_the_same_identification_hash_in_two_sessions_is_mapped_once(tmp_path):
    sessions = {
        "bank-one": {**SESSIONS["bank-one"], "accounts": [space(UID_SPACE)]},
        "bank-two": {**SESSIONS["bank-two"], "accounts": [space(UID_SPACE_TOO)]},
    }
    # The bank may give each login its own references for the same booking.
    fetched = flow.save_pages(
        FakeBank({UID_SPACE: [{"transactions": [tx("s1")]}], UID_SPACE_TOO: [{"transactions": [tx("other-s1")]}]}),
        sessions, tmp_path, FROM, TO,
    )
    firefly = SpaceFirefly()
    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(created=1)
    assert [p["transactions"][0]["external_id"] for p in firefly.stored] == keys("number", SPACE_HASH, tx("s1"))
    # Fetched under the other login only (the first one's fetch failed), it is the same booking.
    only_too = {UID_SPACE_TOO: fetched.pages[UID_SPACE_TOO]}
    assert flow.store_from_disk(firefly, sessions, only_too) == flow.StoreCounts(existing=1)


def space_sessions() -> dict:
    return {"bank-one": {**SESSIONS["bank-one"], "accounts": [space(UID_SPACE)]}}


def store_run(tmp_path, firefly, run: str, pages: list[list[dict]]) -> flow.StoreCounts:
    fetched = flow.save_pages(FakeBank({UID_SPACE: [{"transactions": p} for p in pages]}),
                              space_sessions(), tmp_path / run, FROM, TO)
    return flow.store_from_disk(firefly, space_sessions(), fetched.pages)


def booking(ref: str | None = None, day: str = "2026-01-02", amount: str = "12.34") -> dict:
    return {**tx(ref), "booking_date": day, "transaction_amount": {"amount": amount, "currency": "EUR"}}


def test_identical_same_day_bookings_are_all_stored(tmp_path):
    firefly = SpaceFirefly()
    assert store_run(tmp_path, firefly, "run-1", [[booking(), booking(), booking()]]) == flow.StoreCounts(created=3)
    assert len({p["transactions"][0]["external_id"] for p in firefly.stored}) == 3


def test_a_booking_fetched_with_and_without_entry_reference_is_stored_once(tmp_path):
    firefly = SpaceFirefly()
    store_run(tmp_path, firefly, "run-1", [[booking(None), booking(None, amount="5.00")]])
    assert store_run(tmp_path, firefly, "run-2", [[booking("ref-1"), booking("ref-2", amount="5.00")]]) == (
        flow.StoreCounts(existing=2))


def test_the_same_bookings_split_over_other_pages_or_reordered_store_nothing_new(tmp_path):
    same_day = [booking(), booking(amount="5.00"), booking(), booking(day="2026-01-03")]
    firefly = SpaceFirefly()
    store_run(tmp_path, firefly, "run-1", [same_day])
    assert store_run(tmp_path, firefly, "run-2", [same_day[:1], same_day[1:]]) == flow.StoreCounts(existing=4)
    assert store_run(tmp_path, firefly, "run-3", [same_day[::-1]]) == flow.StoreCounts(existing=4)


def test_a_new_identical_booking_later_in_the_day_is_stored(tmp_path):
    firefly = SpaceFirefly()
    store_run(tmp_path, firefly, "run-1", [[booking()]])
    assert store_run(tmp_path, firefly, "run-2", [[booking(), booking()]]) == flow.StoreCounts(created=1, existing=1)


@pytest.mark.parametrize(
    ("exc", "retry"),
    [
        (httpx.ConnectError("refused"), True),
        (FireflyError(503, "down"), True),
        (FireflyError(422, "invalid"), False),
        (ExternalIdConflictError("reused"), False),
        (mapping.MappingError("no booking_date"), False),
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


def test_save_only_runs_are_never_pruned(tmp_path):
    now = 1_800_000_000
    run_dir = tmp_path / "backfill"
    flow.write_json(run_dir / flow.SAVE_ONLY_MARKER, {})
    flow.write_json(run_dir / UID_A / "page-1.json", {"transactions": []})
    os.utime(run_dir, (now - 365 * 86400,) * 2)
    flow.prune_failed_runs(tmp_path, now=now)
    assert (run_dir / UID_A / "page-1.json").exists()


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
        monkeypatch.setattr(flow, "store_pages", self.store_pages)
        monkeypatch.setattr(flow, "prune_failed_runs", lambda root: self.events.append("prune"))
        monkeypatch.setattr(flow, "run_id", lambda: "run-1")
        monkeypatch.setattr(metrics, "push_consent_valid_until", lambda v: self.events.append(("consent", v)))
        monkeypatch.setattr(metrics, "push_success", lambda: self.events.append("success"))

    def store_pages(self, pages, write=True, since=None):
        uids = sorted(pages) if isinstance(pages, dict) else [sorted(run) for run in pages]
        self.events.append(("store" if write else "map-only", uids, *([since] if since else [])))

    def run(self, **params):
        import_flow.fn(**params)


CONSENT = ("consent", {"bank-one": 1798761600.0, "bank-two": 1801440000.0})


def test_save_only_keeps_pages_stores_nothing_and_pushes_no_success(state, monkeypatch):
    monkeypatch.delenv("FIREFLY_WRITE", raising=False)
    harness = FlowHarness(monkeypatch)
    harness.run()
    assert harness.events == [CONSENT]
    assert (state / "raw" / "run-1" / UID_A / "page-1.json").exists()
    assert (state / "raw" / "run-1" / flow.SAVE_ONLY_MARKER).exists()


def test_write_stores_deletes_the_pages_and_pushes_success(state, monkeypatch):
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch)
    harness.run()
    assert harness.events == [CONSENT, "prune", ("store", [UID_A]), "success"]
    assert not (state / "raw" / "run-1").exists()


def test_a_write_run_leaves_no_save_only_marker(state, monkeypatch):
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch, errors=[BankBudgetSpentError("budget spent")])
    with pytest.raises(BankBudgetSpentError):
        harness.run()
    assert not (state / "raw" / "run-1" / flow.SAVE_ONLY_MARKER).exists()


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


@pytest.fixture
def saved_run(state):
    flow.write_json(state / "raw" / "backfill" / UID_A / "page-1.json", {"transactions": []})
    flow.write_json(state / "raw" / "backfill" / UID_C / "page-1.json", {"transactions": []})
    return state / "raw" / "backfill"


def test_replay_stores_a_saved_run_without_fetching_or_pruning_then_deletes_it(saved_run, monkeypatch):
    flow.write_json(saved_run / flow.SAVE_ONLY_MARKER, {})
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch)
    harness.run(replay_run="backfill")
    assert harness.events == [CONSENT, ("store", [[UID_A, UID_C]])]
    assert harness.run_dir is None
    assert not saved_run.exists()


def test_replay_without_write_maps_only(saved_run, monkeypatch):
    monkeypatch.delenv("FIREFLY_WRITE", raising=False)
    harness = FlowHarness(monkeypatch)
    harness.run(replay_run="backfill")
    assert harness.events == [CONSENT, ("map-only", [[UID_A, UID_C]])]
    assert (saved_run / UID_A / "page-1.json").exists()


@pytest.mark.parametrize("run", ["../sessions", "a/b", ""])
def test_replay_refuses_anything_but_a_run_id(state, monkeypatch, run):
    harness = FlowHarness(monkeypatch)
    with pytest.raises(ValueError, match="not a run id"):
        harness.run(replay_run=run)
    assert harness.events == [CONSENT]


def test_store_runs_through_the_real_mapping(tmp_path):
    class NamedFirefly(FakeFirefly):
        def accounts(self, account_type="asset"):
            return [{"id": str(i), "attributes": {"iban": iban, "name": f"Synthetic {i}"}}
                    for i, iban in enumerate((IBAN_A, IBAN_B, IBAN_C))]

    fetched = flow.save_pages(FakeBank({UID_A: [{"transactions": [tx("a1")]}], **empty(UID_B, UID_C)}),
                              SESSIONS, tmp_path, FROM, TO)
    firefly = NamedFirefly()
    rules = mapping.load_rules(tmp_path / "no-such-mapping.toml")
    assert flow.store_from_disk(firefly, SESSIONS, fetched.pages, rules) == flow.StoreCounts(created=1)
    [split] = firefly.stored[0]["transactions"]
    assert (split["external_id"], split["type"], split["source_id"]) == (
        keys("iban", IBAN_A, tx("a1"))[0], "withdrawal", "0")


def test_each_account_logs_its_pages_transaction_count_and_booking_dates(tmp_path, caplog):
    bank = FakeBank({
        UID_A: [{"transactions": [{**tx("a1"), "booking_date": "2026-01-05"}], "continuation_key": "k1"},
                {"transactions": [tx("a2"), {**tx("a3"), "booking_date": "2026-01-03"}]}],
        UID_B: [{"transactions": [{**tx("b1"), "booking_date": None}]}],
        **empty(UID_C),
    })
    with caplog.at_level("INFO", logger="transaction_pipeline.flow"):
        flow.save_pages(bank, SESSIONS, tmp_path, FROM, TO)

    assert caplog.messages == [
        f"session bank-one: account {UID_A}: 2 page(s) saved, 3 transaction(s) booked 2026-01-02 to 2026-01-05",
        f"session bank-one: account {UID_B}: 1 page(s) saved, 1 transaction(s) (1 without a booking_date)",
        f"session bank-two: account {UID_C}: 1 page(s) saved, 0 transaction(s)",
    ]


def test_store_logs_its_counts_per_account_and_in_total(state, monkeypatch, fake_mapping, caplog):
    pages = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], UID_B: [{"transactions": [tx("b1")]}],
                  **empty(UID_C)}),
        SESSIONS, state / "raw" / "run-1", FROM, TO,
    ).pages
    firefly = FakeFirefly(existing=set(keys("iban", IBAN_B, tx("b1"))))
    monkeypatch.setattr(flow, "firefly_client", lambda: contextlib.nullcontext(firefly))
    monkeypatch.setenv("MAPPING_CONFIG", str(state / "no-such-mapping.toml"))

    caplog.clear()
    with caplog.at_level("INFO", logger="transaction_pipeline.flow"):
        flow.store_pages.fn(pages)

    assert caplog.messages == [
        f"account {UID_A}: 1 created, 0 existing, 1 skipped, 0 not written",
        f"account {UID_B}: 0 created, 1 existing, 0 skipped, 0 not written",
        f"account {UID_C}: 0 created, 0 existing, 0 skipped, 0 not written",
        "all accounts: 1 created, 1 existing, 1 skipped, 0 not written",
    ]


def test_a_map_only_store_logs_what_it_did_not_write(state, monkeypatch, fake_mapping, caplog):
    pages = flow.save_pages(FakeBank({UID_A: [{"transactions": [tx("a1"), tx("skip1")]}], **empty(UID_B, UID_C)}),
                            SESSIONS, state / "raw" / "run-1", FROM, TO).pages
    monkeypatch.setattr(flow, "firefly_client", lambda: contextlib.nullcontext(FakeFirefly()))
    monkeypatch.setenv("MAPPING_CONFIG", str(state / "no-such-mapping.toml"))

    caplog.clear()
    with caplog.at_level("INFO", logger="transaction_pipeline.flow"):
        flow.store_pages.fn(pages, write=False)

    assert caplog.messages[0] == f"account {UID_A}: 0 created, 0 existing, 1 skipped, 1 not written"
    assert caplog.messages[-1] == "all accounts: 0 created, 0 existing, 1 skipped, 1 not written"


def test_the_flow_logs_info_to_the_prefect_run_log():
    """With PREFECT_LOGGING_EXTRA_LOGGERS (set in the image), the flow logger gets Prefect's API handler."""
    check = (
        "import logging\n"
        "from prefect.logging.configuration import setup_logging\n"
        "from prefect.logging.handlers import APILogHandler\n"
        "from transaction_pipeline import flow\n"
        "setup_logging()\n"
        "assert flow.log.isEnabledFor(logging.INFO)\n"
        "assert any(isinstance(h, APILogHandler) for h in flow.log.handlers)\n"
        "assert not logging.getLogger('transaction_pipeline.mapping').isEnabledFor(logging.INFO)\n"
    )
    env = {**os.environ, "PREFECT_LOGGING_EXTRA_LOGGERS": "transaction_pipeline.flow"}
    subprocess.run([sys.executable, "-c", check], env=env, check=True)


def test_the_image_sends_the_flow_logger_to_prefect():
    dockerfile = Path(__file__).parents[1] / "Dockerfile"
    if not dockerfile.exists():
        pytest.skip("runs from the repo, not inside the image")
    assert "ENV PREFECT_LOGGING_EXTRA_LOGGERS=transaction_pipeline.flow\n" in dockerfile.read_text()


def move(direction: str, text: str | None = "Synthetic sorter", day: str = "2026-01-02") -> dict:
    """An internal move: a SEPA credit transfer whose counterparty shows no account."""
    return {
        "booking_date": day,
        "transaction_amount": {"amount": "10.00", "currency": "EUR"},
        "credit_debit_indicator": direction,
        "bank_transaction_code": {"code": "ICDT" if direction == "DBIT" else "RCDT", "sub_code": "ESCT"},
        "remittance_information": [text] if text else [],
    }


# A booking on a later day, so the moves' day isn't the latest one in the pages.
LATER = {**tx("later"), "booking_date": "2026-01-05"}


def move_sessions() -> dict:
    """Two logins: an account and a space on one, an account on the other."""
    return {
        "bank-one": {**SESSIONS["bank-one"], "accounts": [SESSIONS["bank-one"]["accounts"][0], space(UID_SPACE)]},
        "bank-two": SESSIONS["bank-two"],
    }


def test_a_move_pairs_across_two_sessions_into_one_transfer(tmp_path):
    sessions = move_sessions()
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [LATER]}], UID_SPACE: [{"transactions": [move("DBIT")]}],
                  UID_C: [{"transactions": [move("CRDT", text=None)]}]}),
        sessions, tmp_path, FROM, TO,
    )
    firefly = SpaceFirefly()

    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(created=2, skipped=1)
    [split] = [p["transactions"][0] for p in firefly.stored if p["transactions"][0]["type"] == "transfer"]
    assert (split["source_id"], split["destination_id"]) == ("9", "2")
    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(existing=2, skipped=1)


def test_internal_moves_on_the_latest_day_wait_for_the_next_run(tmp_path):
    sessions = move_sessions()
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1")]}], UID_SPACE: [{"transactions": [move("DBIT")]}],
                  UID_C: [{"transactions": [move("CRDT")]}]}),
        sessions, tmp_path, FROM, TO,
    )
    assert flow.store_from_disk(SpaceFirefly(), sessions, fetched.pages) == flow.StoreCounts(created=1, deferred=2)


def test_internal_moves_wait_for_a_run_where_every_account_has_pages(tmp_path):
    sessions = move_sessions()
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [tx("a1")]}], UID_SPACE: [{"transactions": [move("DBIT")]}],
                  UID_C: [RateLimitedError(429, "ASPSP_RATE_LIMIT_EXCEEDED", None)]}),
        sessions, tmp_path, FROM, TO,
    )
    firefly = SpaceFirefly()

    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(created=1, deferred=1)
    assert [p["transactions"][0]["type"] for p in firefly.stored] == ["withdrawal"]


def test_an_unpaired_move_fails_the_store_after_storing_the_rest(tmp_path, caplog):
    sessions = move_sessions()
    fetched = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [LATER]}], UID_SPACE: [{"transactions": [move("CRDT")]}],
                  **empty(UID_C)}),
        sessions, tmp_path, FROM, TO,
    )
    firefly = SpaceFirefly()

    with caplog.at_level("INFO", logger="transaction_pipeline.flow"), pytest.raises(
            mapping.UnpairedMoveError, match="1 unpaired booking"):
        flow.store_from_disk(firefly, sessions, fetched.pages)
    assert len(firefly.stored) == 1
    assert f"account {UID_SPACE}: 0 created, 0 existing, 0 skipped, 0 not written, 1 unpaired" in caplog.messages


def test_several_runs_are_mapped_as_one_union_of_their_bookings(tmp_path):
    sessions = move_sessions()
    # Overlapping runs: the day they share is the same bookings, so the same keys.
    run_1 = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [booking(day="2026-01-01"), booking(day="2026-01-02")]}],
                  UID_SPACE: [{"transactions": [move("DBIT", day="2026-01-02")]}], **empty(UID_C)}),
        sessions, tmp_path / "run-1", FROM, TO,
    ).pages
    run_2 = flow.save_pages(
        FakeBank({UID_A: [{"transactions": [booking(day="2026-01-02"), booking(day="2026-01-03")]}],
                  **empty(UID_SPACE), UID_C: [{"transactions": [move("CRDT", day="2026-01-02")]}]}),
        sessions, tmp_path / "run-2", FROM, TO,
    ).pages
    firefly = SpaceFirefly()

    counts = flow.store_from_disk(firefly, sessions, [run_1, run_2], since=date(2026, 1, 2))

    assert counts == flow.StoreCounts(created=3, skipped=1)
    assert sorted(p["transactions"][0]["date"] for p in firefly.stored) == ["2026-01-02", "2026-01-02", "2026-01-03"]


def test_replay_maps_several_saved_runs_together(saved_run, monkeypatch):
    flow.write_json(saved_run.parent / "daily" / UID_A / "page-1.json", {"transactions": []})
    monkeypatch.setenv("FIREFLY_WRITE", "true")
    harness = FlowHarness(monkeypatch)
    harness.run(replay_run=["backfill", "daily"], since="2026-01-02")
    assert harness.events == [CONSENT, ("store", [[UID_A, UID_C], [UID_A]], date(2026, 1, 2))]
    assert not saved_run.exists()
    assert not (saved_run.parent / "daily").exists()


@pytest.mark.parametrize("runs", [[], ["backfill", "../sessions"]])
def test_replay_refuses_an_empty_list_or_any_bad_run_id(saved_run, monkeypatch, runs):
    harness = FlowHarness(monkeypatch)
    with pytest.raises(ValueError):
        harness.run(replay_run=runs)
    assert saved_run.exists()


def test_since_without_a_replay_is_refused(state, monkeypatch):
    harness = FlowHarness(monkeypatch)
    with pytest.raises(ValueError, match="since"):
        harness.run(since="2026-01-02")
    assert harness.events == []


# Monzo sessions: fetched from their own API, adapted when read back.

MONZO_ACC = "acc_0000synthetic00000000001"
MONZO_NUMBER = "04000412345678"
MONZO_POT = "pot_0000synthetic00000000001"


def monzo_session(token: bool = True, pots: list | None = None) -> dict:
    return {
        "provider": "monzo",
        "token": {"access_token": "access-1", "refresh_token": "refresh-1", "expires_at": "2027-01-01T00:00:00+00:00"} if token else None,
        "authorised_at": "2026-01-01T00:00:00+00:00",
        "access": {"valid_until": "2026-03-31T23:55:00+00:00"},
        "accounts": [{"uid": MONZO_ACC, "number": MONZO_NUMBER, "currency": "GBP", "type": "uk_retail"}],
        "pots": [{"id": MONZO_POT, "name": "Synthetic pot", "deleted": False}] if pots is None else pots,
    }


def mtx(id: str, amount: int = -510, **over) -> dict:
    base = {"id": id, "amount": amount, "currency": "GBP", "created": "2026-01-02T12:20:18Z",
            "settled": "2026-01-02T12:20:18Z", "description": "SYNTHETIC GROCER LONDON GBR",
            "merchant": {"id": "merch_1", "name": "Synthetic Grocer"}, "metadata": {}, "notes": ""}
    base.update(over)
    return base


class FakeMonzo:
    def __init__(self, pages: dict[str, list[dict]], errors: dict[str, Exception] | None = None):
        self.pages, self.errors, self.calls = pages, errors or {}, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass

    def transaction_pages(self, uid, since, before):
        self.calls.append((uid, since, before))
        if uid in self.errors:
            raise self.errors[uid]
        yield from self.pages.get(uid, [])


def test_account_key_uses_the_number_of_a_monzo_account():
    assert flow.account_key({"uid": MONZO_ACC, "number": MONZO_NUMBER}) == ("number", MONZO_NUMBER)


def test_a_monzo_session_is_fetched_from_its_own_api_with_the_settle_grace(tmp_path):
    sessions = {"bank-two": SESSIONS["bank-two"], "monzo": monzo_session()}
    fake = FakeMonzo({MONZO_ACC: [{"transactions": [mtx("tx_1")]}, {"transactions": [mtx("tx_2")]}]})
    fetched = flow.save_pages(FakeBank(empty(UID_C)), sessions, tmp_path, FROM, TO, monzo_clients=lambda label, s: fake)
    assert fetched.errors == []
    assert set(fetched.pages) == {UID_C, MONZO_ACC}
    assert fake.calls == [(MONZO_ACC, datetime(2025, 12, 2, tzinfo=UTC), datetime(2026, 1, 9, tzinfo=UTC))]
    saved = [json.loads(p.read_text()) for p in fetched.pages[MONZO_ACC]]
    assert [s["transactions"][0]["id"] for s in saved] == ["tx_1", "tx_2"]  # as the API returned them


def test_monzo_access_gone_skips_that_login_with_the_auth_hint(tmp_path):
    sessions = {"bank-two": SESSIONS["bank-two"], "monzo": monzo_session()}
    gone = monzo.AccessGoneError(403, "forbidden.insufficient_permissions", "no")
    fetched = flow.save_pages(FakeBank(empty(UID_C)), sessions, tmp_path, FROM, TO,
                              monzo_clients=lambda label, s: FakeMonzo({}, {MONZO_ACC: gone}))
    assert set(fetched.pages) == {UID_C}
    [error] = fetched.errors
    assert isinstance(error, ConsentNeededError)
    assert "monzo-auth monzo" in str(error) and "forbidden.insufficient_permissions" in str(error)


def test_another_monzo_error_fails_that_account_and_not_the_rest(tmp_path):
    other = "acc_0000synthetic00000000002"
    session = monzo_session()
    session["accounts"].append({"uid": other, "number": "04000499999999", "currency": "GBP"})
    fake = FakeMonzo({other: [{"transactions": []}]}, {MONZO_ACC: monzo.MonzoError(500, "internal_service", "oops")})
    fetched = flow.save_pages(FakeBank({}), {"monzo": session}, tmp_path, FROM, TO, monzo_clients=lambda label, s: fake)
    assert set(fetched.pages) == {other}
    assert [type(e) for e in fetched.errors] == [monzo.MonzoError]


def test_missing_monzo_credentials_fail_only_that_login(tmp_path, monkeypatch):
    monkeypatch.delenv("MONZO_CLIENT_ID", raising=False)
    monkeypatch.delenv("MONZO_CLIENT_SECRET", raising=False)
    sessions = {"bank-two": SESSIONS["bank-two"], "monzo": monzo_session()}
    fetched = flow.save_pages(FakeBank(empty(UID_C)), sessions, tmp_path, FROM, TO)
    assert set(fetched.pages) == {UID_C}
    [error] = fetched.errors
    assert isinstance(error, ConsentNeededError) and "MONZO_CLIENT_ID" in str(error)


def test_monzo_client_writes_a_refreshed_token_back_to_the_session_file(state, monkeypatch):
    monkeypatch.setenv("MONZO_CLIENT_ID", "client-1")
    monkeypatch.setenv("MONZO_CLIENT_SECRET", "secret-1")
    session = monzo_session()
    path = flow.write_json(flow.session_path("monzo"), session)
    with flow.monzo_client("monzo", session) as client:
        assert client.token.refresh_token == "refresh-1"
        client.on_token(monzo.Token("access-2", "refresh-2", datetime(2026, 1, 1, 6, tzinfo=UTC)))
    saved = json.loads(path.read_text())
    assert saved["token"] == {"access_token": "access-2", "refresh_token": "refresh-2", "expires_at": "2026-01-01T06:00:00+00:00"}
    assert saved["accounts"] == session["accounts"] and saved["pots"] == session["pots"]
    assert oct(path.stat().st_mode & 0o777) == "0o600"


def test_a_monzo_session_without_a_token_asks_for_monzo_auth(monkeypatch):
    monkeypatch.setenv("MONZO_CLIENT_ID", "client-1")
    monkeypatch.setenv("MONZO_CLIENT_SECRET", "secret-1")
    with pytest.raises(ConsentNeededError, match="monzo-auth monzo"):
        flow.monzo_client("monzo", monzo_session(token=False))


class MonzoFirefly(FakeFirefly):
    def __init__(self, pot: bool = True):
        super().__init__()
        self.pot = pot

    def accounts(self, account_type="asset"):
        accounts = [
            {"id": "1", "attributes": {"iban": IBAN_A, "name": "Synthetic A"}},
            {"id": "7", "attributes": {"iban": None, "account_number": MONZO_NUMBER, "name": "Synthetic current"}},
        ]
        if self.pot:
            accounts.append({"id": "8", "attributes": {"iban": None, "account_number": MONZO_POT, "name": "Synthetic pot"}})
        return accounts


def monzo_pages() -> list[dict]:
    return [{"transactions": [
        mtx("tx_1"),
        mtx("tx_2", amount=-2000, merchant=None, scheme="uk_retail_pot", description=MONZO_POT, metadata={"pot_id": MONZO_POT}),
        mtx("tx_3", settled=""),
    ]}]


def test_monzo_pages_are_adapted_when_stored_and_pot_moves_are_transfers(tmp_path):
    sessions = {"monzo": monzo_session()}
    fetched = flow.save_pages(FakeBank({}), sessions, tmp_path, FROM, TO,
                              monzo_clients=lambda label, s: FakeMonzo({MONZO_ACC: monzo_pages()}))
    firefly = MonzoFirefly()
    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(created=2)
    splits = [p["transactions"][0] for p in firefly.stored]
    assert [s["type"] for s in splits] == ["withdrawal", "transfer"]
    assert (splits[0]["source_id"], splits[0]["destination_name"], splits[0]["amount"], splits[0]["currency_code"]) == (
        "7", "Synthetic Grocer", "5.10", "GBP")
    assert (splits[1]["source_id"], splits[1]["destination_id"], splits[1]["amount"], splits[1]["internal_reference"]) == (
        "7", "8", "20.00", "tx_2")
    assert splits[0]["external_id"] == mapping.external_ids(("number", MONZO_NUMBER), monzo.adapt([mtx("tx_1")], {}))[0]
    # The same pages again store nothing new.
    assert flow.store_from_disk(firefly, sessions, fetched.pages) == flow.StoreCounts(existing=2)


def test_a_pot_without_a_firefly_account_fails_by_name(tmp_path):
    sessions = {"monzo": monzo_session()}
    fetched = flow.save_pages(FakeBank({}), sessions, tmp_path, FROM, TO,
                              monzo_clients=lambda label, s: FakeMonzo({MONZO_ACC: monzo_pages()}))
    firefly = MonzoFirefly(pot=False)
    with pytest.raises(mapping.MappingError, match="Synthetic pot"):
        flow.store_from_disk(firefly, sessions, fetched.pages)
    assert firefly.stored == []


def test_a_deleted_pot_needs_no_firefly_account(tmp_path):
    sessions = {"monzo": monzo_session(pots=[{"id": MONZO_POT, "name": "Synthetic pot", "deleted": True}])}
    fetched = flow.save_pages(FakeBank({}), sessions, tmp_path, FROM, TO,
                              monzo_clients=lambda label, s: FakeMonzo({MONZO_ACC: [{"transactions": [mtx("tx_1")]}]}))
    assert flow.store_from_disk(MonzoFirefly(pot=False), sessions, fetched.pages) == flow.StoreCounts(created=1)


def test_compare_keys_reads_monzo_pages_through_the_adapter(tmp_path):
    sessions = {"monzo": monzo_session()}
    fetched = flow.save_pages(FakeBank({}), sessions, tmp_path, FROM, TO,
                              monzo_clients=lambda label, s: FakeMonzo({MONZO_ACC: monzo_pages()}))
    days = compare.keys_by_day(sessions, MONZO_ACC, fetched.pages[MONZO_ACC])
    assert {d: len(k) for d, k in days.items()} == {date(2026, 1, 2): 2}


# Pass-through transfers: the other side's amount comes from the leg in Firefly.

PASS_IBAN = "XX00SYNTHETIC0000000009"
PASS_UK_NUMBER = "23147012345678"


class PassThroughFirefly(FakeFirefly):
    """A GBP Monzo account (7), a EUR pass-through account (9) and the imported EUR account A (1); stores are queryable."""

    def __init__(self, legs: list[dict] = (), linked: set[str] = frozenset(), existing: set[str] = frozenset()):
        super().__init__(existing)
        self.splits = list(legs)
        self.links: list[tuple[str, str]] = []
        self.linked = set(linked)
        self.next_journal = 1000

    def accounts(self, account_type="asset"):
        return [
            {"id": "1", "attributes": {"iban": IBAN_A, "name": "Synthetic A", "currency_code": "EUR"}},
            {"id": "7", "attributes": {"iban": None, "account_number": MONZO_NUMBER, "name": "Synthetic current", "currency_code": "GBP"}},
            {"id": "9", "attributes": {"iban": PASS_IBAN, "account_number": PASS_UK_NUMBER, "name": "Synthetic pass-through",
                                       "currency_code": "EUR"}},
        ]

    def find_by_external_id(self, external_id):
        found = [s for s in self.splits if s.get("external_id") == external_id]
        return {"id": "old", "attributes": {"transactions": found}} if external_id in self.existing and found else None

    def store_if_absent(self, payload):
        split = payload["transactions"][0]
        if split["external_id"] in self.existing:
            found = next(s for s in self.splits if s.get("external_id") == split["external_id"])
            return StoreResult(created=False, group_id="old", journal_id=found["transaction_journal_id"])
        self.existing.add(split["external_id"])
        self.stored.append(payload)
        self.next_journal += 1
        self.splits.append({**split, "transaction_journal_id": str(self.next_journal)})
        return StoreResult(created=True, group_id="new", journal_id=str(self.next_journal))

    def account_transactions(self, account_id, start, end, type="transfer"):
        return [s for s in self.splits
                if s.get("type") == type and account_id in (str(s.get("source_id")), str(s.get("destination_id")))
                and start.isoformat() <= s["date"][:10] <= end.isoformat()]

    def links_of(self, journal_id):
        if journal_id in self.linked or any(journal_id in pair for pair in self.links):
            return [{"id": "link"}]
        return []

    def link(self, inward_id, outward_id, link_type="Related", notes=None):
        self.links.append((inward_id, outward_id))
        return {"id": str(len(self.links))}


def leg(journal_id: str, day: str = "2026-01-03", amount: str = "115.200000000000", into: bool = False, **over) -> dict:
    split = {"transaction_journal_id": journal_id, "type": "transfer", "date": f"{day}T00:00:00+00:00",
             "source_id": "1" if into else "9", "destination_id": "9" if into else "1",
             "amount": amount, "currency_code": "EUR", "foreign_amount": None, "foreign_currency_code": None}
    split.update(over)
    return split


def top_up(id: str = "tx_w1", day: str = "2026-01-02", amount: int = -10000, settled: str | None = None) -> dict:
    return mtx(id, amount=amount, merchant=None, description="TOP UP", scheme="payport_faster_payments",
               created=f"{day}T10:00:00Z", settled=f"{settled or day}T10:00:00Z",
               counterparty={"name": "Synthetic pass-through", "sort_code": "23-14-70", "account_number": "12345678"})


def payout(ref: str, day: str = "2026-01-03", amount: str = "115.20") -> dict:
    return {**tx(ref), "booking_date": day, "credit_debit_indicator": "CRDT",
            "transaction_amount": {"amount": amount, "currency": "EUR"},
            "debtor": {"name": "Synthetic pass-through"}, "debtor_account": {"iban": PASS_IBAN}}


def pass_through_sessions() -> dict:
    return {"bank-one": {**SESSIONS["bank-one"], "accounts": [SESSIONS["bank-one"]["accounts"][0]]},
            "monzo": monzo_session(pots=[])}


def fetch_both(tmp_path, eb_pages: dict, monzo_pages: list[dict]) -> dict:
    return flow.save_pages(FakeBank(eb_pages), pass_through_sessions(), tmp_path, FROM, TO,
                           monzo_clients=lambda label, s: FakeMonzo({MONZO_ACC: monzo_pages})).pages


def transfer_fields(payload: dict) -> tuple:
    s = payload["transactions"][0]
    return tuple(s.get(k) for k in ("type", "source_id", "destination_id", "amount", "currency_code", "foreign_amount", "foreign_currency_code"))


def test_a_pass_through_transfer_takes_its_amount_from_the_leg_in_firefly_and_links_it(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up()]}])
    firefly = PassThroughFirefly(legs=[leg("401")])
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1)
    [payload] = firefly.stored
    assert transfer_fields(payload) == ("transfer", "7", "9", "100.00", "GBP", "115.2", "EUR")
    assert firefly.links == [("1001", "401")]


def test_a_leg_stored_by_the_same_run_is_found(tmp_path):
    pages = fetch_both(tmp_path, {UID_A: [{"transactions": [payout("p1")]}]}, [{"transactions": [top_up()]}])
    firefly = PassThroughFirefly()
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=2)
    assert [transfer_fields(p) for p in firefly.stored] == [
        ("transfer", "9", "1", "115.20", "EUR", None, None),
        ("transfer", "7", "9", "100.00", "GBP", "115.2", "EUR"),
    ]
    assert firefly.links == [("1002", "1001")]


def test_no_leg_yet_leaves_the_transfer_for_a_later_run_while_the_window_is_open(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up(day="2026-01-07"), mtx("tx_g", created="2026-01-08T00:00:00Z", settled="2026-01-08T00:00:00Z")]}])
    firefly = PassThroughFirefly()
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1, deferred=1)
    assert [p["transactions"][0]["type"] for p in firefly.stored] == ["withdrawal"]
    assert firefly.links == []


def test_no_leg_within_the_window_is_unpaired_and_fails_after_the_rest(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up(day="2026-01-02"), mtx("tx_g", created="2026-01-12T00:00:00Z", settled="2026-01-12T00:00:00Z")]}])
    firefly = PassThroughFirefly(legs=[leg("401", day="2026-01-10")])  # too late: the window is 7 days
    with pytest.raises(mapping.UnpairedMoveError, match="1 unpaired booking"):
        flow.store_from_disk(firefly, pass_through_sessions(), pages)
    assert [p["transactions"][0]["type"] for p in firefly.stored] == ["withdrawal"]


def test_a_leg_dated_before_the_booking_is_found_as_the_settlement_day_follows_the_payment(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up(day="2026-01-02", settled="2026-01-03")]}])
    firefly = PassThroughFirefly(legs=[leg("401", day="2026-01-02"), leg("402", day="2025-12-26")])  # 402: 8 days before
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1)
    assert firefly.links == [("1001", "401")]
    assert flow.store_from_disk(PassThroughFirefly(legs=[leg("402", day="2025-12-27")]), pass_through_sessions(),
                                pages) == flow.StoreCounts(created=1)


def test_top_ups_take_legs_in_date_order_and_skip_linked_ones(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up("tx_w2", day="2026-01-04", amount=-20000), top_up("tx_w1", day="2026-01-02")]}])
    firefly = PassThroughFirefly(legs=[leg("401"), leg("402"), leg("403", day="2026-01-05", amount="230.400000000000")], linked={"401"})
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=2)
    assert [transfer_fields(p)[3:] for p in firefly.stored] == [("100.00", "GBP", "115.2", "EUR"), ("200.00", "GBP", "230.4", "EUR")]
    assert firefly.links == [("1001", "402"), ("1002", "403")]


def test_an_existing_pass_through_transfer_is_linked_when_it_has_no_link_yet(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up()]}])
    [key] = mapping.external_ids(("number", MONZO_NUMBER), monzo.adapt([top_up()], {}))
    stored = leg("900", day="2026-01-02", amount="100.00", source_id="7", destination_id="9", currency_code="GBP",
                 foreign_amount="115.2", foreign_currency_code="EUR", external_id=key)
    unlinked = PassThroughFirefly(legs=[leg("401"), stored], existing={key})
    assert flow.store_from_disk(unlinked, pass_through_sessions(), pages) == flow.StoreCounts(existing=1)
    assert unlinked.links == [("900", "401")]
    linked = PassThroughFirefly(legs=[leg("401"), stored], existing={key}, linked={"900"})
    assert flow.store_from_disk(linked, pass_through_sessions(), pages) == flow.StoreCounts(existing=1)
    assert linked.links == []


def test_a_pass_through_transfer_booked_by_hand_under_its_key_needs_no_leg(tmp_path):
    """A top-up the service sent straight back has no EUR leg; booked by hand with the key, it counts as existing."""
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up(day="2025-12-20"), mtx("tx_g")]}])
    [key] = mapping.external_ids(("number", MONZO_NUMBER), monzo.adapt([top_up(day="2025-12-20")], {}))
    by_hand = leg("900", day="2025-12-20", amount="100.00", source_id="7", destination_id="9", currency_code="GBP",
                  foreign_amount="115.2", foreign_currency_code="EUR", external_id=key)
    firefly = PassThroughFirefly(legs=[by_hand], existing={key})
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1, existing=1)
    assert [p["transactions"][0]["type"] for p in firefly.stored] == ["withdrawal"]
    assert firefly.links == []
    refund = top_up(day="2025-12-22", amount=10000)
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [refund, mtx("tx_g")]}])
    [key] = mapping.external_ids(("number", MONZO_NUMBER), monzo.adapt([refund], {}))
    booked_in_eur = leg("901", day="2025-12-22", amount="115.2", source_id="9", destination_id="7", currency_code="EUR",
                        foreign_amount="100.00", foreign_currency_code="GBP", external_id=key)
    firefly = PassThroughFirefly(legs=[booked_in_eur], existing={key})
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1, existing=1)
    wrong = PassThroughFirefly(legs=[{**booked_in_eur, "foreign_amount": "99.00"}], existing={key})
    with pytest.raises(ExternalIdConflictError, match="with 99.00 GBP, the bank says 100.00"):
        flow.store_from_disk(wrong, pass_through_sessions(), pages)


def test_a_dry_run_maps_pass_through_transfers_but_stores_and_links_nothing(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up()]}])
    firefly = PassThroughFirefly(legs=[leg("401")])
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages, write=False) == flow.StoreCounts(not_written=1)
    assert firefly.stored == [] and firefly.links == []


def test_a_credit_from_a_pass_through_account_is_booked_in_its_currency_from_the_leg_into_it(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up(amount=10000)]}])
    firefly = PassThroughFirefly(legs=[leg("401", day="2026-01-02", into=True), leg("402", day="2026-01-03")])
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1)
    [payload] = firefly.stored
    assert transfer_fields(payload) == ("transfer", "9", "7", "115.2", "EUR", "100.00", "GBP")
    assert firefly.links == [("1001", "401")]


def test_a_leg_booked_in_a_third_currency_gives_its_foreign_amount(tmp_path):
    pages = fetch_both(tmp_path, empty(UID_A), [{"transactions": [top_up()]}])
    odd_leg = leg("401", amount="99.000000000000", currency_code="USD", foreign_amount="115.200000000000", foreign_currency_code="EUR")
    firefly = PassThroughFirefly(legs=[odd_leg])
    assert flow.store_from_disk(firefly, pass_through_sessions(), pages) == flow.StoreCounts(created=1)
    assert transfer_fields(firefly.stored[0])[5:] == ("115.2", "EUR")
