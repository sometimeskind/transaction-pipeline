"""compare-keys tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import io
from datetime import date

import pytest

from transaction_pipeline import __main__ as cli
from transaction_pipeline import compare, flow

UID_A = "00000000-0000-4000-8000-00000000000a"
UID_B = "00000000-0000-4000-8000-00000000000b"
IBAN_A = "XX00SYNTHETIC0000000001"
SESSIONS = {
    "bank-one": {
        "accounts": [
            {"uid": UID_A, "account_id": {"iban": IBAN_A}},
            {"uid": UID_B, "account_id": {"iban": None}, "identification_hash": "synthetic-hash-b"},
        ],
        "access": {"valid_until": "2027-01-01T00:00:00+00:00"},
    },
}


def booking(day: int, amount: str = "12.34", ref: str | None = None) -> dict:
    return {
        "entry_reference": ref,
        "booking_date": date(2026, 1, day).isoformat(),
        "transaction_amount": {"amount": amount, "currency": "EUR"},
        "credit_debit_indicator": "DBIT",
        "remittance_information": ["synthetic purchase"],
    }


@pytest.fixture
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    flow.write_json(flow.session_path("bank-one"), SESSIONS["bank-one"])
    return tmp_path


def save(run: str, uid: str, *pages: list[dict]) -> None:
    for n, page in enumerate(pages, start=1):
        flow.write_json(flow.raw_root() / run / uid / f"page-{n}.json", {"transactions": page})


def run_cli(*args: str) -> tuple[int, str]:
    out = io.StringIO()
    code = cli.run_compare_keys(cli.parse_args(["compare-keys", *args]), out)
    return code, out.getvalue()


def test_the_same_bookings_in_two_runs_match_on_the_days_both_cover(state):
    # A backfill of days 1-6 and a later run of days 4-9: days 4 and 5 are compared, not 6,
    # since the backfill may have been fetched during day 6.
    save("backfill", UID_A, [booking(1), booking(4), booking(4), booking(5)], [booking(5), booking(6)])
    save("daily", UID_A, [booking(4, ref="r1"), booking(4, ref="r2")], [booking(5, ref="r3"), booking(5, ref="r4"),
                                                                         booking(6), booking(6), booking(9)])

    code, out = run_cli("backfill", "daily")

    assert code == 0
    assert out == "account 00000000: 4 keys in both, 0 only in A, 0 only in B (2 days compared)\n"


def test_a_key_in_only_one_run_is_counted_and_fails(state):
    save("run-a", UID_A, [booking(1), booking(2), booking(3)])
    save("run-b", UID_A, [booking(1), booking(2, amount="12.35"), booking(2, amount="12.35"), booking(3)])

    code, out = run_cli("run-a", "run-b")

    assert code == 1
    assert "1 keys in both, 1 only in A, 2 only in B (2 days compared)" in out


def test_output_carries_counts_and_a_uid_prefix_only(state):
    save("run-a", UID_A, [booking(1, ref="ref-secret"), booking(2)])
    save("run-b", UID_A, [booking(1), booking(2)])

    _, out = run_cli("run-a", "run-b")

    for content in (UID_A, IBAN_A, "12.34", "ref-secret", "synthetic purchase", "2026-01", "tp1:"):
        assert content not in out


def test_an_account_in_only_one_run_is_listed_but_not_compared(state):
    save("run-a", UID_A, [booking(1), booking(2)])
    save("run-a", UID_B, [booking(1)])
    save("run-b", UID_A, [booking(1), booking(2)])

    code, out = run_cli("run-a", "run-b")

    assert code == 0
    assert out.splitlines()[1] == "account 00000000: not in both runs, not compared"


def test_an_account_without_bookings_compares_no_days():
    assert compare.compare_account(UID_A, {}, {date(2026, 1, 1): {"tp1:x"}}).line().endswith("(0 days compared)")


def test_runs_that_do_not_overlap_compare_no_days(state):
    save("run-a", UID_A, [booking(1), booking(2)])
    save("run-b", UID_A, [booking(5), booking(6)])

    code, out = run_cli("run-a", "run-b")

    assert (code, out) == (0, "account 00000000: 0 keys in both, 0 only in A, 0 only in B (0 days compared)\n")


def test_a_run_id_must_be_a_plain_name(state):
    with pytest.raises(ValueError, match="not a run id"):
        run_cli("../sessions", "run-b")


def test_compare_keys_is_a_subcommand(state):
    save("run-a", UID_A, [booking(1), booking(2)])
    save("run-b", UID_A, [booking(1), booking(2)])

    assert cli.main(["compare-keys", "run-a", "run-b"]) == 0
