"""Mapping tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import re

import pytest

from transaction_pipeline.mapping import (
    AccountBook,
    CounterpartyRule,
    MappingError,
    OwnAccount,
    external_ids,
    load_rules,
    map_transaction,
)

CHECKING = OwnAccount("1", "Synthetic checking", "XX00SYNTHETIC0000000001", imported=True)
SAVINGS = OwnAccount("2", "Synthetic savings", "XX00SYNTHETIC0000000002", imported=True)
ELSEWHERE = OwnAccount("3", "Synthetic account elsewhere", "XX00SYNTHETIC0000000003", imported=False)
BOOK = AccountBook([CHECKING, SAVINGS, ELSEWHERE])

GROCER_IBAN = "XX00SYNTHETIC0000000099"
ACCOUNT_KEY = ("iban", CHECKING.iban)
KEY = "tp1:" + "0" * 64


def _tx(**overrides) -> dict:
    tx = {
        "entry_reference": "ref-1",
        "transaction_amount": {"currency": "EUR", "amount": "12.34"},
        "credit_debit_indicator": "DBIT",
        "status": "BOOK",
        "booking_date": "2026-01-02",
        "value_date": "2026-01-03",
        "creditor": {"name": "Example Grocer"},
        "creditor_account": {"iban": GROCER_IBAN},
        "remittance_information": ["synthetic", "purchase  42"],
    }
    tx.update(overrides)
    return tx


def _split(payload: dict) -> dict:
    assert len(payload["transactions"]) == 1
    return payload["transactions"][0]


def test_debit_to_a_stranger_is_a_withdrawal():
    payload = map_transaction(_tx(), CHECKING, BOOK, KEY)

    assert payload["apply_rules"] is True
    assert _split(payload) == {
        "type": "withdrawal",
        "date": "2026-01-02",
        "amount": "12.34",
        "currency_code": "EUR",
        "description": "synthetic purchase 42",
        "external_id": KEY,
        "internal_reference": "ref-1",
        "source_id": "1",
        "destination_name": "Example Grocer",
        "destination_iban": GROCER_IBAN,
    }


def test_credit_from_a_stranger_is_a_deposit():
    raw = _tx(
        credit_debit_indicator="CRDT",
        creditor=None,
        creditor_account=None,
        debtor={"name": "Example Employer"},
        debtor_account={"iban": "xx00 synthetic 0000 0000 98"},
    )

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert split["type"] == "deposit"
    assert split["destination_id"] == "1"
    assert split["source_name"] == "Example Employer"
    assert split["source_iban"] == "XX00SYNTHETIC0000000098"
    assert "source_id" not in split


def test_debit_to_an_imported_own_account_is_one_transfer():
    raw = _tx(creditor={"name": "Me"}, creditor_account={"iban": SAVINGS.iban})

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert split["type"] == "transfer"
    assert (split["source_id"], split["destination_id"]) == ("1", "2")
    assert "destination_name" not in split


def test_credit_side_of_a_transfer_between_imported_accounts_is_skipped():
    raw = _tx(credit_debit_indicator="CRDT", debtor={"name": "Me"}, debtor_account={"iban": CHECKING.iban})

    assert map_transaction(raw, SAVINGS, BOOK, KEY) is None


def test_credit_from_an_own_account_that_is_not_imported_is_the_transfer():
    raw = _tx(credit_debit_indicator="CRDT", debtor={"name": "Me"}, debtor_account={"iban": ELSEWHERE.iban})

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert split["type"] == "transfer"
    assert (split["source_id"], split["destination_id"]) == ("3", "1")


def test_debit_to_an_own_account_that_is_not_imported_is_a_transfer():
    raw = _tx(creditor_account={"iban": ELSEWHERE.iban})

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert split["type"] == "transfer"
    assert (split["source_id"], split["destination_id"]) == ("1", "3")


def test_foreign_currency_goes_to_foreign_amount():
    raw = _tx(
        exchange_rate={
            "unit_currency": "USD",
            "exchange_rate": "1.0800",
            "instructed_amount": {"currency": "USD", "amount": "13.33"},
        }
    )

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert (split["amount"], split["currency_code"]) == ("12.34", "EUR")
    assert (split["foreign_amount"], split["foreign_currency_code"]) == ("13.33", "USD")


def test_instructed_amount_in_the_account_currency_is_not_foreign():
    raw = _tx(exchange_rate={"instructed_amount": {"currency": "EUR", "amount": "12.34"}})

    assert "foreign_amount" not in _split(map_transaction(raw, CHECKING, BOOK, KEY))


def test_signed_amount_without_indicator_is_a_debit():
    raw = _tx(credit_debit_indicator=None, transaction_amount={"currency": "EUR", "amount": "-5.00"})

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert (split["type"], split["amount"]) == ("withdrawal", "5.00")


def test_unsigned_amount_without_indicator_fails():
    with pytest.raises(MappingError, match="credit_debit_indicator"):
        map_transaction(_tx(credit_debit_indicator=None), CHECKING, BOOK, KEY)


def test_entry_reference_goes_to_internal_reference_not_the_key():
    split = _split(map_transaction(_tx(), CHECKING, BOOK, KEY))

    assert (split["external_id"], split["internal_reference"]) == (KEY, "ref-1")


def test_no_entry_reference_means_no_internal_reference():
    assert "internal_reference" not in _split(map_transaction(_tx(entry_reference=None), CHECKING, BOOK, KEY))


def test_booking_without_booking_date_fails():
    with pytest.raises(MappingError, match="booking_date"):
        map_transaction(_tx(booking_date=None), CHECKING, BOOK, KEY)


def test_content_key_is_versioned_and_a_digest():
    [key] = external_ids(ACCOUNT_KEY, [_tx()])

    assert re.fullmatch(r"tp1:[0-9a-f]{64}", key)


def test_identical_same_day_bookings_get_distinct_keys_by_ordinal():
    keys = external_ids(ACCOUNT_KEY, [_tx(), _tx(), _tx(booking_date="2026-01-05"), _tx()])

    assert len(set(keys)) == 4
    # The third identical one on 2026-01-02 is the third ordinal, wherever the other day sits.
    assert keys[3] == external_ids(ACCOUNT_KEY, [_tx(), _tx(), _tx()])[2]


def test_ordinal_counts_on_from_earlier_pages():
    keys = external_ids(ACCOUNT_KEY, [_tx(), _tx()])

    assert keys[1] != external_ids(ACCOUNT_KEY, [_tx()])[0]


def test_a_reordered_page_gives_the_same_set_of_keys():
    page = [_tx(), _tx(transaction_amount={"currency": "EUR", "amount": "5.00"}), _tx(), _tx(booking_date="2026-01-05")]

    assert set(external_ids(ACCOUNT_KEY, page)) == set(external_ids(ACCOUNT_KEY, page[::-1]))


def test_bank_references_and_text_do_not_change_the_key():
    with_ref = _tx(entry_reference="ref-1", transaction_id="tid-1")
    without = _tx(entry_reference=None, remittance_information=["reworded"], creditor={"name": "Grocer"},
                  value_date="2026-01-09", bank_transaction_code={"code": "XX"})

    assert external_ids(ACCOUNT_KEY, [with_ref]) == external_ids(ACCOUNT_KEY, [without])


def test_amount_formatting_and_currency_case_do_not_change_the_key():
    assert external_ids(ACCOUNT_KEY, [_tx()]) == external_ids(
        ACCOUNT_KEY, [_tx(transaction_amount={"currency": "eur", "amount": "-12.340"})])


def test_a_missing_indicator_keys_on_the_amount_sign():
    signed = _tx(credit_debit_indicator=None, transaction_amount={"currency": "EUR", "amount": "-12.34"})

    assert external_ids(ACCOUNT_KEY, [signed]) == external_ids(ACCOUNT_KEY, [_tx()])


@pytest.mark.parametrize(
    "changed",
    [
        {"booking_date": "2026-01-03"},
        {"transaction_amount": {"currency": "EUR", "amount": "12.35"}},
        {"transaction_amount": {"currency": "GBP", "amount": "12.34"}},
        {"credit_debit_indicator": "CRDT"},
    ],
)
def test_every_key_field_changes_the_key(changed):
    assert external_ids(ACCOUNT_KEY, [_tx()]) != external_ids(ACCOUNT_KEY, [_tx(**changed)])


def test_the_account_key_changes_the_key():
    assert external_ids(ACCOUNT_KEY, [_tx()]) != external_ids(("iban", SAVINGS.iban), [_tx()])


@pytest.mark.parametrize(
    ("changed", "message"),
    [
        ({"booking_date": None}, "booking_date"),
        ({"booking_date": "02.01.2026"}, "booking_date"),
        ({"transaction_amount": None}, "amount"),
        ({"transaction_amount": {"currency": "EUR", "amount": "n/a"}}, "amount"),
        ({"transaction_amount": {"currency": "", "amount": "1.00"}}, "currency"),
        ({"credit_debit_indicator": None}, "credit_debit_indicator"),
    ],
)
def test_a_booking_that_cannot_be_keyed_fails(changed, message):
    with pytest.raises(MappingError, match=message):
        external_ids(ACCOUNT_KEY, [_tx(), _tx(**changed)])


def test_zero_amount_is_skipped():
    assert map_transaction(_tx(transaction_amount={"currency": "EUR", "amount": "0.00"}), CHECKING, BOOK, KEY) is None


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"remittance_information": None}, "Example Grocer"),
        (
            {"remittance_information": [], "creditor": None, "bank_transaction_code": {"description": "Card fee"}},
            "Card fee",
        ),
        ({"remittance_information": [" "], "creditor": None}, "(no description)"),
    ],
)
def test_description_falls_back(overrides, expected):
    assert _split(map_transaction(_tx(**overrides), CHECKING, BOOK, KEY))["description"] == expected


def test_description_is_stable():
    assert map_transaction(_tx(), CHECKING, BOOK, KEY) == map_transaction(_tx(), CHECKING, BOOK, KEY)


def test_counterparty_without_iban_uses_the_other_identifier():
    raw = _tx(creditor={"name": "Example Shop"}, creditor_account={"other": {"identification": "12345678"}})

    split = _split(map_transaction(raw, CHECKING, BOOK, KEY))

    assert split["destination_number"] == "12345678"
    assert "destination_iban" not in split


def test_account_book_from_firefly_marks_imported_accounts():
    accounts = [
        {"id": "1", "attributes": {"name": "Synthetic checking", "iban": "XX00 SYNTHETIC 0000 0000 01"}},
        {"id": "2", "attributes": {"name": "Synthetic savings", "iban": SAVINGS.iban}},
        {"id": "4", "attributes": {"name": "Cash", "iban": None}},
    ]

    book = AccountBook.from_firefly(accounts, imported_ibans=[CHECKING.iban])

    assert book.get("xx00synthetic0000000001") == OwnAccount("1", "Synthetic checking", CHECKING.iban, True)
    assert book.get(SAVINGS.iban).imported is False


def test_account_book_fails_when_an_imported_account_is_not_in_firefly():
    with pytest.raises(MappingError, match="no Firefly account"):
        AccountBook.from_firefly([], imported_ibans=[CHECKING.iban])


SPACE_HASH = "synthetic-identification-hash-" + "0" * 100


def test_account_book_matches_an_account_without_iban_by_account_number():
    accounts = [
        {"id": "1", "attributes": {"name": "Synthetic checking", "iban": CHECKING.iban}},
        {"id": "6", "attributes": {"name": "Synthetic space", "iban": None, "account_number": SPACE_HASH}},
        {"id": "7", "attributes": {"name": "Synthetic other space", "iban": None, "account_number": "other"}},
    ]

    book = AccountBook.from_firefly(accounts, imported_ibans=[CHECKING.iban], imported_numbers=[SPACE_HASH])

    assert book.get_by_number(SPACE_HASH) == OwnAccount("6", "Synthetic space", None, True, SPACE_HASH)
    assert book.get_by_number("other").imported is False
    assert book.get_by_number(None) is None


def test_account_book_fails_when_an_imported_account_number_is_not_in_firefly():
    accounts = [{"id": "6", "attributes": {"name": "Synthetic space", "iban": None, "account_number": "other"}}]
    with pytest.raises(MappingError, match="identification_hash as its account number"):
        AccountBook.from_firefly(accounts, imported_ibans=[], imported_numbers=[SPACE_HASH])


POT = OwnAccount("5", "Synthetic pot", None, imported=False)
POT_RULE = CounterpartyRule("Synthetic pot", name=re.compile("^synthetic pot$", re.IGNORECASE))
RULE_BOOK = AccountBook([CHECKING, SAVINGS, POT], [(POT_RULE, POT)])


def test_move_into_a_sub_account_matched_by_rule_is_a_transfer():
    # Some banks show the account's own IBAN on moves to its sub-accounts.
    raw = _tx(creditor={"name": "Synthetic Pot"}, creditor_account={"iban": CHECKING.iban})

    split = _split(map_transaction(raw, CHECKING, RULE_BOOK, KEY))

    assert split["type"] == "transfer"
    assert (split["source_id"], split["destination_id"]) == ("1", "5")


def test_move_back_from_a_sub_account_is_a_transfer_into_the_account():
    raw = _tx(credit_debit_indicator="CRDT", debtor={"name": "Synthetic Pot"}, debtor_account=None)

    split = _split(map_transaction(raw, CHECKING, RULE_BOOK, KEY))

    assert split["type"] == "transfer"
    assert (split["source_id"], split["destination_id"]) == ("5", "1")


def test_rule_needs_every_pattern_it_gives():
    rule = CounterpartyRule("Synthetic pot", name=re.compile("pot"), remittance=re.compile("^move$"))
    book = AccountBook([CHECKING, POT], [(rule, POT)])
    raw = _tx(creditor={"name": "pot"}, remittance_information=["other text"])

    assert _split(map_transaction(raw, CHECKING, book, KEY))["type"] == "withdrawal"


def test_iban_match_wins_over_a_rule():
    rule = CounterpartyRule("Synthetic pot", name=re.compile("."))
    book = AccountBook([CHECKING, SAVINGS, POT], [(rule, POT)])
    raw = _tx(creditor={"name": "Me"}, creditor_account={"iban": SAVINGS.iban})

    assert _split(map_transaction(raw, CHECKING, book, KEY))["destination_id"] == "2"


def test_rules_resolve_firefly_accounts_by_name():
    accounts = [
        {"id": "1", "attributes": {"name": "Synthetic checking", "iban": CHECKING.iban}},
        {"id": "5", "attributes": {"name": "Synthetic pot", "iban": None}},
    ]

    book = AccountBook.from_firefly(accounts, [CHECKING.iban], rules=[POT_RULE])

    assert book.own_counterparty(CHECKING, "synthetic pot", None, "") == POT


def test_rule_naming_a_missing_firefly_account_fails():
    with pytest.raises(MappingError, match="doesn't exist"):
        AccountBook.from_firefly([], [], rules=[POT_RULE])


def test_load_rules_reads_toml(tmp_path):
    path = tmp_path / "rules.toml"
    path.write_text(
        '[[own_counterparty]]\naccount = "Synthetic pot"\nname = "^Synthetic Pot$"\n'
        '[[own_counterparty]]\naccount = "Synthetic pot"\nremittance = "to pot"\n'
    )

    rules = load_rules(path)

    assert [r.account for r in rules] == ["Synthetic pot", "Synthetic pot"]
    assert rules[0].matches("synthetic pot", "")
    assert rules[1].matches(None, "move TO POT 3")


def test_load_rules_without_a_file_is_empty(tmp_path, monkeypatch):
    monkeypatch.setenv("MAPPING_CONFIG", str(tmp_path / "absent.toml"))

    assert load_rules(tmp_path / "absent.toml") == []
    assert load_rules() == []


def test_load_rules_reads_mapping_config(tmp_path, monkeypatch):
    path = tmp_path / "mapping.toml"
    path.write_text('[[own_counterparty]]\naccount = "Synthetic pot"\nname = "pot"\n')
    monkeypatch.setenv("MAPPING_CONFIG", str(path))

    assert [r.account for r in load_rules()] == ["Synthetic pot"]


@pytest.mark.parametrize(
    "body",
    [
        '[[own_counterparty]]\naccount = "Synthetic pot"\n',
        '[[own_counterparty]]\nname = "pot"\n',
        '[[own_counterparty]]\naccount = "Synthetic pot"\nname = "pot"\nbic = "XX"\n',
    ],
)
def test_load_rules_rejects_incomplete_or_unknown_entries(tmp_path, body):
    path = tmp_path / "rules.toml"
    path.write_text(body)

    with pytest.raises(MappingError, match="own_counterparty"):
        load_rules(path)


def test_rule_ignores_a_stranger_who_writes_matching_text():
    raw = _tx(credit_debit_indicator="CRDT", debtor={"name": "Synthetic Pot"}, debtor_account={"iban": GROCER_IBAN})

    assert _split(map_transaction(raw, CHECKING, RULE_BOOK, KEY))["type"] == "deposit"


def test_rule_with_iban_needs_that_iban():
    hub_iban = "XX00SYNTHETIC0000000077"
    rule = CounterpartyRule("Synthetic pot", remittance=re.compile("relay"), iban=hub_iban)
    book = AccountBook([CHECKING, POT], [(rule, POT)])

    hit = _tx(creditor={"name": "Relay"}, creditor_account={"iban": hub_iban}, remittance_information=["relay 1"])
    miss = _tx(creditor={"name": "Relay"}, creditor_account=None, remittance_information=["relay 1"])

    assert _split(map_transaction(hit, CHECKING, book, KEY))["type"] == "transfer"
    assert _split(map_transaction(miss, CHECKING, book, KEY))["type"] == "withdrawal"
