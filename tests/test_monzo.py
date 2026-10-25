"""Monzo client, session and adapter tests. All fixtures are synthetic, written from the Monzo API docs."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from transaction_pipeline import mapping, monzo
from transaction_pipeline.monzo import AccessGoneError, MonzoClient, MonzoError, Token

API = monzo.API_URL
TOKEN_URL = f"{API}/oauth2/token"
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
ACCOUNT = "acc_0000synthetic00000000001"
POT = "pot_0000synthetic00000000001"
GROCER = {"id": "merch_0000synthetic0000001", "name": "Synthetic Grocer", "category": "groceries"}


def token(expires_in: int = 21600) -> Token:
    return Token("access-1", "refresh-1", datetime.now(UTC) + timedelta(seconds=expires_in))


def token_body(n: int = 2) -> dict:
    return {"access_token": f"access-{n}", "refresh_token": f"refresh-{n}", "expires_in": 21600,
            "token_type": "Bearer", "user_id": "user_synthetic", "client_id": "client-1"}


@pytest.fixture
def client():
    with MonzoClient("client-1", "secret-1", token()) as c:
        yield c


def tx(id: str = "tx_0000synthetic00000000001", amount: int = -510, settled: str = "2026-01-02T12:20:18Z", **over) -> dict:
    base = {
        "id": id,
        "amount": amount,
        "currency": "GBP",
        "created": "2026-01-02T12:20:18Z",
        "settled": settled,
        "description": "SYNTHETIC GROCER LONDON GBR",
        "merchant": dict(GROCER),
        "metadata": {},
        "notes": "",
        "is_load": False,
        "local_amount": amount,
        "local_currency": "GBP",
        "category": "groceries",
    }
    base.update(over)
    return base


# Auth and token


def test_auth_url_carries_the_client_redirect_and_state():
    url = httpx.URL(monzo.auth_url("client-1", "https://redirect.example.test/cb", "state-1"))
    assert url.host == "auth.monzo.com"
    assert dict(url.params) == {"client_id": "client-1", "redirect_uri": "https://redirect.example.test/cb",
                                "response_type": "code", "state": "state-1"}


@respx.mock
def test_exchange_code_posts_the_confidential_client_and_keeps_the_token():
    route = respx.post(TOKEN_URL).respond(json=token_body(1))
    seen = []
    with MonzoClient("client-1", "secret-1", on_token=seen.append) as c:
        got = c.exchange_code("code-1", "https://redirect.example.test/cb")
        assert c.token == got
    form = dict(httpx.QueryParams(route.calls[0].request.content.decode()))
    assert form == {"grant_type": "authorization_code", "client_id": "client-1", "client_secret": "secret-1",
                    "redirect_uri": "https://redirect.example.test/cb", "code": "code-1"}
    assert (got.access_token, got.refresh_token) == ("access-1", "refresh-1")
    assert seen == [got]
    assert not got.expired()


def test_a_token_without_a_refresh_token_is_refused():
    with pytest.raises(MonzoError, match="confidential"):
        Token.from_response({"access_token": "a", "expires_in": 100})


def test_a_token_round_trips_through_json_and_expires_with_a_margin():
    t = Token("a", "r", NOW + timedelta(seconds=120))
    assert Token.from_json(t.to_json()) == t
    assert not t.expired(NOW)
    assert t.expired(NOW + timedelta(seconds=61))


@respx.mock
def test_requests_send_the_bearer_token(client):
    route = respx.get(f"{API}/ping/whoami").respond(json={"authenticated": True, "user_id": "user_synthetic"})
    assert client.whoami()["authenticated"] is True
    assert route.calls[0].request.headers["Authorization"] == "Bearer access-1"


@respx.mock
def test_an_expired_token_is_refreshed_before_the_request():
    refresh = respx.post(TOKEN_URL).respond(json=token_body(2))
    accounts = respx.get(f"{API}/accounts").respond(json={"accounts": []})
    seen = []
    with MonzoClient("client-1", "secret-1", token(expires_in=10), on_token=seen.append) as c:
        assert c.accounts() == []
    form = dict(httpx.QueryParams(refresh.calls[0].request.content.decode()))
    assert form == {"grant_type": "refresh_token", "client_id": "client-1", "client_secret": "secret-1",
                    "refresh_token": "refresh-1"}
    assert accounts.calls[0].request.headers["Authorization"] == "Bearer access-2"
    assert [t.refresh_token for t in seen] == ["refresh-2"]


@respx.mock
def test_a_401_refreshes_once_and_retries(client):
    respx.post(TOKEN_URL).respond(json=token_body(2))
    accounts = respx.get(f"{API}/accounts").mock(side_effect=[
        httpx.Response(401, json={"code": "unauthorized.bad_access_token.evicted", "message": "evicted"}),
        httpx.Response(200, json={"accounts": [{"id": ACCOUNT}]}),
    ])
    assert client.accounts() == [{"id": ACCOUNT}]
    assert [c.request.headers["Authorization"] for c in accounts.calls] == ["Bearer access-1", "Bearer access-2"]


@respx.mock
def test_a_failed_refresh_is_access_gone(client):
    respx.get(f"{API}/accounts").respond(401, json={"code": "unauthorized.bad_access_token", "message": "bad"})
    respx.post(TOKEN_URL).respond(400, json={"code": "bad_request.invalid_grant", "message": "revoked"})
    with pytest.raises(AccessGoneError, match="invalid_grant"):
        client.accounts()


@respx.mock
def test_a_403_is_access_gone_with_the_api_code(client):
    respx.get(f"{API}/accounts").respond(403, json={"code": "forbidden.insufficient_permissions", "message": "no"})
    with pytest.raises(AccessGoneError) as exc:
        client.accounts()
    assert (exc.value.status_code, exc.value.code) == (403, "forbidden.insufficient_permissions")


@respx.mock
def test_other_errors_keep_the_status_and_code(client):
    respx.get(f"{API}/pots").respond(500, json={"code": "internal_service", "message": "oops"})
    with pytest.raises(MonzoError, match="500 internal_service") as exc:
        client.pots(ACCOUNT)
    assert not isinstance(exc.value, AccessGoneError)


def test_a_client_without_a_token_is_not_authorised():
    with MonzoClient("client-1", "secret-1") as c, pytest.raises(AccessGoneError, match="not authorised"):
        c.accounts()


@respx.mock
def test_pots_are_listed_for_an_account(client):
    route = respx.get(f"{API}/pots").respond(json={"pots": [{"id": POT, "name": "Synthetic pot", "deleted": False}]})
    assert client.pots(ACCOUNT)[0]["id"] == POT
    assert route.calls[0].request.url.params["current_account_id"] == ACCOUNT


# Transaction pages


def ids(n: int, start: int = 1) -> list[dict]:
    return [{"id": f"tx_{i:026d}"} for i in range(start, start + n)]


@respx.mock
def test_transaction_pages_follow_the_last_id_and_stop_at_a_short_page(client):
    route = respx.get(f"{API}/transactions").mock(side_effect=[
        httpx.Response(200, json={"transactions": ids(100)}),
        httpx.Response(200, json={"transactions": ids(1, 101)}),
    ])
    pages = list(client.transaction_pages(ACCOUNT, datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 9, tzinfo=UTC)))
    assert [len(p["transactions"]) for p in pages] == [100, 1]
    first = route.calls[0].request.url.params
    assert dict(first) == {"account_id": ACCOUNT, "since": "2026-01-01T00:00:00Z", "before": "2026-01-09T00:00:00Z",
                           "limit": "100", "expand[]": "merchant"}
    second = route.calls[1].request.url.params
    assert second["since"] == ids(100)[-1]["id"]
    assert second["before"] == "2026-01-09T00:00:00Z"


@respx.mock
def test_a_full_last_page_is_followed_by_no_empty_page(client):
    respx.get(f"{API}/transactions").mock(side_effect=[
        httpx.Response(200, json={"transactions": ids(100)}),
        httpx.Response(200, json={"transactions": []}),
    ])
    pages = list(client.transaction_pages(ACCOUNT, NOW, NOW))
    assert [len(p["transactions"]) for p in pages] == [100]


@respx.mock
def test_an_empty_first_page_is_still_a_page(client):
    respx.get(f"{API}/transactions").respond(json={"transactions": []})
    assert list(client.transaction_pages(ACCOUNT, NOW, NOW)) == [{"transactions": []}]


@respx.mock
def test_a_page_that_repeats_the_last_id_is_an_error(client):
    respx.get(f"{API}/transactions").respond(json={"transactions": ids(100)})
    with pytest.raises(MonzoError, match="repeated"):
        list(client.transaction_pages(ACCOUNT, NOW, NOW))


# Session


OPEN_ACCOUNT = {"id": ACCOUNT, "closed": False, "type": "uk_retail", "sort_code": "04-00-04", "account_number": "12345678",
                "currency": "GBP", "created": "2020-01-01T00:00:00Z", "description": "user_synthetic"}
CLOSED_ACCOUNT = {"id": "acc_0000synthetic00000000002", "closed": True, "type": "uk_prepaid"}
FLEX_ACCOUNT = {"id": "acc_0000synthetic00000000003", "closed": False, "type": "uk_monzo_flex", "currency": "GBP"}


def test_session_has_the_provider_token_open_accounts_pots_and_the_reapproval_deadline():
    t = Token("access-1", "refresh-1", NOW + timedelta(hours=6))
    session = monzo.session(t, [OPEN_ACCOUNT, CLOSED_ACCOUNT, FLEX_ACCOUNT], [{"id": POT, "name": "Synthetic pot"}], NOW)
    assert session == {
        "provider": "monzo",
        "token": t.to_json(),
        "authorised_at": NOW.isoformat(),
        "access": {"valid_until": (NOW + timedelta(days=90) - timedelta(minutes=5)).isoformat()},
        "accounts": [{"uid": ACCOUNT, "number": "04000412345678", "currency": "GBP", "type": "uk_retail",
                      "created": "2020-01-01T00:00:00Z"}],
        "pots": [{"id": POT, "name": "Synthetic pot", "deleted": False}],
    }


def test_an_account_without_sort_code_and_number_is_refused():
    with pytest.raises(MonzoError, match="sort code"):
        monzo.account_entry({"id": ACCOUNT, "sort_code": None, "account_number": ""})


def test_open_accounts_without_a_number_are_unnumbered_and_closed_ones_are_not():
    assert monzo.has_number(OPEN_ACCOUNT) and not monzo.has_number(FLEX_ACCOUNT)
    assert monzo.unnumbered([OPEN_ACCOUNT, CLOSED_ACCOUNT, FLEX_ACCOUNT]) == [FLEX_ACCOUNT]


# Adapter


@pytest.mark.parametrize("minor, currency, text", [
    (-510, "GBP", "5.10"), (5, "GBP", "0.05"), (120000, "gbp", "1200.00"), (1200, "JPY", "1200"), (1500, "KWD", "1.500"),
])
def test_amount_text_scales_minor_units_by_currency(minor, currency, text):
    assert monzo.amount_text(minor, currency) == text


def test_a_settled_card_payment_is_a_debit_named_after_the_merchant():
    assert monzo.to_raw(tx(), {}) == {
        "entry_reference": "tx_0000synthetic00000000001",
        "status": "BOOK",
        "booking_date": "2026-01-02",
        "value_date": "2026-01-02",
        "transaction_amount": {"amount": "5.10", "currency": "GBP"},
        "credit_debit_indicator": "DBIT",
        "creditor": {"name": "Synthetic Grocer"},
    }


@pytest.mark.parametrize("over", [{"settled": ""}, {"settled": None}, {"decline_reason": "INSUFFICIENT_FUNDS"}])
def test_pending_and_declined_transactions_are_left_out(over):
    assert monzo.to_raw(tx(**over), {}) is None


def test_the_settlement_date_is_the_booking_date_and_created_the_value_date():
    raw = monzo.to_raw(tx(created="2026-01-01T23:50:00Z", settled="2026-01-03T00:00:00Z"), {})
    assert (raw["booking_date"], raw["value_date"]) == ("2026-01-03", "2026-01-01")


def test_a_bank_transfer_names_the_counterparty_its_account_and_the_reference():
    raw = monzo.to_raw(tx(amount=-10000, merchant=None, description="RENT JAN", scheme="payport_faster_payments",
                          counterparty={"name": "Synthetic Landlord", "sort_code": "04-00-04", "account_number": "87654321",
                                        "user_id": "anonuser_x"}), {})
    assert raw["creditor"] == {"name": "Synthetic Landlord"}
    assert raw["creditor_account"] == {"other": {"identification": "04000487654321", "scheme_name": "UK_SORT_CODE_ACCOUNT_NUMBER"}}
    assert raw["remittance_information"] == ["RENT JAN"]
    assert raw["transaction_amount"] == {"amount": "100.00", "currency": "GBP"}


def test_an_incoming_payment_is_a_credit_from_the_debtor():
    raw = monzo.to_raw(tx(amount=2500, merchant=None, description="Gift", counterparty={"name": "Synthetic Friend"}), {})
    assert raw["credit_debit_indicator"] == "CRDT"
    assert raw["debtor"] == {"name": "Synthetic Friend"}
    assert "debtor_account" not in raw and "creditor" not in raw


def test_a_counterparty_with_a_preferred_name_only_is_named():
    raw = monzo.to_raw(tx(merchant=None, counterparty={"preferred_name": "Synthetic Friend"}), {})
    assert raw["creditor"] == {"name": "Synthetic Friend"}


def test_a_pot_move_is_named_after_the_pot_and_carries_its_id():
    raw = monzo.to_raw(tx(amount=-2000, merchant=None, scheme="uk_retail_pot", description=POT,
                          metadata={"pot_id": POT}), {POT: "Synthetic pot"})
    assert raw["creditor"] == {"name": "Synthetic pot"}
    assert raw["creditor_account"] == {"other": {"identification": POT, "scheme_name": "MONZO_POT"}}
    assert "remittance_information" not in raw


@pytest.mark.parametrize("over", [
    {"scheme": "uk_retail_pot", "description": POT},
    {"metadata": {"pot_id": POT}, "description": "something else"},
])
def test_a_pot_move_is_recognised_by_metadata_or_scheme(over):
    assert monzo.pot_of(tx(**over)) == POT


def test_a_payment_whose_reference_looks_like_a_pot_id_is_no_pot_move():
    raw = monzo.to_raw(tx(amount=2500, merchant=None, scheme="payport_faster_payments", description=POT,
                          counterparty={"name": "Synthetic Stranger", "sort_code": "040004", "account_number": "11111111"}), {POT: "Synthetic pot"})
    assert monzo.pot_of(tx(description=POT)) is None
    assert raw["debtor"] == {"name": "Synthetic Stranger"}
    assert raw["debtor_account"]["other"]["identification"] == "04000411111111"
    assert raw["remittance_information"] == [POT]


def test_a_pot_the_session_does_not_know_keeps_its_id_as_name():
    raw = monzo.to_raw(tx(amount=2000, merchant=None, scheme="uk_retail_pot", description=POT), {})
    assert raw["debtor"] == {"name": POT}


def test_a_note_is_the_remittance_text_whatever_the_payee():
    raw = monzo.to_raw(tx(notes="  shared dinner  "), {})
    assert raw["remittance_information"] == ["shared dinner"]


def test_a_card_payment_abroad_carries_the_local_amount_as_instructed():
    raw = monzo.to_raw(tx(amount=-1234, local_amount=-1450, local_currency="EUR"), {})
    assert raw["exchange_rate"] == {"instructed_amount": {"amount": "14.50", "currency": "EUR"}}


def test_a_payment_in_the_account_currency_has_no_instructed_amount():
    assert "exchange_rate" not in monzo.to_raw(tx(local_amount=-510, local_currency="gbp"), {})


def test_adapt_keeps_the_order_and_drops_what_is_not_mapped():
    raws = monzo.adapt([tx("tx_1"), tx("tx_2", settled=""), tx("tx_3", decline_reason="OTHER"), tx("tx_4")], {})
    assert [r["entry_reference"] for r in raws] == ["tx_1", "tx_4"]


NUMBER = "04000412345678"
CURRENT = mapping.OwnAccount("7", "Synthetic current", None, imported=True, number=NUMBER)
SAVINGS_POT = mapping.OwnAccount("8", "Synthetic pot", None, imported=False, number=POT)


def test_adapted_bookings_key_and_map_like_any_other_source():
    book = mapping.AccountBook([CURRENT, SAVINGS_POT])
    raws = monzo.adapt([
        tx("tx_1"),
        tx("tx_2", amount=-2000, merchant=None, scheme="uk_retail_pot", description=POT, metadata={"pot_id": POT}),
        tx("tx_3", amount=2000, merchant=None, scheme="uk_retail_pot", description=POT, metadata={"pot_id": POT}),
    ], {POT: "Synthetic pot"})
    keys = mapping.external_ids(("number", NUMBER), raws)
    assert len(set(keys)) == 3 and all(k.startswith("tp1:") for k in keys)
    splits = [mapping.map_transaction(raw, CURRENT, book, key)["transactions"][0] for raw, key in zip(raws, keys)]
    assert [s["type"] for s in splits] == ["withdrawal", "transfer", "transfer"]
    assert (splits[0]["source_id"], splits[0]["destination_name"], splits[0]["description"]) == ("7", "Synthetic Grocer", "Synthetic Grocer")
    assert (splits[1]["source_id"], splits[1]["destination_id"], splits[1]["amount"]) == ("7", "8", "20.00")
    assert (splits[2]["source_id"], splits[2]["destination_id"], splits[2]["internal_reference"]) == ("8", "7", "tx_3")
