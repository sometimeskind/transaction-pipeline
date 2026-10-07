"""Enable Banking client tests. All fixtures are synthetic, written from the API schema."""

import json
from datetime import UTC, date, datetime

import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from transaction_pipeline.enable_banking import (
    BASE_URL,
    ConsentRedirectError,
    EnableBankingClient,
    EnableBankingError,
    RateLimitedError,
    SessionGoneError,
    code_from_redirect,
    load_private_key,
    make_jwt,
)

APP_ID = "00000000-0000-4000-8000-000000000001"
ACCOUNT_UID = "00000000-0000-4000-8000-0000000000a1"
TX_URL = f"{BASE_URL}/accounts/{ACCOUNT_UID}/transactions"


@pytest.fixture(scope="module")
def private_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def client(private_key):
    with EnableBankingClient(APP_ID, private_key) as c:
        yield c


def _transaction(ref: str, amount: str = "12.34", indicator: str = "DBIT") -> dict:
    return {
        "entry_reference": ref,
        "transaction_amount": {"currency": "EUR", "amount": amount},
        "credit_debit_indicator": indicator,
        "status": "BOOK",
        "booking_date": "2026-01-02",
        "value_date": "2026-01-02",
        "creditor": {"name": "Example Grocer"},
        "creditor_account": {"iban": "XX00SYNTHETIC0000000001"},
        "remittance_information": ["synthetic purchase"],
    }


def test_jwt_has_the_headers_and_claims_the_api_requires(private_key):
    token = make_jwt(APP_ID, private_key, now=1_700_000_000)

    header = jwt.get_unverified_header(token)
    assert header["alg"] == "RS256"
    assert header["kid"] == APP_ID
    assert header["typ"] == "JWT"
    claims = jwt.decode(
        token,
        private_key.public_key(),
        algorithms=["RS256"],
        audience="api.enablebanking.com",
        options={"verify_exp": False},
    )
    assert claims["iss"] == "enablebanking.com"
    assert claims["iat"] == 1_700_000_000
    assert 0 < claims["exp"] - claims["iat"] <= 86400


def test_load_private_key_reads_a_pem(tmp_path, private_key):
    pem = tmp_path / "app.pem"
    pem.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    assert load_private_key(pem).public_key().public_numbers() == private_key.public_key().public_numbers()


@respx.mock
def test_requests_carry_a_bearer_jwt(client, private_key):
    route = respx.get(f"{BASE_URL}/aspsps").respond(json={"aspsps": []})

    client.list_aspsps("XX")

    auth = route.calls.last.request.headers["Authorization"]
    assert auth.startswith("Bearer ")
    jwt.decode(auth.removeprefix("Bearer "), private_key.public_key(), algorithms=["RS256"],
               audience="api.enablebanking.com")


@respx.mock
def test_list_aspsps_filters_by_country_and_psu_type(client):
    route = respx.get(f"{BASE_URL}/aspsps").respond(
        json={"aspsps": [{"name": "Synthetic Bank", "country": "XX", "maximum_consent_validity": 15552000}]}
    )

    aspsps = client.list_aspsps("XX")

    assert [a["name"] for a in aspsps] == ["Synthetic Bank"]
    assert route.calls.last.request.url.params["country"] == "XX"
    assert route.calls.last.request.url.params["psu_type"] == "personal"


@respx.mock
def test_start_auth_sends_the_consent_request(client):
    route = respx.post(f"{BASE_URL}/auth").respond(
        json={"url": "https://auth.example.test/start?sessionid=s1", "authorization_id": "auth-1"}
    )

    start = client.start_auth(
        aspsp_name="Synthetic Bank",
        aspsp_country="XX",
        redirect_url="https://redirect.example.test/callback",
        valid_until=datetime(2026, 7, 1, tzinfo=UTC),
        state="state-1",
    )

    assert start.url == "https://auth.example.test/start?sessionid=s1"
    assert start.authorization_id == "auth-1"
    sent = route.calls.last.request
    body = json.loads(sent.content)
    assert body == {
        "access": {"valid_until": "2026-07-01T00:00:00+00:00"},
        "aspsp": {"name": "Synthetic Bank", "country": "XX"},
        "state": "state-1",
        "redirect_url": "https://redirect.example.test/callback",
        "psu_type": "personal",
    }


@respx.mock
def test_create_session_exchanges_the_code(client):
    session = {
        "session_id": "00000000-0000-4000-8000-0000000000b1",
        "accounts": [{"uid": ACCOUNT_UID, "account_id": {"iban": "XX00SYNTHETIC0000000002"}, "currency": "EUR"}],
        "aspsp": {"name": "Synthetic Bank", "country": "XX"},
        "psu_type": "personal",
        "access": {"valid_until": "2026-07-01T00:00:00+00:00"},
    }
    route = respx.post(f"{BASE_URL}/sessions").respond(json=session)

    assert client.create_session("code-1") == session
    assert route.calls.last.request.content == b'{"code":"code-1"}'


def test_code_from_redirect_returns_the_code():
    url = "https://redirect.example.test/callback?state=state-1&code=code-1"
    assert code_from_redirect(url, "state-1") == "code-1"


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("https://r.example.test/cb?state=other&code=code-1", "state does not match"),
        ("https://r.example.test/cb?state=state-1", "no code"),
        ("https://r.example.test/cb?state=state-1&error=access_denied&error_description=User+cancelled",
         "access_denied: User cancelled"),
    ],
)
def test_code_from_redirect_rejects_unusable_urls(url, message):
    with pytest.raises(ConsentRedirectError, match=message):
        code_from_redirect(url, "state-1")


@respx.mock
def test_transaction_pages_follows_continuation_keys(client):
    route = respx.get(TX_URL).mock(
        side_effect=[
            httpx.Response(200, json={"transactions": [_transaction("r1")], "continuation_key": "k1"}),
            httpx.Response(200, json={"transactions": [_transaction("r2")], "continuation_key": "k2"}),
            httpx.Response(200, json={"transactions": [_transaction("r3")], "continuation_key": None}),
        ]
    )

    pages = list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))

    assert [t["entry_reference"] for p in pages for t in p["transactions"]] == ["r1", "r2", "r3"]
    params = [c.request.url.params for c in route.calls]
    assert [p.get("continuation_key") for p in params] == [None, "k1", "k2"]
    assert all(p["date_from"] == "2026-01-01" and p["date_to"] == "2026-01-31" for p in params)
    assert all(p["transaction_status"] == "BOOK" for p in params)


@respx.mock
def test_transaction_pages_yields_each_page_before_the_next_request(client):
    route = respx.get(TX_URL).mock(
        side_effect=[
            httpx.Response(200, json={"transactions": [], "continuation_key": "k1"}),
            httpx.Response(200, json={"transactions": []}),
        ]
    )

    pages = client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31))
    next(pages)

    assert route.call_count == 1


@respx.mock
def test_transaction_pages_stops_on_a_repeated_continuation_key(client):
    respx.get(TX_URL).respond(json={"transactions": [], "continuation_key": "same"})

    with pytest.raises(EnableBankingError, match="repeated"):
        list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))


@respx.mock
@pytest.mark.parametrize("code", ["EXPIRED_SESSION", "CLOSED_SESSION", "REVOKED_SESSION"])
def test_a_gone_session_raises_session_gone(client, code):
    respx.get(TX_URL).respond(401, json={"error_code": code, "error_description": "synthetic"})

    with pytest.raises(SessionGoneError) as exc:
        list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))
    assert exc.value.error_code == code


@respx.mock
def test_rate_limit_raises_rate_limited_without_retrying(client):
    route = respx.get(TX_URL).respond(429, json={"error_code": "ASPSP_RATE_LIMIT_EXCEEDED"})

    with pytest.raises(RateLimitedError):
        list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))
    assert route.call_count == 1


@respx.mock
def test_other_errors_raise_with_status_and_code(client):
    respx.get(TX_URL).respond(502, json={"error_code": "ASPSP_ERROR", "error_description": "synthetic"})

    with pytest.raises(EnableBankingError) as exc:
        list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))
    assert type(exc.value) is EnableBankingError
    assert (exc.value.status_code, exc.value.error_code) == (502, "ASPSP_ERROR")


@respx.mock
def test_a_non_json_error_body_still_raises(client):
    respx.get(TX_URL).respond(503, text="<html>bad gateway</html>")

    with pytest.raises(EnableBankingError) as exc:
        list(client.transaction_pages(ACCOUNT_UID, date(2026, 1, 1), date(2026, 1, 31)))
    assert exc.value.status_code == 503
