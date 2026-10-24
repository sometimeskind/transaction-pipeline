"""Firefly client tests. All fixtures are synthetic, written from the Firefly III v1 API schema."""

import json

import httpx
import pytest
import respx

from transaction_pipeline.firefly import ExternalIdConflictError, FireflyClient, FireflyError, StoreResult

URL = "https://firefly.example.test"
SEARCH = f"{URL}/api/v1/search/transactions"
STORE = f"{URL}/api/v1/transactions"


@pytest.fixture
def firefly():
    with FireflyClient(URL + "/", "synthetic-token") as c:
        yield c


def _payload(external_id: str | None = "ref-1", amount: str = "12.34") -> dict:
    return {
        "apply_rules": True,
        "transactions": [
            {
                "type": "withdrawal",
                "date": "2026-01-02",
                "amount": amount,
                "description": "synthetic purchase",
                "external_id": external_id,
                "source_id": "1",
                "destination_name": "Example Grocer",
            }
        ],
    }


def _group(group_id: str, external_id: str, amount: str = "12.340000000000") -> dict:
    return {
        "type": "transactions",
        "id": group_id,
        "attributes": {"transactions": [{"external_id": external_id, "amount": amount}]},
    }


def _page(data: list, page: int = 1, total_pages: int = 1) -> dict:
    return {"data": data, "meta": {"pagination": {"current_page": page, "total_pages": total_pages}}}


@respx.mock
def test_accounts_follows_pagination_and_sends_the_token(firefly):
    route = respx.get(f"{URL}/api/v1/accounts").mock(
        side_effect=[
            httpx.Response(200, json=_page([{"id": "1"}], 1, 2)),
            httpx.Response(200, json=_page([{"id": "2"}], 2, 2)),
        ]
    )

    assert [a["id"] for a in firefly.accounts("asset")] == ["1", "2"]
    request = route.calls[0].request
    assert request.headers["Authorization"] == "Bearer synthetic-token"
    assert request.url.params["type"] == "asset"
    assert route.calls[1].request.url.params["page"] == "2"


@respx.mock
def test_store_if_absent_creates_when_the_external_id_is_new(firefly):
    search = respx.get(SEARCH).respond(json=_page([]))
    store = respx.post(STORE).respond(200, json={"data": {"type": "transactions", "id": "77"}})

    assert firefly.store_if_absent(_payload()) == StoreResult(created=True, group_id="77")
    assert search.calls[0].request.url.params["query"] == 'external_id_is:"ref-1"'
    assert json.loads(store.calls[0].request.content) == _payload()


@respx.mock
def test_store_if_absent_does_not_touch_an_existing_transaction(firefly):
    respx.get(SEARCH).respond(json=_page([_group("5", "ref-1")]))
    store = respx.post(STORE)

    assert firefly.store_if_absent(_payload()) == StoreResult(created=False, group_id="5")
    assert not store.called


@respx.mock
def test_a_looser_search_hit_is_not_a_match(firefly):
    respx.get(SEARCH).respond(json=_page([_group("5", "ref-10")]))
    respx.post(STORE).respond(200, json={"data": {"id": "78"}})

    assert firefly.store_if_absent(_payload()).created is True


@respx.mock
def test_same_external_id_with_another_amount_is_a_conflict(firefly):
    respx.get(SEARCH).respond(json=_page([_group("5", "ref-1", amount="99.00")]))
    store = respx.post(STORE)

    with pytest.raises(ExternalIdConflictError, match="99.00"):
        firefly.store_if_absent(_payload())
    assert not store.called


@respx.mock
def test_external_id_on_several_groups_is_a_conflict(firefly):
    respx.get(SEARCH).respond(json=_page([_group("5", "ref-1"), _group("6", "ref-1")]))

    with pytest.raises(ExternalIdConflictError, match="several groups"):
        firefly.store_if_absent(_payload())


def test_store_without_external_id_is_refused(firefly):
    with pytest.raises(ValueError, match="external_id"):
        firefly.store_if_absent(_payload(external_id=None))


def test_external_id_that_cannot_be_quoted_is_refused(firefly):
    with pytest.raises(ValueError, match="quote"):
        firefly.find_by_external_id('ref"1')


@respx.mock
def test_validation_error_carries_firefly_messages(firefly):
    respx.get(SEARCH).respond(json=_page([]))
    respx.post(STORE).respond(
        422,
        json={"message": "The given data was invalid.", "errors": {"transactions.0.amount": ["Invalid amount."]}},
    )

    with pytest.raises(FireflyError) as err:
        firefly.store_if_absent(_payload())
    assert err.value.status_code == 422
    assert "transactions.0.amount" in err.value.errors


# Journal ids, account transactions and links (pass-through transfers)

from datetime import date  # noqa: E402

CREATED = {"data": {"type": "transactions", "id": "77",
                    "attributes": {"transactions": [{"external_id": "ref-1", "transaction_journal_id": "501"}]}}}


@respx.mock
def test_a_created_transaction_reports_its_journal_id(firefly):
    respx.get(SEARCH).respond(json=_page([]))
    respx.post(STORE).respond(200, json=CREATED)
    assert firefly.store_if_absent(_payload()) == StoreResult(created=True, group_id="77", journal_id="501")


@respx.mock
def test_an_existing_transaction_reports_its_journal_id(firefly):
    group = _group("5", "ref-1")
    group["attributes"]["transactions"][0]["transaction_journal_id"] = "401"
    respx.get(SEARCH).respond(json=_page([group]))
    assert firefly.store_if_absent(_payload()) == StoreResult(created=False, group_id="5", journal_id="401")


@respx.mock
def test_account_transactions_flatten_the_splits_of_a_date_range(firefly):
    route = respx.get(f"{URL}/api/v1/accounts/9/transactions").respond(json=_page([
        {"id": "1", "attributes": {"transactions": [{"transaction_journal_id": "11"}, {"transaction_journal_id": "12"}]}},
        {"id": "2", "attributes": {"transactions": [{"transaction_journal_id": "21"}]}},
    ]))
    splits = firefly.account_transactions("9", date(2026, 1, 2), date(2026, 1, 9))
    assert [s["transaction_journal_id"] for s in splits] == ["11", "12", "21"]
    params = route.calls[0].request.url.params
    assert (params["start"], params["end"], params["type"]) == ("2026-01-02", "2026-01-09", "transfer")


@respx.mock
def test_links_of_a_journal(firefly):
    respx.get(f"{URL}/api/v1/transaction-journals/11/links").respond(
        json=_page([{"id": "3", "attributes": {"inward_id": "11", "outward_id": "21"}}]))
    respx.get(f"{URL}/api/v1/transaction-journals/12/links").respond(json=_page([]))
    assert [link["id"] for link in firefly.links_of("11")] == ["3"]
    assert firefly.links_of("12") == []


@respx.mock
def test_link_posts_the_type_by_name(firefly):
    route = respx.post(f"{URL}/api/v1/transaction-links").respond(200, json={"data": {"id": "3"}})
    assert firefly.link("11", "21")["id"] == "3"
    assert json.loads(route.calls[0].request.content) == {"link_type_name": "Related", "inward_id": "11", "outward_id": "21"}
