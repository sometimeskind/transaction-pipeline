"""Subcommand and consent tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import io
import json
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from transaction_pipeline import __main__ as cli
from transaction_pipeline import flow, monzo
from transaction_pipeline.enable_banking import AuthStart, ConsentRedirectError

NOW = datetime(2026, 1, 1, tzinfo=UTC)
SESSION = {
    "session_id": "00000000-0000-4000-8000-0000000000c1",
    "accounts": [{"uid": "00000000-0000-4000-8000-00000000000a", "account_id": {"iban": "XX00SYNTHETIC0000000001"}}],
    "access": {"valid_until": "2026-06-30T00:00:00+00:00"},
}


class FakeBank:
    def __init__(self, aspsps):
        self.aspsps = aspsps
        self.auth: dict = {}
        self.codes: list[str] = []

    def list_aspsps(self, country, psu_type="personal"):
        return self.aspsps

    def start_auth(self, **kwargs):
        self.auth = kwargs
        return AuthStart(url="https://bank.example.test/authorize?id=1", authorization_id="auth-1")

    def create_session(self, code):
        self.codes.append(code)
        return dict(SESSION)


class Redirect(io.StringIO):
    """stdin that pastes back a redirect carrying the state the CLI just generated."""

    def __init__(self, bank, state=None):
        super().__init__()
        self.bank, self.state = bank, state

    def readline(self, *args):
        return f"https://redirect.example.test/cb?code=code-1&state={self.state or self.bank.auth['state']}\n"


def consent_args(*extra):
    return cli.parse_args(["consent", "bank-one", "--aspsp-name", "synthetic bank", "--country", "XX",
                           "--redirect-url", "https://redirect.example.test/cb", *extra])


@pytest.fixture
def state(monkeypatch, tmp_path):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def serve(monkeypatch):
    served, umasks = [], []
    monkeypatch.setattr(flow.import_flow, "serve", lambda **kw: served.append(kw))
    monkeypatch.setattr(cli.os, "umask", umasks.append)
    return served, umasks


@pytest.mark.parametrize("argv", [[], ["serve"]])
def test_serve_is_the_default_and_unscheduled_without_fetch_cron(monkeypatch, serve, argv):
    monkeypatch.delenv("FETCH_CRON", raising=False)
    assert cli.main(argv) == 0
    assert serve == ([{"name": "transaction-import", "cron": None}], [0o077])


def test_serve_schedules_on_fetch_cron(monkeypatch, serve):
    monkeypatch.setenv("FETCH_CRON", "0 6 * * *")
    assert cli.main(["serve"]) == 0
    assert serve[0] == [{"name": "transaction-import", "cron": "0 6 * * *"}]


@pytest.mark.parametrize("argv", [
    ["consent", "bank-one", "--country", "XX"],
    ["consent", "--aspsp-name", "x", "--country", "XX", "--redirect-url", "https://r.example.test"],
    ["consent", "../evil", "--aspsp-name", "x", "--country", "XX", "--redirect-url", "https://r.example.test"],
])
def test_consent_requires_a_plain_label_the_bank_and_redirect(argv):
    with pytest.raises(SystemExit) as exc:
        cli.parse_args(argv)
    assert exc.value.code == 2


def test_consent_parses_its_flags():
    args = consent_args("--psu-type", "business")
    assert (args.command, args.label, args.aspsp_name, args.country, args.psu_type) == (
        "consent", "bank-one", "synthetic bank", "XX", "business")


def test_consent_writes_the_labelled_session_with_the_bank_used(state):
    bank = FakeBank([{"name": "Synthetic Bank", "country": "XX", "maximum_consent_validity": 180 * 86400}])
    out = io.StringIO()

    cli.run_consent(bank, consent_args(), Redirect(bank), out, now=NOW)

    saved = json.loads((state / "sessions" / "bank-one.json").read_text())
    assert saved == {**SESSION, "aspsp": {"name": "Synthetic Bank", "country": "XX"}}
    assert bank.codes == ["code-1"]
    assert bank.auth["aspsp_name"] == "Synthetic Bank"
    assert bank.auth["redirect_url"] == "https://redirect.example.test/cb"
    assert bank.auth["valid_until"] == NOW + timedelta(days=180) - cli.CONSENT_MARGIN
    assert "https://bank.example.test/authorize?id=1" in out.getvalue()


def test_reconsent_replaces_only_its_own_session(state):
    other = state / "sessions" / "bank-two.json"
    flow.write_json(other, {"untouched": True})
    flow.write_json(state / "sessions" / "bank-one.json", {"expired": True})
    bank = FakeBank([{"name": "Synthetic Bank"}])
    cli.run_consent(bank, consent_args(), Redirect(bank), io.StringIO(), now=NOW)
    assert json.loads((state / "sessions" / "bank-one.json").read_text())["session_id"] == SESSION["session_id"]
    assert json.loads(other.read_text()) == {"untouched": True}


def test_consent_falls_back_to_90_days_without_a_published_maximum(state):
    bank = FakeBank([{"name": "Synthetic Bank", "country": "XX"}])
    cli.run_consent(bank, consent_args(), Redirect(bank), io.StringIO(), now=NOW)
    assert bank.auth["valid_until"] == NOW + timedelta(days=90) - cli.CONSENT_MARGIN


def test_an_unknown_bank_suggests_close_names_and_writes_nothing(state):
    bank = FakeBank([{"name": "Synthetic Bank Business"}, {"name": "Other Bank"}])
    with pytest.raises(cli.ConsentError, match="Synthetic Bank Business"):
        cli.run_consent(bank, consent_args(), Redirect(bank), io.StringIO(), now=NOW)
    assert not (state / "sessions" / "bank-one.json").exists()


def test_a_redirect_from_another_request_writes_nothing(state):
    bank = FakeBank([{"name": "Synthetic Bank"}])
    with pytest.raises(ConsentRedirectError, match="state"):
        cli.run_consent(bank, consent_args(), Redirect(bank, state="someone-else"), io.StringIO(), now=NOW)
    assert bank.codes == []
    assert not (state / "sessions" / "bank-one.json").exists()


def test_an_empty_paste_writes_nothing(state):
    bank = FakeBank([{"name": "Synthetic Bank"}])
    with pytest.raises(cli.ConsentError, match="no redirect URL"):
        cli.run_consent(bank, consent_args(), io.StringIO(""), io.StringIO(), now=NOW)
    assert not (state / "sessions" / "bank-one.json").exists()


# monzo-auth

MONZO_ACC = "acc_0000synthetic00000000001"
MONZO_POT = "pot_0000synthetic00000000001"
MONZO_ACCOUNTS = [
    {"id": MONZO_ACC, "closed": False, "type": "uk_retail", "sort_code": "040004", "account_number": "12345678",
     "currency": "GBP", "created": "2020-01-01T00:00:00Z"},
    {"id": "acc_0000synthetic00000000002", "closed": True, "type": "uk_prepaid"},
    {"id": "acc_0000synthetic00000000003", "closed": False, "type": "uk_monzo_flex", "currency": "GBP"},
]


class FakeMonzo:
    def __init__(self, accounts, pots=(), pages=None, denials: int = 0):
        self.client_id = "client-1"
        self.token = None
        self.on_token = None
        self.accounts_, self.pots_, self.pages = accounts, list(pots), pages or {}
        self.denials = denials
        self.exchanged: list[tuple[str, str]] = []
        self.page_calls: list[tuple] = []

    def exchange_code(self, code, redirect_url):
        self.exchanged.append((code, redirect_url))
        self.token = monzo.Token("access-1", "refresh-1", NOW + timedelta(hours=6))
        return self.token

    def accounts(self):
        if self.denials:
            self.denials -= 1
            raise monzo.AccessGoneError(403, "forbidden.insufficient_permissions", "approve in the app")
        return self.accounts_

    def pots(self, account_id):
        return self.pots_

    def transaction_pages(self, uid, since, before):
        self.page_calls.append((uid, since, before))
        yield from self.pages.get(uid, [])


class MonzoRedirect(io.StringIO):
    """stdin that pastes back a redirect carrying the state from the URL the CLI printed."""

    def __init__(self, out, state=None):
        super().__init__()
        self.out, self.state = out, state

    def readline(self, *args):
        url = self.out.getvalue().splitlines()[1]
        state = self.state or parse_qs(urlsplit(url).query)["state"][0]
        return f"https://redirect.example.test/cb?code=code-1&state={state}\n"


def monzo_args(*extra):
    return cli.parse_args(["monzo-auth", "monzo", "--redirect-url", "https://redirect.example.test/cb", *extra])


@pytest.mark.parametrize("argv", [
    ["monzo-auth", "monzo"],
    ["monzo-auth", "--redirect-url", "https://r.example.test"],
    ["monzo-auth", "../evil", "--redirect-url", "https://r.example.test"],
])
def test_monzo_auth_requires_a_plain_label_and_the_redirect(argv):
    with pytest.raises(SystemExit) as exc:
        cli.parse_args(argv)
    assert exc.value.code == 2


def test_monzo_auth_waits_for_the_approval_then_writes_the_session(state):
    client = FakeMonzo(MONZO_ACCOUNTS, pots=[{"id": MONZO_POT, "name": "Synthetic pot", "deleted": False}], denials=2)
    out, waits = io.StringIO(), []

    session = cli.run_monzo_auth(client, monzo_args(), MonzoRedirect(out), out, now=lambda: NOW, wait=waits.append)

    assert client.exchanged == [("code-1", "https://redirect.example.test/cb")]
    assert waits == [cli.APPROVAL_POLL_SECONDS] * 2
    path = state / "sessions" / "monzo.json"
    saved = json.loads(path.read_text())
    assert saved == session
    assert (saved["provider"], saved["token"]["refresh_token"], saved["authorised_at"]) == ("monzo", "refresh-1", NOW.isoformat())
    assert saved["accounts"] == [{"uid": MONZO_ACC, "number": "04000412345678", "currency": "GBP", "type": "uk_retail",
                                  "created": "2020-01-01T00:00:00Z"}]
    assert saved["pots"] == [{"id": MONZO_POT, "name": "Synthetic pot", "deleted": False}]
    assert saved["access"]["valid_until"] == (NOW + timedelta(days=90) - monzo.REAUTH_MARGIN).isoformat()
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert "auth.monzo.com" in out.getvalue() and "1 account(s), 1 pot(s)" in out.getvalue()
    # The open Flex account has no sort code and account number: said so, left out of the session.
    assert "Skipping account acc_0000synthetic00000000003 (uk_monzo_flex)" in out.getvalue()
    # A token refreshed later lands in the file too.
    client.on_token(monzo.Token("access-2", "refresh-2", NOW))
    assert json.loads(path.read_text())["token"]["refresh_token"] == "refresh-2"
    assert json.loads(path.read_text())["accounts"] == saved["accounts"]


def test_monzo_auth_fails_when_no_account_has_a_sort_code_and_number(state):
    client = FakeMonzo([MONZO_ACCOUNTS[1], MONZO_ACCOUNTS[2]])
    out = io.StringIO()
    with pytest.raises(cli.ConsentError, match="no open account with a sort code"):
        cli.run_monzo_auth(client, monzo_args(), MonzoRedirect(out), out, now=lambda: NOW, wait=lambda s: None)
    assert not (state / "sessions" / "monzo.json").exists()


def test_monzo_auth_gives_up_when_the_approval_does_not_come(state):
    client = FakeMonzo(MONZO_ACCOUNTS, denials=10**6)
    clock = [NOW]

    def now():
        clock[0] += timedelta(seconds=301)
        return clock[0]

    out, waits = io.StringIO(), []
    with pytest.raises(cli.ConsentError, match="approval"):
        cli.run_monzo_auth(client, monzo_args(), MonzoRedirect(out), out, now=now, wait=waits.append)
    assert waits == [cli.APPROVAL_POLL_SECONDS]
    assert not (state / "sessions" / "monzo.json").exists()


def test_monzo_auth_backfill_saves_a_save_only_run_for_a_replay(state, monkeypatch):
    pages = [{"transactions": [{"id": "tx_1"}, {"id": "tx_2"}]}, {"transactions": [{"id": "tx_3"}]}]
    client = FakeMonzo(MONZO_ACCOUNTS, pages={MONZO_ACC: pages})
    monkeypatch.setattr(flow, "run_id", lambda: "manual-20260101T120000Z")
    out = io.StringIO()

    cli.run_monzo_auth(client, monzo_args("--backfill"), MonzoRedirect(out), out, now=lambda: NOW, wait=lambda s: None)

    run_dir = state / "raw" / "manual-20260101T120000Z"
    assert (run_dir / flow.SAVE_ONLY_MARKER).exists()
    assert sorted(p.name for p in (run_dir / MONZO_ACC).iterdir()) == ["page-1.json", "page-2.json"]
    assert json.loads((run_dir / MONZO_ACC / "page-2.json").read_text()) == pages[1]
    assert client.page_calls == [(MONZO_ACC, datetime(2020, 1, 1, tzinfo=UTC), NOW)]
    text = out.getvalue()
    assert f"account {MONZO_ACC}: 2 page(s), 3 transaction(s)" in text
    assert '"replay_run": ["manual-20260101T120000Z"]' in text


def test_monzo_auth_without_backfill_fetches_nothing(state):
    client = FakeMonzo(MONZO_ACCOUNTS)
    out = io.StringIO()
    cli.run_monzo_auth(client, monzo_args(), MonzoRedirect(out), out, now=lambda: NOW, wait=lambda s: None)
    assert client.page_calls == [] and not (state / "raw").exists()


def test_a_redirect_from_another_request_writes_no_monzo_session(state):
    client = FakeMonzo(MONZO_ACCOUNTS)
    out = io.StringIO()
    with pytest.raises(ConsentRedirectError, match="state"):
        cli.run_monzo_auth(client, monzo_args(), MonzoRedirect(out, state="someone-else"), out, now=lambda: NOW)
    assert client.exchanged == []
    assert not (state / "sessions" / "monzo.json").exists()


def test_monzo_auth_fails_without_the_client_credentials(state, monkeypatch, capsys):
    monkeypatch.delenv("MONZO_CLIENT_ID", raising=False)
    monkeypatch.delenv("MONZO_CLIENT_SECRET", raising=False)
    assert cli.main(["monzo-auth", "monzo", "--redirect-url", "https://r.example.test"]) == 1
    assert "MONZO_CLIENT_ID" in capsys.readouterr().err
