"""Subcommand and consent tests. All fixtures are synthetic, written from the Enable Banking API schema."""

import io
import json
from datetime import UTC, datetime, timedelta

import pytest

from transaction_pipeline import __main__ as cli
from transaction_pipeline import flow
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
