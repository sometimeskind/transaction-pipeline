"""`python -m transaction_pipeline [serve | consent ... | monzo-auth ... | compare-keys ...]`.

`serve` (also the default, so the image's bare CMD keeps working) runs the Prefect
deployment, scheduled by FETCH_CRON (unset: registered without a schedule).
`consent` is run by hand in the pod, once per bank login, when consent is new or
expired:

    python -m transaction_pipeline consent LABEL --aspsp-name NAME --country CC --redirect-url URL

It prints the bank's URL, the operator approves in the bank and pastes back the URL
the browser lands on, and the session goes to STATE_DIR/sessions/LABEL.json. No
web UI, and no reseal per consent.

`monzo-auth LABEL --redirect-url URL [--backfill]` is the same for a Monzo login:
it prints the Monzo login URL, the operator logs in (emailed link) and pastes back
the redirect URL, then approves access in the Monzo app; the command waits for that
approval, lists the accounts and pots and writes the session, with the token, to
STATE_DIR/sessions/LABEL.json. The holder re-approves every 90 days, with the same
command. With `--backfill` it also fetches the whole history right away (the five
minutes after an approval are the only time Monzo serves more than 90 days) into a
save-only run, to be stored with `replay_run`.

`compare-keys RUN_A RUN_B` compares the content keys of two saved runs (see
compare.py) and prints counts per account, never transaction content. It exits
1 when any account has keys in only one of the runs.
"""

from __future__ import annotations

import argparse
import logging
import os
import secrets
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TextIO

from transaction_pipeline import compare, flow, monzo
from transaction_pipeline.enable_banking import (
    ConsentRedirectError,
    EnableBankingClient,
    EnableBankingError,
    code_from_redirect,
)

# Used when the bank doesn't publish `maximum_consent_validity`; 90 days is the
# shortest maximum banks cap AIS consent at.
FALLBACK_CONSENT_DAYS = 90
# Ask for a little under the bank's maximum so clock skew can't push us over it.
CONSENT_MARGIN = timedelta(minutes=5)
# How long monzo-auth waits for the holder to approve in the Monzo app, polling /accounts.
APPROVAL_POLL_SECONDS = 5
APPROVAL_TIMEOUT_SECONDS = 600


log = logging.getLogger(__name__)


class ConsentError(Exception):
    pass


def session_label(value: str) -> str:
    if not flow.LABEL_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError("lowercase letters, digits and dashes, starting with a letter or digit")
    return value


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m transaction_pipeline")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("serve", help="run the Prefect deployment (the default)")
    consent = commands.add_parser("consent", help="consent to one bank login and save its session to the state dir")
    consent.add_argument("label", type=session_label, help="name for this bank login, e.g. n26; re-consent reuses it")
    consent.add_argument("--aspsp-name", required=True, help="bank name exactly as Enable Banking lists it")
    consent.add_argument("--country", required=True, help="bank's two-letter country code")
    consent.add_argument("--redirect-url", required=True, help="a redirect URL registered on the Enable Banking app")
    consent.add_argument("--psu-type", default="personal", choices=["personal", "business"])
    monzo_auth = commands.add_parser("monzo-auth", help="authorise a Monzo login and save its session to the state dir")
    monzo_auth.add_argument("label", type=session_label, help="name for this login, e.g. monzo; re-approval reuses it")
    monzo_auth.add_argument("--redirect-url", required=True, help="the redirect URL registered on the Monzo OAuth client")
    monzo_auth.add_argument(
        "--backfill", action="store_true",
        help="fetch the whole history into a save-only run right after the approval (first authorisation only)",
    )
    compare_keys = commands.add_parser(
        "compare-keys", help="compare two saved runs' booking keys for the days both cover; prints counts only"
    )
    compare_keys.add_argument("run_a", help="run id: a directory under STATE_DIR/raw")
    compare_keys.add_argument("run_b", help="run id: a directory under STATE_DIR/raw")
    return parser.parse_args(argv)


def find_aspsp(aspsps: list[dict], name: str) -> dict:
    for aspsp in aspsps:
        if aspsp["name"].casefold() == name.casefold():
            return aspsp
    close = sorted(a["name"] for a in aspsps if name.casefold() in a["name"].casefold())
    hint = f"; did you mean one of: {', '.join(close)}" if close else ""
    raise ConsentError(f"no bank named {name!r} in this country{hint}")


def consent_valid_until(aspsp: dict, now: datetime) -> datetime:
    seconds = aspsp.get("maximum_consent_validity")
    validity = timedelta(seconds=seconds) if seconds else timedelta(days=FALLBACK_CONSENT_DAYS)
    return now + validity - CONSENT_MARGIN


def run_consent(
    client: EnableBankingClient,
    args: argparse.Namespace,
    stdin: TextIO,
    stdout: TextIO,
    now: datetime | None = None,
) -> dict:
    aspsp = find_aspsp(client.list_aspsps(args.country, args.psu_type), args.aspsp_name)
    valid_until = consent_valid_until(aspsp, now or datetime.now(UTC))
    state = secrets.token_urlsafe(16)
    auth = client.start_auth(
        aspsp_name=aspsp["name"],
        aspsp_country=args.country,
        redirect_url=args.redirect_url,
        valid_until=valid_until,
        state=state,
        psu_type=args.psu_type,
    )
    print(f"Open this URL and approve access in the bank (consent until {valid_until:%Y-%m-%d}):", file=stdout)
    print(auth.url, file=stdout)
    print("Paste the full URL the browser lands on afterwards:", file=stdout, flush=True)
    redirect = stdin.readline().strip()
    if not redirect:
        raise ConsentError("no redirect URL given")
    session = client.create_session(code_from_redirect(redirect, state))
    session["aspsp"] = {"name": aspsp["name"], "country": args.country}
    path = flow.session_path(args.label)
    flow.write_json(path, session)
    print(
        f"Saved the session to {path}: {len(session.get('accounts', []))} account(s), "
        f"valid until {session['access']['valid_until']}",
        file=stdout,
    )
    return session


def run_monzo_auth(
    client: monzo.MonzoClient,
    args: argparse.Namespace,
    stdin: TextIO,
    stdout: TextIO,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: Callable[[float], None] = time.sleep,
) -> dict:
    state = secrets.token_urlsafe(16)
    print("Open this URL, log in to Monzo (it emails a link), and paste back the URL the browser lands on:", file=stdout)
    print(monzo.auth_url(client.client_id, args.redirect_url, state), file=stdout, flush=True)
    redirect = stdin.readline().strip()
    if not redirect:
        raise ConsentError("no redirect URL given")
    client.exchange_code(code_from_redirect(redirect, state), args.redirect_url)
    print("Now approve access in the Monzo app (waiting for the approval)", file=stdout, flush=True)
    accounts = wait_for_approval(client, now, wait)
    authorised_at = now()
    open_accounts = [a for a in accounts if not a.get("closed")]
    pots = [pot for account in open_accounts for pot in client.pots(account["id"])]
    session = monzo.session(client.token, open_accounts, pots, authorised_at)  # type: ignore[arg-type]
    path = flow.session_path(args.label)
    flow.write_json(path, session)
    # From here on, a refreshed token goes to the file too.
    client.on_token = lambda token: flow.write_json(path, {**session, "token": token.to_json()})
    print(
        f"Saved the session to {path}: {len(open_accounts)} account(s), {len(pots)} pot(s), "
        f"approval valid until {session['access']['valid_until'][:10]}",
        file=stdout,
    )
    if args.backfill:
        backfill(client, session, authorised_at, stdout)
    return session


def wait_for_approval(
    client: monzo.MonzoClient, now: Callable[[], datetime], wait: Callable[[float], None]
) -> list[dict]:
    """Poll /accounts until the token has permissions (403 until the holder approves in the app)."""
    deadline = now() + timedelta(seconds=APPROVAL_TIMEOUT_SECONDS)
    while True:
        try:
            return client.accounts()
        except monzo.AccessGoneError:
            if now() >= deadline:
                raise ConsentError("no approval in the Monzo app within the time limit; run the command again") from None
            wait(APPROVAL_POLL_SECONDS)


def backfill(client: monzo.MonzoClient, session: dict, authorised_at: datetime, stdout: TextIO) -> str:
    """Fetch every account's whole history into a save-only run, inside the five-minute SCA window."""
    run = flow.run_id()
    run_dir = flow.raw_root() / run
    flow.write_json(run_dir / flow.SAVE_ONLY_MARKER, {"saved_at": authorised_at.isoformat()})
    for account in session["accounts"]:
        since = datetime.fromisoformat(account["created"]) if account.get("created") else datetime(2015, 1, 1, tzinfo=UTC)
        count = pages = 0
        for pages, page in enumerate(client.transaction_pages(account["uid"], since, authorised_at), start=1):
            flow.write_json(run_dir / account["uid"] / f"page-{pages}.json", page)
            count += len(page.get("transactions") or [])
        print(f"account {account['uid']}: {pages} page(s), {count} transaction(s) saved", file=stdout)
    print(
        f"Backfill saved as run {run} (save-only, never pruned). Store it with a replay of the deployment: "
        f'parameters {{"replay_run": ["{run}"], "since": "<first date to import>"}}',
        file=stdout,
    )
    return run


def run_compare_keys(args: argparse.Namespace, stdout: TextIO) -> int:
    results = compare.compare_runs(args.run_a, args.run_b, flow.load_sessions(flow.sessions_dir()))
    for result in results:
        print(result.line(), file=stdout)
    return 1 if any(r.only_a or r.only_b for r in results) else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    # Everything this process writes to the state dir is personal financial data.
    # Flow runs started by serve() inherit the umask.
    os.umask(0o077)
    if args.command in (None, "serve"):
        cron = os.environ.get("FETCH_CRON") or None
        logging.basicConfig(level=logging.INFO)
        log.info("transaction-import schedule: %s", cron or "none (FETCH_CRON unset)")
        flow.import_flow.serve(name="transaction-import", cron=cron)
        return 0
    if args.command == "compare-keys":
        return run_compare_keys(args, sys.stdout)
    if args.command == "monzo-auth":
        try:
            client_id, client_secret = flow.monzo_credentials()
            with monzo.MonzoClient(client_id, client_secret) as client:
                run_monzo_auth(client, args, sys.stdin, sys.stdout)
        except (ConsentError, ConsentRedirectError, monzo.MonzoError, flow.ConsentNeededError) as exc:
            print(f"monzo-auth failed: {exc}", file=sys.stderr)
            return 1
        return 0
    try:
        with flow.enable_banking_client() as client:
            run_consent(client, args, sys.stdin, sys.stdout)
    except (ConsentError, ConsentRedirectError, EnableBankingError) as exc:
        print(f"consent failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
