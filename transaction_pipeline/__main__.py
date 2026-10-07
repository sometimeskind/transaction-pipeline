"""`python -m transaction_pipeline [serve | consent ...]`.

`serve` (also the default, so the image's bare CMD keeps working) runs the Prefect
deployment, scheduled by FETCH_CRON (unset: registered without a schedule).
`consent` is run by hand in the pod, once per bank login, when consent is new or
expired:

    python -m transaction_pipeline consent LABEL --aspsp-name NAME --country CC --redirect-url URL

It prints the bank's URL, the operator approves in the bank and pastes back the URL
the browser lands on, and the session goes to STATE_DIR/sessions/LABEL.json. No
web UI, and no reseal per consent.
"""

from __future__ import annotations

import argparse
import logging
import os
import secrets
import sys
from datetime import UTC, datetime, timedelta
from typing import TextIO

from transaction_pipeline import flow
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
    try:
        with flow.enable_banking_client() as client:
            run_consent(client, args, sys.stdin, sys.stdout)
    except (ConsentError, ConsentRedirectError, EnableBankingError) as exc:
        print(f"consent failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
