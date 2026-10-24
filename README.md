# transaction-pipeline

Prefect pipeline that imports bank transactions into Firefly III. It fetches
booked transactions from [Enable Banking](https://enablebanking.com/) (PSD2
account information) and from the [Monzo developer API](https://docs.monzo.com/)
(a UK bank Enable Banking doesn't reach), maps them with logic tuned to our own
accounts, and writes them through the Firefly III API. It replaces the Firefly III
data importer. Intent and done-when:
[homelab#1981](https://github.com/sometimeskind/homelab/issues/1981) (Enable Banking,
live since 2026-10-15) and [#18](https://github.com/sometimeskind/transaction-pipeline/issues/18)
(Monzo).

## Consent

Consent is given by hand in the running pod, once per bank login, when it is new or
has expired:

```
python -m transaction_pipeline consent LABEL --aspsp-name NAME --country CC --redirect-url URL
```

`LABEL` names the bank login (lowercase letters, digits and dashes, e.g. `n26`);
re-consent reuses it and replaces only that session. `--redirect-url` must be one
registered on the Enable Banking app. The command prints the bank's URL; approve
access there, then paste back the full URL the browser lands on (the page itself may
not load; only the URL matters). The session goes to `$STATE_DIR/sessions/LABEL.json`,
so re-consent needs no secret and no PR. The consent validity asked for is the bank's
published maximum.

A Monzo login is authorised the same way, against Monzo's own OAuth (a confidential
client of the holder's own, created at developers.monzo.com; the API is for personal
use only):

```
python -m transaction_pipeline monzo-auth LABEL --redirect-url URL [--backfill]
```

It prints the Monzo login URL (Monzo emails a link); paste back the URL the browser
lands on, then approve access in the Monzo app. The command waits for that approval,
lists the open accounts and the pots, and writes the session, token included, to
`$STATE_DIR/sessions/LABEL.json` with `"provider": "monzo"`. Under Strong Customer
Authentication the holder re-approves every 90 days: the session's `valid_until` is
that deadline, so the same gauge and alerts cover it, and the same command renews
it. `--backfill` fetches every account's whole history right away into a save-only
run: the five minutes after an approval are the only time Monzo serves more than
the last 90 days. Use it on the first authorisation only, then store the run with
`replay_run` (below).

## Configuration

| Env var | Meaning |
|---------|---------|
| `ENABLE_BANKING_APP_ID`, `ENABLE_BANKING_PRIVATE_KEY_PATH` | Enable Banking app and its `.pem` |
| `MONZO_CLIENT_ID`, `MONZO_CLIENT_SECRET` | the Monzo OAuth client (confidential); only needed with a Monzo session |
| `FIREFLY_URL`, `FIREFLY_TOKEN` | Firefly III API |
| `STATE_DIR` | state directory, default `/state` |
| `FETCH_CRON` | schedule for `serve`; unset registers the deployment without one |
| `FIREFLY_WRITE` | `true` or `false` (default). Until `true`, runs only fetch and save raw pages |
| `MAPPING_CONFIG` | mapping rules file, default `/config/mapping.toml`; no file means no rules |
| `PUSHGATEWAY_URL` | metrics; unset means none are pushed |

Metrics, both in Pushgateway group `transaction-pipeline`:
`transaction_pipeline_consent_valid_until_timestamp{session="LABEL"}` on every run, and
`transaction_pipeline_last_success_timestamp` after a run that stored into Firefly
(never in save-only mode, so a staleness alert keeps firing until the first real import).

## State

`STATE_DIR` (default `/state`, a PVC in the cluster):

- `sessions/<label>.json`: one Enable Banking session per bank login, with the bank
  name and country used, or a Monzo session (`"provider": "monzo"`) with its OAuth
  token, accounts and pots; the refresh token rotates, so the flow rewrites the file
  after each refresh. The flow fetches every account of every session.
- `raw/<flow-run-id>/<account-uid>/page-<n>.json`: every transaction page as the
  bank returned it, written before anything is mapped. The store step reads them back,
  so it retries (on Firefly errors only) without spending the bank budget.

Both are personal financial data: owner-only files, never passed through Prefect. With
`FIREFLY_WRITE=true`, a run's raw pages are deleted once stored, and a failed run's
stay 7 days for debugging. In save-only mode raw pages are kept: they are what the
mapping rules are written from. A save-only run marks its directory with a
`save-only` file, and pruning never deletes a marked directory, so the backfill
survives until it is replayed. The save-only runs left over after go-live are deleted
by hand.

**Backfill at go-live.** The first fetch right after consent is exempt from the PSD2
budget and gets the most history, so it runs save-only with a large `window_days`
(e.g. 1095) and its pages stay on the PVC. After the clean start, the flow parameter
`replay_run=<run-id>` maps and stores that run's pages without fetching. Given a list
of run ids, it maps them in one pass, each account's bookings being the union by key
of every run's: each login's backfill is its own run, and moves between a shared
Space and either login's account only pair when both are mapped together. Add the
save-only daily runs since the backfills to cover the days up to go-live, and
`since=<date>` to leave out the days before every account's history starts. It honours
`FIREFLY_WRITE`: when that is off, it maps only and stores nothing, which checks the
rules against real data. A replay never prunes and pushes no success timestamp. A
replay that writes deletes the replayed pages once they are stored. It is for the
backfill only: failure recovery is the overlapping window. A run saved before a re-consent can't be replayed, because accounts get new
uids on re-consent.

One bank login failing (expired consent, rate limit) doesn't stop the others: their
transactions are still stored, and the run then fails naming what went wrong.

## Data rule

This repo is public on purpose. **Test data is synthetic, always.** Real
transactions, account numbers/IBANs, counterparty names, Enable Banking session
or account IDs, and raw bank responses never go into commits, fixtures, issues or
PRs here. Debugging that needs real data happens in the homelab repo's issues.
Write fixtures from the Enable Banking API schema, not from a captured response.

## Constraints the design follows

- **4 unattended requests per account per 24h** (PSD2 SCA RTS art. 36(5)), so the
  import runs daily and a retry must not hit the bank blindly.
- **Consent lasts up to 180 days** (90 at some banks). The session's
  `valid_until` drives an advance warning, not just a failure after expiry.
- **Transaction responses paginate** via `continuation_key`.
- **Monzo has no request budget but a 90-day approval** (SCA), and serves the full
  history only in the five minutes after an approval; afterwards the last 90 days.
  Its API filters on `created` and a card payment settles days later, so its fetch
  window starts 30 days before the run's, and only settled transactions are mapped.

Prefect is the scheduler, never the source of truth
([homelab#1981 comment](https://github.com/sometimeskind/homelab/issues/1981), from the
Prefect review in homelab#1983):

- **No Prefect retries on the bank fetch.** A retry spends the PSD2 budget. The raw
  Enable Banking response (every `continuation_key` page) is saved to the state PVC
  before mapping, and a failed Firefly write retries from disk, not from the bank.
- **Idempotency lives in Firefly** (a key derived from each booking's content →
  Firefly `external_id`, see [Idempotency](#idempotency)), not in Prefect run
  state. A re-run after a Prefect outage or a lost Prefect database creates no
  duplicates.
- **Staleness is alerted without Prefect.** Each successful run pushes
  `transaction_pipeline_last_success_timestamp` to the Pushgateway; the homelab
  alerts on its age. `PrefectDeploymentFailing` can't see a scheduler that stopped
  scheduling.

## Mapping

`mapping.py` turns one raw Enable Banking transaction into one Firefly
transaction: a withdrawal, a deposit, or a transfer when the other side is an own
account. Own accounts are found in four ways, in this order:

- **By IBAN:** any Firefly asset account with its IBAN set. A transfer between
  two imported accounts is created once, from its debit side.
- **By account number, for an account without an IBAN:** some banks list
  sub-accounts (N26 Spaces) as accounts of their own with no IBAN. These are
  matched by Enable Banking's `identification_hash`, which stays the same across
  re-consent and across logins. To set one up, create a Firefly asset account
  for it and put the account's `identification_hash` (from the session file
  under `STATE_DIR/sessions/`) in its **account number**, leaving the IBAN
  empty. A Monzo account has no IBAN either: its Firefly account carries the sort
  code followed by the account number, digits only (the session's `number`), and
  a counterparty showing those digits on another own account's booking is a
  transfer to it. A Monzo pot is a sub-account whose moves appear on the current
  account naming the pot by id: give each pot a Firefly asset account with the pot
  id as its account number. A run fails while an imported account has no Firefly
  account with its IBAN or number, or a pot of a Monzo session has none.
- **By pairing internal moves:** some banks book a move between two own accounts
  as a SEPA credit transfer (`ICDT/ESCT` out, `RCDT/ESCT` in) whose counterparty
  shows no IBAN or account number, and often no name, on both sides (N26 Spaces,
  and some transfers between two N26 customers). The text doesn't reliably say
  which account is on the other side. So each run pairs these *internal moves*
  across every imported account of every session: an outgoing and an incoming one
  on two accounts, on the same `booking_date`, with the same amount and currency,
  and the same remittance text or none on one side. A pair becomes one transfer
  from its debit side. Where several bookings could pair (the same amount moved
  out and back on one day, or a Space paying both current accounts), each group
  of the same day, amount and currency is paired as a maximum matching, preferring
  equal texts. It pairs only when every such matching gives the same transfers
  between accounts, so which of two bookings on one account goes with which
  credit makes no difference; otherwise nothing in the group pairs.
- **By rule:** for own accounts that bookings don't name by IBAN, such as
  sub-accounts or a payment service in the middle. Rules live in a TOML file at
  `MAPPING_CONFIG` (default `/config/mapping.toml`). The file is optional, and no
  file means no rules. The deployment mounts it from a secret, because the real
  rules describe our accounts and never go in this repo. The format is in the
  `mapping.py` docstring. A rule can be limited to bookings on one account
  (`booked_on`) and can say the other side never appears in the bank data
  (`other_side_missing`), so the credit itself is the transfer: N26 round-ups show
  as a credit on the Space, while the card payment carries the purchase amount only.
- **Not a move at all:** a nameless booking with the same bank codes that isn't
  a move between own accounts, such as a bank's own refund (N26 chargeback
  refunds, fee refunds), gets a counterparty name from a `[[counterparty]]` rule
  (`name` and `remittance`, optional `booked_on`). It is then booked as an
  ordinary deposit or withdrawal. Such a rule never applies to a booking that
  has a name.

An internal move without a counterparty name that neither pairs nor matches a rule
is never booked against Firefly's cash account: it isn't stored, and the run fails
with a count per account once everything else is stored. When an account has no
pages in a run (its fetch failed), the other side of a move may be missing, so that
run stores no internal moves at all and counts them as deferred; the next run's
overlapping window pairs them. Internal moves on the latest booking day in a run's
pages are deferred too, since the run may have seen only part of that day.

An account that two logins share (a shared Space, a joint account) is listed by
both sessions under different uids. Its bookings get the same keys under either
session, so a run maps the union, by key, of what both sessions fetched for it.

`firefly.py` stores each transaction only when no transaction with its
`external_id` exists, and never updates an existing one.

### Pass-through transfers

A transfer with an own account that is not imported and keeps another currency (a
payment service such as Wise between a GBP and a EUR account) needs Firefly's
`foreign_amount` in that account's currency, and the bank data has only this side's
amount. The flow stores such bookings last and takes the amount from the account's
other leg in Firefly: the transfer out of it (for a debit into it; into it for a
credit) to another own account, dated from the booking day to 7 days later, the
earliest one that has no transaction link yet. Once stored, the two are linked
(link type "Related"), so the pairing shows in Firefly and a rerun is idempotent.
No leg yet while that window is still open leaves the booking for a later run; a
closed window counts it as unpaired, and the run fails after storing everything
else. The way out is to create the missing leg in Firefly by hand. The
pass-through account's balance then stays near zero, and a drift flags a missing
leg.

## Idempotency

Every booking's Firefly `external_id` is derived from its content:
`tp1:` followed by a SHA-256 hex digest of

- the account key: the account's IBAN, else its number (Enable Banking's
  `identification_hash`; a Monzo account's sort code and account number), never
  the session uid, which changes on re-consent,
- `booking_date`, the amount (unsigned), currency and direction
  (`credit_debit_indicator`, or the amount's sign when there is none),
- an ordinal among the bookings with those same fields on that account and day:
  1, 2, … in the order the run's pages list them, counted across all pages.

Only fields that can't change once a booking is final go in. Counterparty,
remittance text, `value_date` and bank codes stay out (banks trim or reword text);
they still feed the description. The `tp1:` prefix makes keys of a later scheme
recognisable. A booking without a `booking_date`, an amount or a direction fails
the run, since there is nothing to key it on.

**Why not the bank's references.** N26 through Enable Banking sends no
`entry_reference` or `transaction_id` for most bookings, and sends an
`entry_reference` for only some recent ones. A booking fetched once with a
reference and once without would get two keys and be stored twice. So references
are never part of the key; an `entry_reference`, when present, goes into the
Firefly transaction's `internal_reference` for tracing.

**Why the ordinal is stable.** The fetch window covers whole days, so every fetch
sees the same bookings for a past day. If the bank lists two identical bookings in
another order, the two keys still both exist, so nothing is stored twice. A booking
added to a day after an earlier fetch gets the next ordinal, a new key. A changed
amount is a new key too; the store's amount check (`ExternalIdConflictError`) stays
for a key that comes back with another amount, which the key rules out by
construction.

**Checking it against real data.** In the pod, compare two saved runs (directory
names under `$STATE_DIR/raw/`), e.g. the backfill and a later save-only daily run:

```
python -m transaction_pipeline compare-keys RUN_A RUN_B
```

It loads the sessions, computes both runs' keys and prints one line per account
(uid prefix only): how many keys of the days both runs cover are in both, only in
A and only in B. Counts only, never transaction content. The days compared run
from the later of the two runs' first booking dates to the earlier of their last
booking dates, leaving out that last day, which the earlier run may have been
fetched during. An account with pages in only one run is listed as not compared.
It exits 1 when any account has a key in only one run; zero on both sides for
every account means the key is stable between fetches.

## Development

```
pip install -r requirements.txt -r requirements-dev.txt -e .
pytest --import-mode=importlib tests/
```

Or build and run the `dev` stage of the Dockerfile.
