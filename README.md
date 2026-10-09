# transaction-pipeline

Prefect pipeline that imports bank transactions into Firefly III. It fetches
booked transactions from [Enable Banking](https://enablebanking.com/) (PSD2
account information), maps them with logic tuned to our own accounts, and writes
them through the Firefly III API. It replaces the Firefly III data importer.
Intent and done-when: [homelab#1981](https://github.com/sometimeskind/homelab/issues/1981).

**Status:** complete, not live yet. The consent command, the flow, the Firefly client and
the mapping exist. Go-live (save-only runs, backfill, clean start, writes on) is
tracked in homelab#1981.

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

## Configuration

| Env var | Meaning |
|---------|---------|
| `ENABLE_BANKING_APP_ID`, `ENABLE_BANKING_PRIVATE_KEY_PATH` | Enable Banking app and its `.pem` |
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
  name and country used. The flow fetches every account of every session.
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
`replay_run=<run-id>` maps and stores that run's pages without fetching. It honours
`FIREFLY_WRITE`: when that is off, it maps only and stores nothing, which checks the
rules against real data. A replay never prunes and pushes no success timestamp. A
replay that writes deletes the replayed pages once they are stored. It is for the backfill only: failure recovery is the overlapping
window. A run saved before a re-consent can't be replayed, because accounts get new
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
account. Own accounts are found in three ways:

- **By IBAN:** any Firefly asset account with its IBAN set. A transfer between
  two imported accounts is created once, from its debit side.
- **By account number, for an account without an IBAN:** some banks list
  sub-accounts (N26 Spaces) as accounts of their own with no IBAN. These are
  matched by Enable Banking's `identification_hash`, which stays the same across
  re-consent and across logins. To set one up, create a Firefly asset account
  for it and put the account's `identification_hash` (from the session file
  under `STATE_DIR/sessions/`) in its **account number**, leaving the IBAN
  empty. A run fails while an imported account has no Firefly account with its
  IBAN or hash.
- **By rule:** for own accounts that bookings don't name by IBAN, such as
  sub-accounts or a payment service in the middle. Rules live in a TOML file at
  `MAPPING_CONFIG` (default `/config/mapping.toml`). The file is optional, and no
  file means no rules. The deployment mounts it from a secret, because the real
  rules describe our accounts and never go in this repo. The format is in the
  `mapping.py` docstring.

An account that two logins share (a shared Space, a joint account) is listed by
both sessions under different uids, and is mapped once per run, from the first
session that fetched it. Its bookings get the same keys under either session.

`firefly.py` stores each transaction only when no transaction with its
`external_id` exists, and never updates an existing one.

## Idempotency

Every booking's Firefly `external_id` is derived from its content:
`tp1:` followed by a SHA-256 hex digest of

- the account key: the account's IBAN, else its Enable Banking
  `identification_hash` (never the session uid, which changes on re-consent),
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
