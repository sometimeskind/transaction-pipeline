# transaction-pipeline

Prefect pipeline that imports bank transactions into Firefly III. It fetches
booked transactions from [Enable Banking](https://enablebanking.com/) (PSD2
account information), maps them with logic tuned to our own accounts, and writes
them through the Firefly III API. It replaces the Firefly III data importer.
Intent and done-when: [homelab#1981](https://github.com/sometimeskind/homelab/issues/1981).

**Status:** the flow and the consent command exist; the Firefly client and the mapping
(`firefly.py`, `mapping.py`) are being written.

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
- **Idempotency lives in Firefly** (Enable Banking `entry_reference`, or
  `transaction_id` when a bank sends none → Firefly `external_id`), not in Prefect
  run state. A booking with neither fails the run. A re-run after a Prefect outage or a lost
  Prefect database creates no duplicates.
- **Staleness is alerted without Prefect.** Each successful run pushes
  `transaction_pipeline_last_success_timestamp` to the Pushgateway; the homelab
  alerts on its age. `PrefectDeploymentFailing` can't see a scheduler that stopped
  scheduling.

## Mapping

`mapping.py` turns one raw Enable Banking transaction into one Firefly
transaction: a withdrawal, a deposit, or a transfer when the other side is an own
account. Own accounts are found in two ways:

- **By IBAN:** any Firefly asset account with its IBAN set. A transfer between
  two imported accounts is created once, from its debit side.
- **By rule:** for own accounts that bookings don't name by IBAN, such as
  sub-accounts or a payment service in the middle. Rules live in a TOML file at
  `MAPPING_CONFIG` (default `/config/mapping.toml`). The file is optional, and no
  file means no rules. The deployment mounts it from a secret, because the real
  rules describe our accounts and never go in this repo. The format is in the
  `mapping.py` docstring.

`firefly.py` stores each transaction only when no transaction with its
`external_id` exists, and never updates an existing one.

## Development

```
pip install -r requirements.txt -r requirements-dev.txt -e .
pytest --import-mode=importlib tests/
```

Or build and run the `dev` stage of the Dockerfile.
