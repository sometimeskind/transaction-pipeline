# transaction-pipeline

Prefect pipeline that imports bank transactions into Firefly III. It fetches
booked transactions from [Enable Banking](https://enablebanking.com/) (PSD2
account information), maps them with logic tuned to our own accounts, and writes
them through the Firefly III API. It replaces the Firefly III data importer.
Intent and done-when: [homelab#1981](https://github.com/sometimeskind/homelab/issues/1981).

**Status:** skeleton. The Enable Banking client (`enable_banking.py`), the Firefly
client (`firefly.py`) and the mapping (`mapping.py`) exist; the flow does nothing yet.

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
