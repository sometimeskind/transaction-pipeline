"""Prefect flows.

Skeleton only: the daily import lands in later PRs (homelab#1981).
"""

from prefect import flow


@flow(name="transaction-import")
def import_flow() -> None:
    """Fetch booked transactions from Enable Banking and write them to Firefly III."""
    raise NotImplementedError("transaction-import is not implemented yet (homelab#1981)")
