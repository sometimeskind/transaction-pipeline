"""Pushgateway metrics, as in document-pipeline: pushed per run, no-op when PUSHGATEWAY_URL is unset.

Both gauges go to one group, `transaction-pipeline`, and both pushes are POSTs
(pushadd), which replace only the metrics they name. A failing run pushes the
consent gauge but must leave `last_success_timestamp` standing: its age is what
the staleness alert reads.
"""

from __future__ import annotations

import logging
import os
import time

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

logger = logging.getLogger(__name__)

JOB = "transaction-pipeline"


def push_consent_valid_until(valid_until: dict[str, float]) -> None:
    """Push each session's `valid_until` (unix seconds), labelled by session. Every run with a session pushes it.

    All sessions go in one push, which replaces the metric in the group: a
    session whose file was removed stops being reported.
    """
    registry = CollectorRegistry()
    gauge = Gauge(
        "transaction_pipeline_consent_valid_until_timestamp",
        "Unix timestamp at which the Enable Banking consent (session valid_until) expires",
        ["session"],
        registry=registry,
    )
    for label, timestamp in valid_until.items():
        gauge.labels(session=label).set(timestamp)
    _pushadd(registry)


def push_success() -> None:
    registry = CollectorRegistry()
    Gauge(
        "transaction_pipeline_last_success_timestamp",
        "Unix timestamp of the last successful transaction-import run",
        registry=registry,
    ).set(time.time())
    _pushadd(registry)


def _pushadd(registry: CollectorRegistry) -> None:
    url = os.environ.get("PUSHGATEWAY_URL", "")
    if not url:
        return
    try:
        pushadd_to_gateway(url, job=JOB, registry=registry, timeout=10)
    except Exception as exc:
        logger.warning("Pushgateway push failed: %s", exc)
